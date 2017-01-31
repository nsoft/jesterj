/*
 * Copyright 2014 Needham Software LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jesterj.ingest;

import com.google.common.io.Resources;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.docopt.clj;
import org.jesterj.ingest.forkjoin.JesterJForkJoinThreadFactory;
import org.jesterj.ingest.persistence.Cassandra;
import org.jesterj.ingest.model.Plan;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.security.AllPermission;
import java.security.CodeSource;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 7/5/14
 */

/**
 * Start a running instance. Each instance should have an id and a password (freely chosen
 * by the user starting the process. The ID will be used to display the node in the control
 * console and the password is meant to provide temporary security until the node is
 * configured properly.
 */
public class Main {

  public static String JJ_DIR;

  static {
    // set up a config dir in user's home dir
    String userDir = System.getProperty("user.home");
    File jjDir = new File(userDir + "/.jj");
    if (!jjDir.exists() && !jjDir.mkdir()) {
      throw new RuntimeException("could not create " + jjDir);
    } else {
      try {
        JJ_DIR = jjDir.getCanonicalPath();
      } catch (IOException e) {
        e.printStackTrace();
        System.exit(1);
      }
    }
  }

  private static Logger log;

  public static void main(String[] args) {
    try {
      System.setProperty("java.util.concurrent.ForkJoinPool.common.threadFactory", JesterJForkJoinThreadFactory.class.getName());

      // set up log output dir
      String logDir = System.getProperty("jj.log.dir");
      if (logDir == null) {
        System.setProperty("jj.log.dir", JJ_DIR + "/logs");
      } else {
        if (!(new File(logDir).canWrite())) {
          System.out.println("Cannot write to log dir," + logDir + " switching to default...");
        }
      }
      System.out.println("logs will be written to: " + System.getProperty("jj.log.dir"));

      initClassloader();
      Thread contextClassLoaderFix = new Thread(() -> {
        try {

          initRMI();

          // Next check our args and die if they are FUBAR
          Map<String, Object> parsedArgs = usage(args);

          startCassandra(parsedArgs);

          // now we are allowed to look at log4j2.xml
          log = LogManager.getLogger();

          Properties sysProps = System.getProperties();
          for (Object prop : sysProps.keySet()) {
            log.trace(prop + "=" + sysProps.get(prop));
          }

          String javaConfig = System.getProperty("jj.javaConfig");
          if (javaConfig != null) {
            log.info("Looking for configuration class in {}", javaConfig);
            runJavaConfig(javaConfig);
          } else {
            System.out.println("Please specify the java config via -Djj.javaConfig=<location of jar file>");
            System.exit(1);
          }

          //noinspection InfiniteLoopStatement
          while (true) {
            try {
              System.out.print(".");
              Thread.sleep(5000);
            } catch (InterruptedException e) {

              // Yeah, I know this isn't going to do anything right now.. Placeholder to remind me to implement a real
              // graceful shutdown... also keeps IDE from complaining stop() isn't used.

              e.printStackTrace();
              Cassandra.stop();
              System.exit(0);
            }
          }
        } catch (Exception e) {
          log.fatal("CRASH and BURNED:", e);
        }

      });
      // unfortunately due to the hackery necessary to get things playing nice with one-jar, the contextClassLoader
      // is now out of sync with the system class loader, which messe up the Reflections library. So hack on hack...
      Field _f_contextClassLoader = Thread.class.getDeclaredField("contextClassLoader");
      _f_contextClassLoader.setAccessible(true);
      _f_contextClassLoader.set(contextClassLoaderFix, ClassLoader.getSystemClassLoader());
      contextClassLoaderFix.setDaemon(false); // keep the JVM running please
      contextClassLoaderFix.start();
    } catch (Exception e) {
      log.fatal("CRASH and BURNED:", e);
    }
  }

  static void startCassandra(Map<String, Object> parsedArgs) {
    String cassandraHome = (String) parsedArgs.get("--cassandra-home");
    File cassandraDir = null;
    if (cassandraHome != null) {
      cassandraHome = cassandraHome.replaceFirst("^~", System.getProperty("user.home"));
      cassandraDir = new File(cassandraHome);
      if (!cassandraDir.isDirectory()) {
        System.err.println("\nERROR: --cassandra-home must specify a directory\n");
        System.exit(1);
      }
    }
    if (cassandraDir == null) {
      cassandraDir = new File(JJ_DIR + "/cassandra");
    }
    Cassandra.start(cassandraDir);
  }


  static void runJavaConfig(String javaConfig) throws InstantiationException, IllegalAccessException {
    ClassLoader onejarLoader = null;
    File file = new File(javaConfig);
    if (!file.exists()) {
      System.err.println("File not found:" + file);
      System.exit(1);
    }

    try {
      File jarfile = new File(javaConfig);
      URL url = jarfile.toURI().toURL();

      ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
      onejarLoader = systemClassLoader;

      // This relies on us wrapping onejar's loader in a URL loader so we can add stuff.
      URLClassLoader classLoader = (URLClassLoader) systemClassLoader;
      Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
      method.setAccessible(true);
      method.invoke(classLoader, url);
    } catch (Exception ex) {
      ex.printStackTrace();
    }

    // Unfortunately this classpath scan adds quite a bit to startup time.... It seems to scan all the
    // Jdk classes (but not classes loaded by onejar, thank goodness) It only works with URLClassLoaders
    // but perhaps we can provide a temporary sub-class 
    Reflections reflections = new Reflections(new ConfigurationBuilder().addUrls(ClasspathHelper.forClassLoader(onejarLoader)));
    ArrayList<Class> planProducers = new ArrayList<>(reflections.getTypesAnnotatedWith(JavaPlanConfig.class));

    log.info("Found the following @JavaPlanConfig classes (first in list will be used):{}", planProducers);

    Class config = planProducers.get(0);
    PlanProvider provider = (PlanProvider) config.newInstance();
    Plan plan = provider.getPlan();
    log.info("Activating Plan: {}", plan.getName());
    plan.activate();
  }

  // will come back in some form when we serialize config to a file.. 

//  private static void writeConfig(Plan myPlan, String groupId) {
//    // This ~/.jj/groups is going to be the default location for loadable configs
//    // if the commandline startup id matches the name of a directory in the groups directory
//    // that configuration will be loaded. 
//    String sep = System.getProperty("file.separator");
//    File jjConfigDir = new File(JJ_DIR, "groups" + sep + groupId + sep + myPlan.getName());
//    if (jjConfigDir.exists() || jjConfigDir.mkdirs()) {
//      System.out.println("made directories");
//      PlanImpl.Builder tmpBuilder = new PlanImpl.Builder();
//      String yaml = tmpBuilder.toYaml(myPlan);
//      System.out.println("created yaml string");
//      File file = new File(jjConfigDir, "config.jj");
//      try (FileOutputStream fis = new FileOutputStream(file)) {
//        fis.write(yaml.getBytes("UTF-8"));
//        System.out.println("created file");
//      } catch (IOException e) {
//        log.error("failed to write file", e);
//        throw new RuntimeException(e);
//      }
//    } else {
//      throw new RuntimeException("Failed to make config directories");
//    }
//  }

  /**
   * Set up security policy that allows RMI and JINI code to work. Also seems to be
   * helpful for running embedded cassandra. TODO: Minimize the permissions granted.
   */
  private static void initRMI() {
    // must do this before any jini code
    String policyFile = System.getProperty("java.security.policy");
    if (policyFile == null) {
      // for river/jni
      final Permissions pc = new Permissions();
      pc.add(new AllPermission());
      Policy.setPolicy(new Policy() {
        @Override
        public PermissionCollection getPermissions(CodeSource codesource) {
          return pc;
        }

        @Override
        public PermissionCollection getPermissions(ProtectionDomain domain) {
          return pc;
        }

      });
      System.setSecurityManager(new SecurityManager());
    }
  }

  /**
   * Initialize the classloader. This method fixes up an issue with OneJar's class loaders. Nothing in or before
   * this method should touch logging, or 3rd party jars that logging that might try to setup log4j.
   *
   * @throws NoSuchFieldException   if the system class loader field has changed in this version of java and is not "scl"
   * @throws IllegalAccessException if we are unable to set the system class loader
   */
  private static void initClassloader() throws NoSuchFieldException, IllegalAccessException {
    // for river
    System.setProperty("java.rmi.server.RMIClassLoaderSpi", "net.jini.loader.pref.PreferredClassProvider");

    // fix bug in One-Jar with an ugly hack
    ClassLoader myClassLoader = Main.class.getClassLoader();
    String name = myClassLoader.getClass().getName();
    if ("com.simontuffs.onejar.JarClassLoader".equals(name)) {
      Field scl = ClassLoader.class.getDeclaredField("scl"); // Get system class loader
      scl.setAccessible(true); // Set accessible
      scl.set(null, new URLClassLoader(new URL[]{}, myClassLoader)); // Update it to our class loader
    }
  }

  private static AbstractMap<String, Object> usage(String[] args) throws IOException {
    URL usage = Resources.getResource("usage.docopts.txt");
    String usageStr = Resources.toString(usage, Charset.forName("UTF-8"));
    @SuppressWarnings("unchecked")
    AbstractMap<String, Object> result = clj.docopt(usageStr, args);
    if (result != null) {
      System.out.println("\nReceived arguments:");
      for (String s : result.keySet()) {
        System.out.printf("   %s:%s\n", s, result.get(s));
      }
    }
    if (result == null || result.get("--help") != null) {
      System.out.println(usageStr);
      System.exit(1);
    }
    return result;
  }

}


