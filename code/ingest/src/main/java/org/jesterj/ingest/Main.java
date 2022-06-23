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
import com.needhamsoftware.unojar.JarClassLoader;
import io.github.classgraph.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.docopt.Docopt;
import org.jesterj.ingest.forkjoin.JesterJForkJoinThreadFactory;
import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.persistence.Cassandra;
import org.jesterj.ingest.utils.JesterJLoader;
import org.jesterj.ingest.utils.JesterjPolicy;
import org.jetbrains.annotations.NotNull;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.Policy;
import java.util.*;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

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

  // WARNING: do not add a logger init to this class! See below for classloading highjinks that
  // force us to wait to initialize logging
  private static Logger log;

  private static final Object HAPPENS_BEFORE = new Object();

  public static String JJ_DIR;
  private static final Thread DUMMY_HOOK = new Thread();

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
        System.exit(10);
      }
    }
  }

  public static void main(String[] args) {
//    Thread.sleep(10000); // debugger connect time


    synchronized (HAPPENS_BEFORE) {
      try {
        System.setProperty("java.util.concurrent.ForkJoinPool.common.threadFactory", JesterJForkJoinThreadFactory.class.getName());
        System.setProperty("cassandra.insecure.udf", "true");

        initClassloader();

        Thread contextClassLoaderFix = new Thread(() -> {
          // ensure that the main method completes before this thread runs.
          synchronized (HAPPENS_BEFORE) {
            try {
              initRMI();

              // Next check our args and die if they are FUBAR
              Map<String, Object> parsedArgs = usage(args);

              String id = (String) parsedArgs.get("<id>");
              String ourHome = JJ_DIR + "/" + id;
              String logDir = setLogDir(ourHome);
              System.out.println("Logs will be written to: " + logDir);
              String outfile = (String) parsedArgs.get("-z");

              String javaConfig = (String) parsedArgs.get("<plan.jar>");
              boolean runPlan = !Boolean.parseBoolean(String.valueOf(parsedArgs.get("--cassandra-only")));
              System.out.println("Looking for configuration class in " + javaConfig);
              if (outfile != null) {
                // in this case we aren't starting a node, and we don't care if logging doesn't make it to
                // cassandra (in fact better if it doesn't) so go ahead and call what we like INSIDE this if
                // block only.
                Plan p = loadJavaConfig(javaConfig);
                System.out.println("Generating visualization for " + p.getName() + " into " + outfile);
                BufferedImage img = p.visualize();
                ImageIO.write(img, "PNG", new File(outfile));
                System.exit(0);
              }
              startCassandra(parsedArgs);

              // this should reload the config with cassandra available.
              LogManager.getFactory().removeContext(LogManager.getContext(false));

              // now we are allowed to look at log4j2.xml
              log = LogManager.getLogger(Main.class);

              Properties sysProps = System.getProperties();
              for (Object prop : sysProps.keySet()) {
                log.trace(prop + "=" + sysProps.get(prop));
              }

              if (runPlan) {
                if (javaConfig != null) {
                  Plan p = loadJavaConfig(javaConfig);
                  log.info("Activating Plan: {}", p.getName());
                  p.activate();
                } else {
                  System.out.println("Please specify the java config via -Djj.javaConfig=<location of jar file>");
                  System.exit(11);
                }
              }
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
              e.printStackTrace();
              log.fatal("CRASH and BURNED:", e);
            }
          }
        });
        // unfortunately due to the hackery necessary to get things playing nice with one-jar, the contextClassLoader
        // is now out of sync with the system class loader, which messes up the Reflections library. So hack on hack...
        // todo: document why this reflection is necessary (I suspect I had some sort of security manager issue?) or remove
        // otherwise it seems like the following would be fine:
        // contextClassLoaderFix.setContextClassLoader(ClassLoader.getSystemClassLoader());
        Field _f_contextClassLoader = Thread.class.getDeclaredField("contextClassLoader");
        _f_contextClassLoader.setAccessible(true);
        _f_contextClassLoader.set(contextClassLoaderFix, ClassLoader.getSystemClassLoader());
        contextClassLoaderFix.setDaemon(false); // keep the JVM running please
        contextClassLoaderFix.start();

      } catch (Exception e) {
        System.out.println("CRASH and BURNED before starting main thread:");
        e.printStackTrace();
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ex) {
          ex.printStackTrace();
        }
        System.exit(12);
      }
    }
  }

  @NotNull
  private static String setLogDir(String logdir) throws IOException {
    // set up log output dir
    String logDir = System.getProperty("jj.log.dir");
    if (logDir == null) {
      System.setProperty("jj.log.dir", logdir + "/logs");
    }
    logDir = System.getProperty("jj.log.dir");

    // Check that we can write to the log dir
    File logDirFile = new File(logDir);

    if (!logDirFile.mkdirs() && !(logDirFile.canWrite())) {
      System.out.println("Cannot write to " + logDir + " \n" +
          "Please fix the filesystem permissions or provide a writable location with -Djj.log.dir property on the command line.");
      System.exit(99);
    }
    String logConfig = logDir + "/log4j2.xml";
    System.setProperty("log4j.configurationFile", logConfig);
    File configFile = new File(logConfig);
    if (!configFile.exists()) {
      InputStream log4jxml = Main.class.getResourceAsStream("/log4j2.xml");
      Files.copy(log4jxml, configFile.toPath());
    }

    return logDir;
  }

  private static void startCassandra(Map<String, Object> parsedArgs) {
    String cassandraHome = (String) parsedArgs.get("--cassandra-home");
    File cassandraDir = null;
    if (cassandraHome != null) {
      cassandraHome = cassandraHome.replaceFirst("^~", System.getProperty("user.home"));
      cassandraDir = new File(cassandraHome);
      if (!cassandraDir.isDirectory()) {
        System.err.println("\nERROR: --cassandra-home must specify a directory\n");
        System.exit(13);
      }
    }
    String id = (String) parsedArgs.get("<id>");
    if (cassandraDir == null) {
      cassandraDir = new File(JJ_DIR + "/" + id + "/cassandra");
    }
    Cassandra.start(cassandraDir);
  }


  private static Plan loadJavaConfig(String javaConfig) throws InstantiationException, IllegalAccessException {
    File file = new File(javaConfig);
    if (!file.exists()) {
      System.err.println("File not found:" + file);
      System.exit(14);
    }

    boolean isUnoJar = false;
    try {
      JarFile test = new JarFile(file);
      Attributes attrs = test.getManifest().getMainAttributes();
      String attr = attrs.getValue("Archive-Type");
      isUnoJar = attr != null && attr.trim().equalsIgnoreCase("uno-jar");
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(15);
    }
    boolean finalIsUnoJar = isUnoJar;
    if (log != null) {
      // can be null when outputting a visualization
      log.info("Loading from {} which is a {} file", () -> file, () ->
          finalIsUnoJar ? "Uno-Jar" : "Standard Jar");
    } else {
      System.out.println("Loading from "+file+" which is a "+(finalIsUnoJar ? "Uno-Jar" : "Standard Jar")+" file");
    }
    JesterJLoader jesterJLoader;

    File jarfile = new File(javaConfig);
    URL planConfigJarURL;
    try {
      planConfigJarURL = jarfile.toURI().toURL();
    } catch (MalformedURLException e) {
      throw new RuntimeException(e); // boom
    }

    jesterJLoader = (JesterJLoader) ClassLoader.getSystemClassLoader();

    ClassLoader loader;
    if (isUnoJar) {
      JarClassLoader jarClassLoader = new JarClassLoader(jesterJLoader, planConfigJarURL.toString());
      jarClassLoader.load(null);
      loader = jarClassLoader;
    } else {
      loader = new URLClassLoader(new URL[]{planConfigJarURL}, jesterJLoader);
    }
    jesterJLoader.addExtLoader(loader);

    try (ScanResult scanResult =
             new ClassGraph()
                 //.verbose()                   // Log to stderr
                 .overrideClassLoaders(loader)
                 .ignoreParentClassLoaders()
                 .enableClassInfo()
                 .enableAnnotationInfo()
                 .scan()) {                   // Start the scan
      ClassInfoList classesWithAnnotation;
      String routeAnnotation = JavaPlanConfig.class.getName();
      classesWithAnnotation = scanResult.getClassesWithAnnotation(routeAnnotation);
      List<String> planProducers = classesWithAnnotation.stream().map(ClassInfo::getName).collect(Collectors.toList());
      if (log != null) {
        // can be null when outputting a visualization
        log.info("Found the following @JavaPlanConfig classes (first in list will be used):{}", planProducers);
      } else {
        System.out.println("Found the following @JavaPlanConfig classes (first in list will be used):" + planProducers);
      }
      if (classesWithAnnotation.size() == 0) {
        System.err.println("No Plan Found!");
        System.exit(16);
      }
      @SuppressWarnings("rawtypes")
      Class config = jesterJLoader.loadClass(planProducers.get(0));
      //noinspection deprecation
      PlanProvider provider = (PlanProvider) config.newInstance();
      return provider.getPlan();
    } catch (ClassNotFoundException e) {
      System.err.println("Found a plan class but could not load Plan's class file!");
      System.exit(17);
      throw new RuntimeException(); // compiler doesn't understand system.exit
    }

  }


  /**
   * Set up security policy that allows RMI and JINI code to work. Also seems to be
   * helpful for running embedded cassandra. TODO: Minimize the permissions granted.
   */
  private static void initRMI() {
    // must do this before any jini code
    String policyFile = System.getProperty("java.security.policy");
    if (policyFile == null) {
      System.out.println("Installing JesterjPolicy");
      Policy.setPolicy(new JesterjPolicy());
    } else {
      System.out.println("Existing Policy File:" + policyFile);
    }
    System.setSecurityManager(new SecurityManager());
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
    ClassLoader unoJarClassLoader = Main.class.getClassLoader();
    String name = unoJarClassLoader.getClass().getName();
    if ("com.needhamsoftware.unojar.JarClassLoader".equals(name)) {
      Field scl = ClassLoader.class.getDeclaredField("scl"); // Get system class loader
      scl.setAccessible(true); // Set accessible
      scl.set(null, new JesterJLoader(new URL[]{}, unoJarClassLoader)); // Update it to our class loader
    }
  }

  @SuppressWarnings("UnstableApiUsage")
  private static Map<String, Object> usage(String[] args) throws IOException {
    URL usage = Resources.getResource("usage.docopts.txt");
    String usageStr = Resources.toString(usage, StandardCharsets.UTF_8);
    Map<String, Object> result = new Docopt(usageStr).parse(args);
    System.out.println("\nReceived arguments:");
    for (String s : result.keySet()) {
      System.out.printf("   %s:%s\n", s, result.get(s));
    }
    if ((boolean) result.get("--help")) {
      System.out.println(usageStr);
      System.exit(18);
    }
    return result;
  }

  /**
   * This is a heuristic test for system shutdown. It is potentially expensive, so it should only be used in
   * code that is not performance sensitive. (i.e. code where an exception is already being thrown).
   *
   * @return true if the system is shutting down
   */

  public synchronized static boolean isNotShuttingDown() {
    try {
      Runtime.getRuntime().addShutdownHook(DUMMY_HOOK);
      Runtime.getRuntime().removeShutdownHook(DUMMY_HOOK);
    } catch (IllegalStateException e) {
      return false;
    }

    return true;
  }
}


