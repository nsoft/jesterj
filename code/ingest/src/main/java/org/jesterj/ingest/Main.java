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
import org.apache.logging.log4j.ThreadContext;
import org.docopt.clj;
import org.jesterj.ingest.forkjoin.JesterJForkJoinThreadFactory;
import org.jesterj.ingest.logging.Cassandra;
import org.jesterj.ingest.logging.JesterJAppender;
import org.jesterj.ingest.logging.Markers;
import org.jesterj.ingest.model.impl.PlanImpl;
import org.jesterj.ingest.model.impl.StepImpl;
import org.jesterj.ingest.processors.CopyField;
import org.jesterj.ingest.processors.SendToSolrCloudProcessor;
import org.jesterj.ingest.processors.SimpleDateTimeReformater;
import org.jesterj.ingest.processors.TikaProcessor;
import org.jesterj.ingest.scanners.SimpleFileWatchScanner;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.AllPermission;
import java.security.CodeSource;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.util.AbstractMap;
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

  private static final String ACCESSED = "format accessed date";
  private static final String CREATED = "format created date";
  private static final String MODIFIED = "format modified date";
  private static final String SIZE_TO_INT = "size to int";
  private static final String TIKA = "tika";
  public static String JJ_DIR;

  public static IngestNode node;

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

  private static final String SHAKESPEAR = "Shakespear scanner";

  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, NoSuchFieldException, IllegalAccessException {
    System.setProperty("java.util.concurrent.ForkJoinPool.common.threadFactory", JesterJForkJoinThreadFactory.class.getName());
    initClassloader();
    initRMI();
    Cassandra.start();

    // now we can see log4j2.xml
    log = LogManager.getLogger();

    log.info(Markers.LOG_MARKER, "Test regular log with marker");
    log.info("Test regular log without marker");

    try {
      ThreadContext.put(JesterJAppender.JJ_INGEST_DOCID, "file:///foo/bar.txt");
      log.error(Markers.SET_DROPPED, "Test fti drop");
    } finally {
      ThreadContext.clearAll();
    }


    // Next check our args and die if they are FUBAR
    Map<String, Object> parsedArgs = usage(args);

    String id = String.valueOf(parsedArgs.get("id"));
    String password = String.valueOf(parsedArgs.get("password"));

    Properties sysprops = System.getProperties();
    for (Object prop : sysprops.keySet()) {
      log.trace(prop + "=" + sysprops.get(prop));
    }

    // This  does nothing useful yet, just for testing right now.

    log.debug("Starting injester node...");
    node = new IngestNode(id, password);
    new Thread(node).start();

    String property = System.getProperty("jj.example");
    if ("run".equals(property)) {
      PlanImpl.Builder planBuilder = new PlanImpl.Builder();
      SimpleFileWatchScanner.Builder scanner = new SimpleFileWatchScanner.Builder();
      StepImpl.Builder formatCreated = new StepImpl.Builder();
      StepImpl.Builder formatModified = new StepImpl.Builder();
      StepImpl.Builder formatAccessed = new StepImpl.Builder();
      StepImpl.Builder renameFileszieToInteger = new StepImpl.Builder();
      StepImpl.Builder tikaBuilder = new StepImpl.Builder();
      StepImpl.Builder sendToSolrBuilder = new StepImpl.Builder();

      File testDocs = new File("/Users/gus/projects/solrsystem/jesterj/code/ingest/src/test/resources/test-data/");

      scanner
          .stepName(SHAKESPEAR)
          .withRoot(testDocs)
          .scanFreqMS(100);
      formatCreated
          .stepName(CREATED)
          .withProcessor(
              new SimpleDateTimeReformater.Builder()
                  .from("created")
                  .into("created_dt")
                  .build()
          );
      formatModified
          .stepName(MODIFIED)
          .withProcessor(
              new SimpleDateTimeReformater.Builder()
                  .from("modified")
                  .into("modified_dt")
                  .build()
          );
      formatAccessed
          .stepName(ACCESSED)
          .withProcessor(
              new SimpleDateTimeReformater.Builder()
                  .from("accessed")
                  .into("accessed_dt")
                  .build()
          );
      renameFileszieToInteger
          .stepName(SIZE_TO_INT)
          .withProcessor(
              new CopyField.Builder()
                  .from("file_size")
                  .into("file_size_i")
                  .retainingOriginal(false)
                  .build()
          );
      tikaBuilder
          .stepName(TIKA)
          .withProcessor(new TikaProcessor()
          );
      sendToSolrBuilder
          .stepName("solr sender")
          .withProcessor(
              new SendToSolrCloudProcessor.Builder()
                  .withZookeperHost("localhost")
                  .atZookeeperPort(9983)
                  .usingCollection("jjtest")
                  .placingTextContentIn("_text_")
                  .build()
          );
      planBuilder
          .addStep(null, scanner)
          .addStep(new String[]{SHAKESPEAR}, formatCreated)
          .addStep(new String[]{CREATED}, formatModified)
          .addStep(new String[]{MODIFIED}, formatAccessed)
          .addStep(new String[]{ACCESSED}, renameFileszieToInteger)
          .addStep(new String[]{SIZE_TO_INT}, tikaBuilder)
          .addStep(new String[]{TIKA}, sendToSolrBuilder)
          .withIdField("id")
          .build()
          .activate();
    }

    //noinspection InfiniteLoopStatement
    while (true) {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {

        // Yeah, I know this isn't going to do anything right now.. Placeholder to remind me to implement a real
        // graceful shutdown... also keeps IDE from complaining stop() isn't used.

        e.printStackTrace();
        Cassandra.stop();
        System.exit(0);
      }
    }


  }

  /**
   * Set up security policy that allows RMI and JINI code to work. Also seems to be
   * helpful for running embedded cassandra. TODO: Minimize the permissions granted.
   *
   * @throws NoSuchFieldException
   * @throws IllegalAccessException
   */
  private static void initRMI() throws NoSuchFieldException, IllegalAccessException {
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
   * Initialize the classloader. This method fixes up an issue with OneJar's classloaders. Nothing in or before
   * this method should touch logging, or 3rd party jars that logging that might try to setup log4j.
   *
   * @throws NoSuchFieldException
   * @throws IllegalAccessException
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
      scl.set(null, myClassLoader); // Update it to your class loader
    }
  }

  private static AbstractMap<String, Object> usage(String[] args) throws IOException {
    URL usage = Resources.getResource("usage.docopts.txt");
    String usageStr = Resources.toString(usage, Charset.forName("UTF-8"));
    @SuppressWarnings("unchecked")
    AbstractMap<String, Object> result = clj.docopt(usageStr, args);
    if (result != null) {
      for (String s : result.keySet()) {
        log.debug("{}:{}", s, result.get(s));
      }
    }
    if (result == null || result.get("--help") != null) {
      System.out.println(usageStr);
      System.exit(1);
    }
    return result;
  }

}


