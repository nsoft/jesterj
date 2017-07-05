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

package org.jesterj.ingest.persistence;

import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.ConfigurationLoader;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.CassandraDaemon;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;


/**
 * Starts a Casandra Daemon after creating or loading the cassandra config. The main purpose of this
 * class is to wrap the CassandraDaemon and feed it a programmatically created configuration file.
 */
public class Cassandra {

  // LOGGER WARNING: Do not set up a logger in this class, our logging configuration relies on this class and
  // that circularity will cause a deadlock (trust me it's ugly). So yes the System.out prints must stay,
  // though they should all fire only during system startup.

  private static CassandraDaemon cassandra;
  private static final ConcurrentLinkedQueue<RunnableFuture> finalBootActions = new ConcurrentLinkedQueue<>();
  private static String listenAddress;

  private volatile static boolean booting = true;

  /**
   * Indicates whether cassandra has finished booting. Does NOT indicate if
   * cassandra has subsequently been stopped. This method is not synchronized because
   * it is not meant to continuously track the state of cassandra, only serve as a latch
   * to release code that must not execute while cassandra has not yet booted.
   *
   * @return true indicating that cassandra has definitely finished booting, false indicates that cassandra may or
   * may not have booted yet.
   */
  public static boolean isBooting() {
    return booting;
  }

  public static void start(File cassandraDir) {

    System.out.println("Booting internal cassandra");
    boolean firstboot = false;
    try {
      if (!cassandraDir.exists() && !cassandraDir.mkdirs()) {
        throw new RuntimeException("could not create" + cassandraDir);
      }
      File yaml = new File(cassandraDir, "cassandra.yaml");
      String confURI = yaml.toURI().toString();
      System.setProperty("cassandra.config", confURI);
      System.out.println("Using cassandra config file at: " + confURI);

      if (!yaml.exists()) {
        firstboot = true;
        CassandraConfig cfg = new CassandraConfig(cassandraDir.getCanonicalPath());
        cfg.guessIp();
        String cfgStr = new Yaml().dumpAsMap(cfg);
        System.out.println("First time startup detected, writing default config to " + yaml.toPath());
        System.out.println(cfgStr);
        Files.write(yaml.toPath(), cfgStr.getBytes(), StandardOpenOption.CREATE);
      }

      ConfigurationLoader cl = new YamlConfigurationLoader();
      Config conf = cl.loadConfig();
      listenAddress = conf.listen_address;

      System.out.println("Listen Address:" + listenAddress);
    } catch (IOException | ConfigurationException e) {
      e.printStackTrace();
    }
    cassandra = new CassandraDaemon();
    try {
      // keep cassandra from clobering system.out and sytem.err
      System.setProperty("cassandra-foreground", "true");
      cassandra.activate();
    } catch (Exception e) {
      e.printStackTrace();
    }

    // Issue #59
    // Cassandra waits 10 seconds before creating the super-user, see CassandraRoleManager.scheduleSetupTask()
    // https://github.com/apache/cassandra/blob/cassandra-3.11.0/src/java/org/apache/cassandra/auth/CassandraRoleManager.java#L403
    if (firstboot) {
      try {
        // wait 11 seconds...
        System.out.println("First time startup... waiting for Cassandra to create it's default roles");
        Thread.sleep((long) (AuthKeyspace.SUPERUSER_SETUP_DELAY * 1.1));
      } catch (InterruptedException e) {
        throw new RuntimeException("interrupted during startup");
      }
    }
    synchronized (finalBootActions) {
      while (finalBootActions.peek() != null) {
        finalBootActions.remove().run();
      }
      booting = false;
    }
    System.out.println("Cassandra booted");
  }

  public static String getListenAddress() {
    return listenAddress;
  }

  public static void stop() {
    cassandra.deactivate();
  }

  public static Future whenBooted(Callable<Object> callable) {
    FutureTask<Object> t = new FutureTask<>(callable);
    synchronized (finalBootActions) {
      if (isBooting()) {
        finalBootActions.add(t);
      } else {
        t.run();
      }
    }
    return t;
  }

}
