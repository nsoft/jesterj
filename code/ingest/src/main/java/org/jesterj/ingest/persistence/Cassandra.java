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

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.metadata.Node;
import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.ConfigurationLoader;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.log4j.Logger; // must not use before cassandra boots!
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.ServerSocketChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Map;
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

  static {
    System.setProperty("cassandra.native.epoll.enabled", "false");
  }

  private static CassandraDaemon cassandra;
  @SuppressWarnings("rawtypes")
  private static final ConcurrentLinkedQueue<RunnableFuture> finalBootActions = new ConcurrentLinkedQueue<>();
  private static String listenAddress;

  private static Object log;
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
    start(cassandraDir, null);
  }

  public static void start(File cassandraDir, String listenAddress) {

    System.out.println("Booting internal cassandra");
    boolean firstboot = false;
    try {
      File triggers = new File(cassandraDir, "triggers");
      System.setProperty("cassandra.triggers_dir", triggers.getCanonicalPath());
      if (!cassandraDir.exists() && !cassandraDir.mkdirs() && triggers.mkdirs()) {
        throw new RuntimeException("could not create" + cassandraDir);
      }
      File yaml = new File(cassandraDir, "cassandra.yaml");
      String confURI = yaml.toURI().toString();
      System.setProperty("cassandra.config", confURI);
      System.out.println("Using cassandra config file at: " + confURI);

      if (!yaml.exists()) {
        firstboot = true;
        CassandraConfig cfg = new CassandraConfig(cassandraDir.getCanonicalPath());
        if (listenAddress == null) {
          cfg.guessIp();
        } else {
          cfg.setListen_address(listenAddress);
        }
        String cfgStr = new Yaml().dumpAsMap(cfg);
        System.out.println("First time startup detected, writing default config to " + yaml.toPath());
        System.out.println(cfgStr);
        Files.write(yaml.toPath(), cfgStr.getBytes(), StandardOpenOption.CREATE);
      }

      ConfigurationLoader cl = new YamlConfigurationLoader();
      Config conf = cl.loadConfig();
      System.out.println("CASSANDRA DIR:" + cassandraDir);

      try {
        ServerSocket s = new ServerSocket(0);
        int freePort = s.getLocalPort();
        s.close();
        ServerSocketChannel testSocket = ServerSocketChannel.open();
        InetSocketAddress addr = new InetSocketAddress(conf.listen_address, freePort);
        // the line below is what has to work when cassandra tries to do it (only on cassandra's port)
        testSocket.bind(addr);
        testSocket.close();
      } catch (BindException e) {
        // try not to blow up every time we want to demo something on someone else's wireless...
        System.out.println("WARNING: ipAddress has changed, temporarily picking new address");
        CassandraConfig cfg = new CassandraConfig(cassandraDir.getCanonicalPath());
        cfg.guessIp();
        conf.listen_address = cfg.getListen_address();
      }

      Cassandra.listenAddress = conf.listen_address;

      System.out.println("Listen Address:" + conf.listen_address);
    } catch (IOException | ConfigurationException e) {
      e.printStackTrace();
    }
    cassandra = new JJCassandraDaemon();
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
    System.out.println("Starting final boot actions (" + finalBootActions.size() + ")");
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

  public static InetSocketAddress getSocketAddress() {
    return new InetSocketAddress(listenAddress, 9042);
  }

  public static void stop() {
    booting = true;
    log = null;
    cassandra.stop();
    cassandra.destroy();
  }

  static <T> Future<T> whenBooted(Callable<T> callable) {
    FutureTask<T> t = new FutureTask<>(callable);
    synchronized (finalBootActions) {
      if (isBooting()) {
        finalBootActions.add(t);
      } else {
        t.run();
      }
    }
    return t;
  }

  public static void main(String[] args) {
    // DO NOT USE: this main method exists for developer debugging when developing JesterJ itself only!
    // Unmaintained, potentially harmful
    // Use at own risk
    // Your mileage may vary
    // Limitations and exclusions may apply
    // Offers not in combination
    // "Ah, this is obviously some strange usage of the word 'safe' that I wasn't previously aware of"
    File file = new File(args[0]);
    start(file);
  }

  public static void printErrors(ResultSet rs) {
    for (Map.Entry<Node, Throwable> error : rs.getExecutionInfo().getErrors()) {
      String message = error.getKey() + ":" + error.getValue();
      if (booting) {
        System.out.println(message);
      } else {
        if (log == null) {
          try {
            // LogManager can't be referenced directly because it starts a status logger of its own.
            Class<?> logManagerClass = Class.forName("org.apache.logging.log4j.LogManager");
            Method getLogger = logManagerClass.getMethod("getLogger");
            log = getLogger.invoke(logManagerClass);
          } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            System.out.println("Failed to instantiate logger in Cassandra.java:" + e.getMessage());
          }
        }
        if (log != null) {
          ((Logger) log).error(message);
        }
      }
    }
  }
}
