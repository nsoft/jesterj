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

package org.jesterj.ingest.logging;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.appender.AbstractManager;
import org.jesterj.ingest.Main;

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 11/15/14
 */
public class CassandraManager extends AbstractManager {

  public static final String CREATE_LOG_KEYSPACE =
      "CREATE KEYSPACE IF NOT EXISTS jj_logging " +
          "WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };";
  public static final String CREATE_LOG_TABLE =
      "CREATE TABLE IF NOT EXISTS jj_logging.regular(" +
          "id uuid PRIMARY KEY, " +
          "logger text, " +
          "tstamp timestamp, " +
          "level text, " +
          "thread text, " +
          "message text" +
          ");";
  public static final String CREATE_FT_TABLE =
      "CREATE TABLE IF NOT EXISTS jj_logging.fault_tolerant(" +
          "id uuid PRIMARY KEY, " +
          "logger text, " +
          "tstamp timestamp, " +
          "level text, " +
          "thread text, " +
          "status text, " +
          "docid text, " +
          "message text" +
          ");";
  private final Future cassandraReady;

  Executor executor = new ThreadPoolExecutor(1, 1, 100, TimeUnit.SECONDS, new SynchronousQueue<>());


  // This constructor is complicated! It's critically important not to throw an exception or
  // do anything that causes a log message in another thread before this constructor completes.
  // (such as starting cassandra) Doing so causes that thread and this one to deadlock and hangs
  // everything.
  protected CassandraManager(String name) {
    super(name);
    Config config;
    try {
      File f = new File(Main.JJ_DIR + "/cassandra/cassandra.yaml");
      long start = System.currentTimeMillis();
      // On first startup we need to wait for the cassandra thread to create this....
      // but if it takes more than 5 sec, something is wrong and failing is appropriate.
      while (!f.exists() && (System.currentTimeMillis() - start) < 5000) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
          System.exit(2);
        }
      }
      if (!f.exists()) {
        System.exit(4);
      }
      String configURI = f.toURI().toString().replaceAll("file:/([^/])", "file:///$1");
      System.setProperty("cassandra.config", configURI);
      config = (new YamlConfigurationLoader()).loadConfig();
    } catch (ConfigurationException e) {
      throw new RuntimeException("Could not find cassandra config", e);
    }

    Callable<Object> makeTables = new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        Session session = Cluster.builder()
            .addContactPoint(config.listen_address)
            .withCredentials("cassandra", "cassandra")
            .build().newSession();

        try {
          session.execute(CREATE_LOG_KEYSPACE);
          session.execute(CREATE_LOG_TABLE);
          session.execute(CREATE_FT_TABLE);
        } catch (Exception e) {
          // complain and die if we can't set up the tables in cassandra.
          e.printStackTrace();
          executor.execute(() -> {
            try {
              LogManager.getRootLogger().error("!!!!\n!!!!\nShutting down in 5 seconds due to persistence failure: " + e.getMessage() + "\n!!!!\n!!!!");
              Thread.sleep(5000);
            } catch (InterruptedException e1) {
              e1.printStackTrace();
            }
            System.exit(3);
          });
        }
        return null;
      }
    };
    this.cassandraReady = Cassandra.whenBooted(makeTables);
  }

  @Override
  protected void releaseSub() {

  }


  public boolean isReady() {
    return cassandraReady.isDone();
  }
}
