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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.appender.AbstractManager;

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

  // Possibly move these to wide rows with time values?
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


  // This constructor is weird for a reason. It's critically important not to throw an exception or
  // do anything that causes a log message in another thread before this constructor completes.
  // (such as starting cassandra) Doing so causes that thread and this one to deadlock and hangs
  // everything. This includes calling System.exit() since that gets checked in another JVM thread and
  // the JVM thread tries to start a JUL logger!
  protected CassandraManager(String name) {
    super(name);

    Callable<Object> makeTables = new Callable<Object>() {

      @Override
      public Object call() throws Exception {
        boolean tryAgain = true;
        int tryCount = 0;
        // ugly but effective fix for https://github.com/nsoft/jesterj/issues/1
        while(tryAgain) {
          try (
              Session session = Cluster.builder()
                  // safe to use getListenAddress since we will only be called after cassandra is booted.
                  .addContactPoint(Cassandra.getListenAddress())
                  //TODO: something secure!
                  .withCredentials("cassandra", "cassandra")
                  .build().newSession()
          ) {
            session.execute(CREATE_LOG_KEYSPACE);
            session.execute(CREATE_LOG_TABLE);
            session.execute(CREATE_FT_TABLE);
            tryAgain = false;
          } catch (Exception e) {
            tryCount++;
            // complain and die if we can't set up the tables in cassandra.
            e.printStackTrace();
            Thread.sleep(1000);
            if (tryCount > 10) {
              die(e);
              tryAgain = false;
            }
          }
        }
        return null;
      }
    };
    this.cassandraReady = Cassandra.whenBooted(makeTables);
  }

  void die(Exception e) {
    // Let logging config complete and then die. This avoids deadlocking the JVM shutdown thread
    // as it attempts to create a JUL logger.
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

  @Override
  protected void releaseSub() {

  }


  public boolean isReady() {
    return cassandraReady.isDone();
  }
}
