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

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractManager;
import org.jesterj.ingest.persistence.CassandraSupport;

import java.util.concurrent.*;

public class CassandraLog4JManager extends AbstractManager {

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
          "docid text , " +
          "logger text, " +
          "tstamp timestamp, " +
          "level text, " +
          "thread text," +
          "scanner text, " +
          "status text, " +
          "message text," +
          "md5hash text," +
          "PRIMARY KEY (docid, scanner));";

  public static final String FTI_STATUS_INDEX = "CREATE INDEX IF NOT EXISTS fti_statuses ON jj_logging.fault_tolerant( status );";
  public static final String FTI_SCANNER_INDEX = "CREATE INDEX IF NOT EXISTS fti_scanners ON jj_logging.fault_tolerant( scanner );";
  private final Future cassandraReady;

  Executor executor = new ThreadPoolExecutor(1, 1, 100, TimeUnit.SECONDS, new SynchronousQueue<>());


  // This constructor is weird for a reason. It's critically important not to throw an exception or
  // do anything that causes a log message in another thread before this constructor completes.
  // (such as starting cassandra) Doing so causes that thread and this one to deadlock and hangs
  // everything. This includes calling System.exit() since that gets checked in another JVM thread and
  // the JVM thread tries to start a JUL logger, which gets piped into log4j and deadlock!
  protected CassandraLog4JManager(String name) {
    super(LoggerContext.getContext(), name);
    System.out.println(">>>> Creating CassandraLog4JManager");
    CassandraSupport cassandra = new CassandraSupport();

    Callable<Object> makeTables = new Callable<Object>() {


      @Override
      public Object call() throws Exception {
        System.out.println("Table and key space creation thread started");
        boolean tryAgain = true;
        int tryCount = 0;
        // ugly but effective fix for https://github.com/nsoft/jesterj/issues/1
        while(tryAgain) {
          try {
            CqlSession session = cassandra.getSession();
            session.execute(CREATE_LOG_KEYSPACE);
            session.execute(CREATE_LOG_TABLE);
            session.execute(CREATE_FT_TABLE);
            session.execute(FTI_STATUS_INDEX);
            session.execute(FTI_SCANNER_INDEX);
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
    this.cassandraReady = cassandra.whenBooted(makeTables);
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


  public boolean isReady() {
    return cassandraReady.isDone();
  }
}
