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
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractManager;
import org.jesterj.ingest.persistence.CassandraSupport;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
          "error_count int," +
          "PRIMARY KEY (docid, scanner));";

  public static final String UPGRADE_FT_TABLE_ADD_COL =
      "ALTER TABLE jj_logging.<T> ADD <C> <Y>";

  public static final String FTI_STATUS_INDEX = "CREATE INDEX IF NOT EXISTS fti_statuses ON jj_logging.fault_tolerant( status );";
  public static final String FTI_SCANNER_INDEX = "CREATE INDEX IF NOT EXISTS fti_scanners ON jj_logging.fault_tolerant( scanner );";
  private final Future<Object> cassandraReady;

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

    // Be aware that this appears to get called twice, so it should be
    // idempotent. I suspect the cassandra code is doing screwy things
    // with class loaders leading to a second version of this class.
    Callable<Object> makeTables = new SchemaChecker(cassandra);
    this.cassandraReady = cassandra.whenBooted(makeTables);
  }

  void ensureBasicSchema(CqlSession session) {
    session.setSchemaMetadataEnabled(false);
    session.execute(CREATE_LOG_KEYSPACE);
    session.execute(CREATE_LOG_TABLE);
    session.execute(CREATE_FT_TABLE);
    session.execute(FTI_STATUS_INDEX);
    session.execute(FTI_SCANNER_INDEX);
    session.setSchemaMetadataEnabled(true);
    session.checkSchemaAgreement();
  }

  @SuppressWarnings("SameParameterValue")
  void upgradeAddColIfMissing(CqlSession session, String tableName, String colName, String type) {
    Metadata metadata = session.getMetadata();
    Optional<KeyspaceMetadata> jj_logging = metadata.getKeyspace("jj_logging");
    jj_logging.ifPresent((keySpace) -> {
      Optional<TableMetadata> fault_tolerant = keySpace.getTable(tableName);
      fault_tolerant.ifPresent((table) -> {
        Optional<ColumnMetadata> error_count = table.getColumn(colName);
        if (error_count.isEmpty()) {
          System.out.println("Upgrading existing table to add error_count column.");
          String replace = UPGRADE_FT_TABLE_ADD_COL
              .replace("<T>", tableName)
              .replace("<C>", colName)
              .replace("<Y>", type);
          logger().info(replace);
          session.execute(replace);
          session.checkSchemaAgreement();
        }
      });
    });
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

  private class SchemaChecker implements Callable<Object> {

    private final CassandraSupport cassandra;

    public SchemaChecker(CassandraSupport cassandra) {
      this.cassandra = cassandra;
    }

    @Override
    public Object call() throws Exception {
      // Note this can get called more than once on startup!
      System.out.println("Table and key space creation thread started");
      boolean tryAgain = true;
      int tryCount = 0;
      // ugly but effective fix for https://github.com/nsoft/jesterj/issues/1
      while (tryAgain) {
        try {
          CqlSession session = getCassandra().getSession();
          ensureBasicSchema(session);
          upgradeAddColIfMissing(session, "fault_tolerant", "error_count", "int");
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

    CassandraSupport getCassandra() {
      return cassandra;
    }
  }
}
