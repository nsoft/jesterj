package org.jesterj.ingest.scanners;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import guru.nidi.graphviz.engine.Format;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.model.impl.NamedBuilder;
import org.jesterj.ingest.model.impl.PlanImpl;
import org.jesterj.ingest.model.impl.StepImpl;
import org.jesterj.ingest.persistence.Cassandra;
import org.jesterj.ingest.persistence.CassandraSupport;
import org.jesterj.ingest.processors.*;
import org.jesterj.ingest.routers.RouteByStepName;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class NonLinearFTITest extends ScannerImplTest {

  @SuppressWarnings("unused")
  private static final Logger log = LogManager.getLogger();

  // GitHub actions run on VERY slow machines, need to give lots of time for sleeping threads to resume.
  // Sleeping is a test only behavior meant to give the plan time to process a clearly defined number of documents
  // before turning it off again without hard coding that limit into the plan (so that if documents do flow down
  // incorrect paths or statuses are set multiple times we can still see it). Been working fine on my local
  // machine (1950x processor), but overshooting on GitHub, presumably due to the main test thread not resuming
  // promptly and allowing more pause periods to expire before shutting down the plan. Thus, this ugly hack...
  public static final long PAUSE_MILLIS = 3000L;
  public static final String PAUSE_STEP_DB = "pauseStepDb";
  public static final String PAUSE_STEP_FILE = "pauseStepFile";
  public static final String PAUSE_STEP_COMEDY = "pauseStepComedy";
  public static final String PAUSE_STEP_TRAGEDY = "pauseStepTragedy";
  public static final String PAUSE_STEP_OTHER = "pauseStepOther";
  public static final String ERROR_STEP_FILE = "errorStepFile";
  public static final String ERROR_STEP_DB = "errorStepDb";
  public static final String ERROR_STEP_COMEDY = "errorStepComedy";
  public static final String ERROR_STEP_TRAGEDY = "errorStepTragedy";
  public static final String ERROR_STEP_OTHER = "errorStepOther";
  public static final String COUNT_STEP_TRAGEDY = "countStepTragedy";
  public static final String COUNT_STEP_COMEDY = "countStepComedy";
  public static final String COUNT_STEP_OTHER = "countStepOther";
  public static final String COPY_ID_TO_CATEGORY_STEP = "copyIdToCategoryStep";
  public static final String COPY_FILE_NAME_TO_CATEGORY_STEP = "copyFileNameToCategoryStep";
  public static final String EDIT_CATEGORY = "editCategory";
  public static final String DEFAULT_CATEGORY_TO_OTHER_STEP = "defaultCategoryToOtherStep";
  public static final String CATEGORY_COUNTER_STEP = "categoryCounterStep";
  public static final String FILE_SCANNER = "fileScanner";
  public static final String JDBC_SCANNER = "jdbcScanner";


  @Test
  public void testScanWithMemory() throws Exception {
    // GitHub: "I want the threads!"
    // Me: "You can't handle the threads!"
    if((System.getenv("GITHUB_ACTION") != null)) return;

    // works great on my 16 core 32 thread 1950x, I expect it probably works fine on 4 or more cores, GitHub gives us 2
    // which means threads in the plan just keep doing their thing after they were supposed to have shut down, and so
    // some counts come out wrong and fail the test.
    loadShakespeareToHSQL();
    File tempDir = getUniqueTempDir();
    Cassandra.start(tempDir, "127.0.0.1");
    try {

      Plan plan = getPlan("testScan");
      System.out.println(plan.visualize(Format.DOT).toString());
      // http://magjac.com/graphviz-visual-editor/?dot=digraph%20%22visualize%22%20%7B%0A%22fileScanner%22%20%5B%22color%22%3D%22blue%22%2C%22penwidth%22%3D%223.0%22%5D%0A%22jdbcScanner%22%20%5B%22color%22%3D%22blue%22%2C%22penwidth%22%3D%223.0%22%5D%0A%22fileScanner%22%20-%3E%20%22pauseStepFile%22%0A%22pauseStepFile%22%20-%3E%20%22errorStepFile%22%0A%22errorStepFile%22%20-%3E%20%22copyFileNameToCategoryStep%22%0A%22copyFileNameToCategoryStep%22%20-%3E%20%22copyIdToCategoryStep%22%0A%22copyIdToCategoryStep%22%20-%3E%20%22editCategory%22%0A%22editCategory%22%20-%3E%20%22defaultCategoryToOtherStep%22%0A%22defaultCategoryToOtherStep%22%20-%3E%20%22categoryCounterStep%22%0A%22categoryCounterStep%22%20-%3E%20%22pauseStepComedy%22%0A%22categoryCounterStep%22%20-%3E%20%22pauseStepTragedy%22%0A%22categoryCounterStep%22%20-%3E%20%22pauseStepOther%22%0A%22pauseStepComedy%22%20-%3E%20%22errorStepComedy%22%0A%22errorStepComedy%22%20-%3E%20%22countStepComedy%22%0A%22pauseStepTragedy%22%20-%3E%20%22errorStepTragedy%22%0A%22errorStepTragedy%22%20-%3E%20%22countStepTragedy%22%0A%22pauseStepOther%22%20-%3E%20%22errorStepOther%22%0A%22errorStepOther%22%20-%3E%20%22countStepOther%22%0A%22jdbcScanner%22%20-%3E%20%22pauseStepDb%22%0A%22pauseStepDb%22%20-%3E%20%22errorStepDb%22%0A%22errorStepDb%22%20-%3E%20%22copyFileNameToCategoryStep%22%0A%7D

      shortPauses(plan, PAUSE_STEP_COMEDY);
      shortPauses(plan, PAUSE_STEP_TRAGEDY);
      shortPauses(plan, PAUSE_STEP_OTHER);
      plan.activate();
      // now scanner should find all docs, attempt to index them, all marked
      // as processing...
      Thread.sleep(PAUSE_MILLIS / 3 + 2L * PAUSE_MILLIS);
      blockCounters(plan, true);
      // the pause ever 5 should have let 5 through and then paused for 30 sec
      plan.deactivate();
      assertEquals(getCountForCategory(plan, CATEGORY_COUNTER_STEP, "other"), getDocCount(plan, COUNT_STEP_OTHER));
      assertEquals(getCountForCategory(plan, CATEGORY_COUNTER_STEP, "tragedies"), getDocCount(plan, COUNT_STEP_TRAGEDY));
      assertEquals(getCountForCategory(plan, CATEGORY_COUNTER_STEP, "comedies"), getDocCount(plan, COUNT_STEP_COMEDY));

      int actualIndexed = sizeForCounter(plan, COUNT_STEP_OTHER) +
          sizeForCounter(plan, COUNT_STEP_TRAGEDY) +
          sizeForCounter(plan, COUNT_STEP_COMEDY);
      assertEquals(30, actualIndexed);

      CassandraSupport support = new CassandraSupport();

      ResultSet tables = support.getSession().execute("SELECT table_name, keyspace_name from system_schema.tables");
      for (Row table : tables) {
        String keyspaceName = table.getString("keyspace_name");
        if (keyspaceName == null || !keyspaceName.startsWith("jj_")) {
          continue;
        }
        String tableName = keyspaceName + "." + table.getString("table_name");
        System.out.print(tableName + " ");
        ResultSet count = support.getSession().execute("select count(*) from " + tableName);
        @SuppressWarnings("DataFlowIssue")
        long rowCount = count.one().getLong(0);
        if (tableName.endsWith("_hash")) {
          assertEquals(44,rowCount); // one entry for each file or DB row
        }
        if (tableName.endsWith("_status")) {
          assertEquals(44 + 15, rowCount ); // initial processing, and 15 of them got to indexed
        }
        System.out.print(rowCount + " --> ");

        support.getSession().execute("truncate table " + tableName);
        count = support.getSession().execute("select count(*) from " + tableName);
        //noinspection DataFlowIssue
        System.out.println(count.one().getLong(0) );
      }

      ///// NOW for a full uninterrupted run as a baseline

      blockCounters(plan, false);
      shortPauses(plan, PAUSE_STEP_COMEDY);
      shortPauses(plan, PAUSE_STEP_TRAGEDY);
      shortPauses(plan, PAUSE_STEP_OTHER);
      shortPauses(plan, PAUSE_STEP_DB);
      shortPauses(plan, PAUSE_STEP_FILE);

      // Now let it run to completion and index everything.
      plan.activate();
      Thread.sleep(3*PAUSE_MILLIS);
      plan.deactivate();
      Thread.sleep(PAUSE_MILLIS);

      tables = support.getSession().execute("SELECT table_name, keyspace_name from system_schema.tables");
      for (Row table : tables) {
        String keyspaceName = table.getString("keyspace_name");
        if (keyspaceName == null || !keyspaceName.startsWith("jj_")) {
          continue;
        }
        String tableName = keyspaceName + "." + table.getString("table_name");
        ResultSet count = support.getSession().execute("select count(*) from " + tableName);
        @SuppressWarnings("DataFlowIssue")
        long rowCount = count.one().getLong(0);
        if (tableName.endsWith("_hash")) {
          assertEquals(44,rowCount); // one entry for each file or DB row
        } else if (tableName.endsWith("_status")) {
          System.out.println("Testing table:" + tableName);
          assertEquals(44 + 44, rowCount ); // initial processing, and all of them got to indexed
        } else {
          assertEquals("Found unexpected table" + tableName, "jj_logging.regular", tableName);
        }

        // NOTE intentionally NOT truncating tables this time

      }

      // NOW start up and run again, no new content to index so nothing should change
      plan.activate();
      Thread.sleep(3*PAUSE_MILLIS );
      plan.deactivate();
      Thread.sleep(PAUSE_MILLIS);

      tables = support.getSession().execute("SELECT table_name, keyspace_name from system_schema.tables");
      for (Row table : tables) {
        String keyspaceName = table.getString("keyspace_name");
        if (keyspaceName == null || !keyspaceName.startsWith("jj_")) {
          continue;
        }
        String tableName = keyspaceName + "." + table.getString("table_name");
        ResultSet count = support.getSession().execute("select count(*) from " + tableName);
        @SuppressWarnings("DataFlowIssue")
        long rowCount = count.one().getLong(0);
        if (tableName.endsWith("_hash")) {
          assertEquals(44,rowCount); // one entry for each file or DB row
        } else if (tableName.endsWith("_status")) {
          assertEquals(44 + 44, rowCount ); // initial processing, and all of them got to indexed
        } else {
          assertEquals("Found unexpected table" + tableName, "jj_logging.regular", tableName);
        }
        support.getSession().execute("truncate table " + tableName);
        count = support.getSession().execute("select count(*) from " + tableName);
        //noinspection DataFlowIssue
        assertEquals(0L,count.one().getLong(0));
      }

      // NOW turn on errors for Comedies, and we expect only the table for the comedies output step to
      // have an increase. This should be enough time that the comedies all process and all the errors are successfully
      // reprocessed.

      startErrors(plan, ERROR_STEP_COMEDY);
      plan.activate();
      Thread.sleep(3*PAUSE_MILLIS);
      plan.deactivate();
      Thread.sleep(PAUSE_MILLIS);

      tables = support.getSession().execute("SELECT table_name, keyspace_name from system_schema.tables");
      int rowsBothScanners = 0;
      Set<String> tablesWithErrors = new HashSet<>();
      for (Row table : tables) {
        String keyspaceName = table.getString("keyspace_name");
        if (keyspaceName == null || !keyspaceName.startsWith("jj_")) {
          continue;
        }
        String tableName = keyspaceName + "." + table.getString("table_name");
        ResultSet count = support.getSession().execute("select count(*) from " + tableName);
        @SuppressWarnings("DataFlowIssue")
        long rowCount = count.one().getLong(0);
        if (tableName.endsWith("_hash")) {
          assertEquals(44,rowCount); // one entry for each file or DB row
        } else if (tableName.endsWith("_status")) {
          ResultSet example = support.getSession().execute("select outputStepName from " + tableName + " LIMIT 1");
          @SuppressWarnings("DataFlowIssue")
          String stepName = example.one().getString(0);
          if (rowCount != 44+44) {
            assertEquals("Found " + rowCount + " for " + stepName,COUNT_STEP_COMEDY, stepName);
            // we can get errors for docs from either scanner
            tablesWithErrors.add(tableName);
            ///////////////////////////////////////////////////////////////////////////////////////
            //
            // THIS is the key test. It shows that we only processed the missing documents and only
            // sent them to the output steps that didn't get them the first time.
            //
            // There are 44 documents total, 3 of 17 comedies from 2 scanner should error out, (for a total
            // of 6 errors on the first pass and the second pass will feed the 6 errors, one of which will
            // error a second time so these 3 cases are events per document:
            //   - PROCESSING,INDEXED (41 * 2 * 2 events = 164 events)
            //   - PROCESSING,ERROR,PROCESSING,INDEXED (5 of 6 * 4 events = 20 events)
            //   - PROCESSING,ERROR,PROCESSING,ERROR,PROCESSING,INDEXED (1 * 6 events = 6 events)
            // So across 2 tables we should get a total of 164 + 20 + 6 = 190 events
            // It is not possible to predict which table will get hit with the second error.
            // This test has a minuscule chance of failing which would require thread scheduling to completely
            // pause one scanner and all the errors show up for the other scanner type. The build output will have
            // a line that says 'countStepComedy has 88' and another line that says 'countStepComedy has 102'
            // if the failure is indeed spurious.
            //
            rowsBothScanners += rowCount;
            if (tablesWithErrors.size() == 2) {
              int expected = 190;
              if (expected != rowsBothScanners) {
                for (String tableWithErrors : tablesWithErrors) {
                  System.out.println(tableWithErrors);
                  showTable(support, tableWithErrors);
                }
              }
              assertEquals(expected, rowsBothScanners);
            }
            System.out.println(stepName + " has " + rowCount);
          } else {
            // Anything else should be 88
            if (COUNT_STEP_COMEDY.equals(stepName)) {
              showTable(support, tableName);
            }
            assertNotEquals("But my errors... what happened to my errors?", COUNT_STEP_COMEDY, stepName);
            System.out.println(stepName + " has " + rowCount);
          }
        } else {
          assertEquals("Found unexpected table" + tableName, "jj_logging.regular", tableName);
        }
      }
      assertEquals( 2,tablesWithErrors.size());
    } catch (Exception e) {
      e.printStackTrace();
      log.error(e);
    }finally {
      Thread.sleep(100);
      System.out.println("sleeping in finally");
      System.out.flush();
      //Thread.sleep(5000000);
      Cassandra.stop();
    }

  }

  private static void showTable(CassandraSupport support, String tableName) {
    ResultSet showMe = support.getSession().execute("select * from " + tableName);
    int rowNum = 1;
    for (Row row : showMe) {
      System.out.println(rowNum++ +") " + row.getFormattedContents());
    }
  }

  Map<String, DocumentFieldMatchCounter.DocCounted> getScannedDocsForCategory(Plan plan, String stepName, String category) {
    StepImpl test2 = (StepImpl) plan.findStep(stepName);
    Map<String, DocumentFieldMatchCounter.DocCounted> stringDocCountedMap = ((DocumentFieldMatchCounter) test2.getProcessor()).getScannedDocsByValue().get(category);
    return stringDocCountedMap != null ? stringDocCountedMap : new HashMap<>();
  }

  @SuppressWarnings("SameParameterValue")
  int getCountForCategory(Plan plan, String stepName, String category) {
    return getScannedDocsForCategory(plan, stepName, category).size();
  }

  @SuppressWarnings("SameParameterValue")
  private Plan getPlan(String name) {

    // set up two sources each of which can be paused or have errors before routing
    SimpleFileScanner.Builder fileScannerBuilder = new SimpleFileScanner.Builder();
    JdbcScanner.Builder dbScannerBuilder = new JdbcScanner.Builder();
    StepImpl.Builder filePause = new StepImpl.Builder();
    StepImpl.Builder dbPause = new StepImpl.Builder();
    StepImpl.Builder fileError = new StepImpl.Builder();
    StepImpl.Builder dbError = new StepImpl.Builder();

    // two steps for which all documents flow through
    StepImpl.Builder defaultCategoryOther = new StepImpl.Builder();
    StepImpl.Builder copyIdToCategory = new StepImpl.Builder();
    StepImpl.Builder copyFileNameToCategory = new StepImpl.Builder();
    StepImpl.Builder editCategory = new StepImpl.Builder();
    StepImpl.Builder categoryCounter = new StepImpl.Builder();

    // Three destinations all of which can be paused or errored
    StepImpl.Builder tragedyPause = new StepImpl.Builder();
    StepImpl.Builder comedyPause = new StepImpl.Builder();
    StepImpl.Builder othersPause = new StepImpl.Builder();
    StepImpl.Builder tragedyError = new StepImpl.Builder();
    StepImpl.Builder comedyError = new StepImpl.Builder();
    StepImpl.Builder otherError = new StepImpl.Builder();
    StepImpl.Builder tragedyCounter = new StepImpl.Builder();
    StepImpl.Builder comedyCounter = new StepImpl.Builder();
    StepImpl.Builder otherCounter = new StepImpl.Builder();

    // configure our steps

    File tragedies = new File("src/test/resources/test-data");
    fileScannerBuilder.named(FILE_SCANNER)
        .withRoot(tragedies)
        .rememberScannedIds(true)
        .detectChangesViaHashing(true)
        .scanFreqMS(PAUSE_MILLIS / 4);

    dbScannerBuilder.named(JDBC_SCANNER)
        .withAutoCommit(true)
        .withFetchSize(1000)
        .withJdbcDriver("org.hsqldb.jdbc.JDBCDriver")
        .withJdbcPassword("")
        .withJdbcUrl("jdbc:hsqldb:mem:shakespeare;ifexists=true")
        .withJdbcUser("SA")
        .withPKColumn("ID")
        .representingTable("play")
        .withQueryTimeout(3600)
        .withSqlStatement("SELECT * FROM play order by id desc") // reverse order so that not duplicating file scanner
        .rememberScannedIds(true)
        .detectChangesViaHashing(true)
        .withContentColumn("play_text")
        .scanFreqMS(PAUSE_MILLIS / 4);

    dbPause.named(PAUSE_STEP_DB)
        .withProcessor(new PauseEveryFiveTestProcessor.Builder()
            .pausingFor(PAUSE_MILLIS).named("pauseProcDb"));

    filePause.named(PAUSE_STEP_FILE)
        .withProcessor(new PauseEveryFiveTestProcessor.Builder()
            .pausingFor(PAUSE_MILLIS).named("pauseProcFile"));

    comedyPause.named(PAUSE_STEP_COMEDY)
        .withProcessor(new PauseEveryFiveTestProcessor.Builder()
            .pausingFor(PAUSE_MILLIS).named("pauseProcComedy"));

    tragedyPause.named(PAUSE_STEP_TRAGEDY)
        .withProcessor(new PauseEveryFiveTestProcessor.Builder()
            .pausingFor(PAUSE_MILLIS).named("pauseProcTragedy"));

    othersPause.named(PAUSE_STEP_OTHER)
        .withProcessor(new PauseEveryFiveTestProcessor.Builder()
            .pausingFor(PAUSE_MILLIS).named("pauseProcOther"));

    String[] fileErrorHolder = new String[1];
    String[] dbErrorHolder = new String[1];
    String[] comedyErrorHolder = new String[1];
    String[] tragedyErrorHolder = new String[1];
    String[] otherErrorHolder = new String[1];

    fileError.named(ERROR_STEP_FILE).withProcessor(errorProc(fileErrorHolder, "errorProcFile"));
    dbError.named(ERROR_STEP_DB).withProcessor(errorProc(dbErrorHolder, "errorProcDb"));
    comedyError.named(ERROR_STEP_COMEDY).withProcessor(errorProc(comedyErrorHolder, "errorProcComedy"));
    tragedyError.named(ERROR_STEP_TRAGEDY).withProcessor(errorProc(tragedyErrorHolder, "errorProcTragedy"));
    otherError.named(ERROR_STEP_OTHER).withProcessor(errorProc(otherErrorHolder, "errorProcOther"));

    tragedyCounter.named(COUNT_STEP_TRAGEDY).withProcessor(getScannedDocRecorder("recordedTragedies"));
    comedyCounter.named(COUNT_STEP_COMEDY).withProcessor(getScannedDocRecorder("recordedComedies"));
    otherCounter.named(COUNT_STEP_OTHER).withProcessor(getScannedDocRecorder("recordedOther"));

    copyIdToCategory.named(COPY_ID_TO_CATEGORY_STEP)
        .withProcessor(new CopyField.Builder()
            .named("copyIdToCategoryProc")
            .from("id").into("category")
            .retainingOriginal(true));

    copyFileNameToCategory.named(COPY_FILE_NAME_TO_CATEGORY_STEP)
        .withProcessor(new CopyField.Builder()
            .named("copyFilenameToCategoryProc")
            .from("FILENAME").into("category")
            .retainingOriginal(true));

    editCategory.named(EDIT_CATEGORY)
        .withProcessor(new RegexValueReplace.Builder()
            .named("editCategory")
            .editingField("category")
            .withRegex(".*/(comedies|tragedies)/.*")
            .andReplacement("$1")
            .discardingUnmatched());

    defaultCategoryOther.named(DEFAULT_CATEGORY_TO_OTHER_STEP)
        .withProcessor(new SetStaticValue.Builder()
            .named("defaultCategoryToOtherProc")
            .skipIfHasValue()
            .adding("category")
            .withValue("other"));

    categoryCounter.named(CATEGORY_COUNTER_STEP)
        .withProcessor(new DocumentFieldMatchCounter.Builder()
            .named("categoryCounterProc")
            .matchesForField("category")
        )
        .routingBy(new RouteByStepName.Builder()
            .keyValuesInField("category")
            .mappingValueFromTo("other", PAUSE_STEP_OTHER)
            .mappingValueFromTo("comedies", PAUSE_STEP_COMEDY)
            .mappingValueFromTo("tragedies", PAUSE_STEP_TRAGEDY));

    PlanImpl.Builder planBuilder = new PlanImpl.Builder();
    planBuilder
        .named(name)

        .addStep(fileScannerBuilder)
        .addStep(filePause, FILE_SCANNER)
        .addStep(fileError, PAUSE_STEP_FILE)

        .addStep(dbScannerBuilder)
        .addStep(dbPause, JDBC_SCANNER)
        .addStep(dbError, PAUSE_STEP_DB)

        .addStep(copyFileNameToCategory, ERROR_STEP_DB, ERROR_STEP_FILE) // two sources join
        .addStep(copyIdToCategory, COPY_FILE_NAME_TO_CATEGORY_STEP)
        .addStep(editCategory, COPY_ID_TO_CATEGORY_STEP)
        .addStep(defaultCategoryOther, EDIT_CATEGORY)
        .addStep(categoryCounter, DEFAULT_CATEGORY_TO_OTHER_STEP)

        // split to three destinations
        .addStep(comedyPause, CATEGORY_COUNTER_STEP)
        .addStep(tragedyPause, CATEGORY_COUNTER_STEP)
        .addStep(othersPause, CATEGORY_COUNTER_STEP)

        .addStep(comedyError, PAUSE_STEP_COMEDY)
        .addStep(tragedyError, PAUSE_STEP_TRAGEDY)
        .addStep(otherError, PAUSE_STEP_OTHER)

        .addStep(comedyCounter, ERROR_STEP_COMEDY)
        .addStep(tragedyCounter, ERROR_STEP_TRAGEDY)
        .addStep(otherCounter, ERROR_STEP_OTHER)

        .withIdField("id");

    return planBuilder.build();
  }

  private static NamedBuilder<? extends DocumentProcessor> errorProc(String[] errorId, String name) {
    return new ErrorFourthTestProcessor.Builder()
        .named(name)
        .withErrorReporter(errorId)
        .erroringFromStart(false);
  }

  private void loadShakespeareToHSQL() throws SQLException, IOException {
    AtomicInteger idNext = new AtomicInteger(1);
    Connection c = DriverManager.getConnection("jdbc:hsqldb:mem:shakespeare", "SA", "");
    PreparedStatement createTable = c.prepareStatement(
        "CREATE TABLE play (id varchar(16),name varchar(64),play_text clob,fileName varchar(1024))");
    createTable.execute();
    @SuppressWarnings("SqlResolve")
    PreparedStatement insert = c.prepareStatement("INSERT INTO play VALUES (?,?,?,?)");

    File tragedies = new File("src/test/resources/test-data");
    java.nio.file.Files.walkFileTree(tragedies.toPath(), new FileVisitor<>() {
      @Override
      public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        try {
          insert.setString(1, String.valueOf(idNext.getAndIncrement()));
          String uri = file.toUri().toString();
          insert.setString(2, uri.substring(uri.lastIndexOf(File.separator) + 1));
          String text = new String(java.nio.file.Files.readAllBytes(file));
          insert.setString(3, text);
          insert.setString(4, file.toFile().toString());
          insert.execute();
        } catch (SQLException e) {
          throw new IOException(e);
        }
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
        throw new IOException();
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
        return FileVisitResult.CONTINUE;
      }
    });
  }
}
