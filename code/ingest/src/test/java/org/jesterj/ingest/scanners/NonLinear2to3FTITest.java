package org.jesterj.ingest.scanners;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import guru.nidi.graphviz.engine.Format;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.model.Status;
import org.jesterj.ingest.model.impl.NamedBuilder;
import org.jesterj.ingest.model.impl.PlanImpl;
import org.jesterj.ingest.model.impl.StepImpl;
import org.jesterj.ingest.persistence.Cassandra;
import org.jesterj.ingest.persistence.CassandraSupport;
import org.jesterj.ingest.processors.*;
import org.jesterj.ingest.routers.RouteByStepName;
import org.jetbrains.annotations.NotNull;
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
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class NonLinear2to3FTITest extends ScannerImplTest {

  @SuppressWarnings("unused")
  private static final Logger log = LogManager.getLogger();

  // GitHub actions run on VERY slow machines, need to give lots of time for sleeping threads to resume.
  // Crave.io machines are better, but this test still has trouble.
  // Sleeping is a test only behavior meant to give the plan time to process a clearly defined number of documents
  // before turning it off again without hard coding that limit into the plan (so that if documents do flow down
  // incorrect paths or statuses are set multiple times we can still see it). Been working fine on my local
  // machine (1950x processor), but overshooting on GitHub, presumably due to the main test thread not resuming
  // promptly and allowing more pause periods to expire before shutting down the plan. Thus, this ugly hack...
  public static final long PAUSE_MILLIS = 6000L;
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
  public void testScanWithMemoryTwoIntoThree() throws Exception {
    // GitHub: "I want the threads!"
    // Me: "You can't handle the threads!"
    if ((System.getenv("GITHUB_ACTION") != null)) return;

    // works great on my 16 core 32 thread 1950x, I expect it probably works fine on 4 or more cores, GitHub gives us 2
    // which means threads in the plan just keep doing their thing after they were supposed to have shut down, and so
    // some counts come out wrong and fail the test.
    loadShakespeareToHSQL();
    File tempDir = getUniqueTempDir();
    Cassandra.start(tempDir, "127.0.0.1");
    CassandraSupport support = new CassandraSupport();
    try {

      Plan plan = getTwoIntoThreePlan("testScan");
      System.out.println(plan.visualize(Format.DOT).toString());
      // http://magjac.com/graphviz-visual-editor/?dot=digraph%20%22visualize%22%20%7B%0A%22fileScanner%22%20%5B%22color%22%3D%22blue%22%2C%22penwidth%22%3D%223.0%22%5D%0A%22jdbcScanner%22%20%5B%22color%22%3D%22blue%22%2C%22penwidth%22%3D%223.0%22%5D%0A%22fileScanner%22%20-%3E%20%22pauseStepFile%22%0A%22pauseStepFile%22%20-%3E%20%22errorStepFile%22%0A%22errorStepFile%22%20-%3E%20%22copyFileNameToCategoryStep%22%0A%22copyFileNameToCategoryStep%22%20-%3E%20%22copyIdToCategoryStep%22%0A%22copyIdToCategoryStep%22%20-%3E%20%22editCategory%22%0A%22editCategory%22%20-%3E%20%22defaultCategoryToOtherStep%22%0A%22defaultCategoryToOtherStep%22%20-%3E%20%22categoryCounterStep%22%0A%22categoryCounterStep%22%20-%3E%20%22pauseStepComedy%22%0A%22categoryCounterStep%22%20-%3E%20%22pauseStepTragedy%22%0A%22categoryCounterStep%22%20-%3E%20%22pauseStepOther%22%0A%22pauseStepComedy%22%20-%3E%20%22errorStepComedy%22%0A%22errorStepComedy%22%20-%3E%20%22countStepComedy%22%0A%22pauseStepTragedy%22%20-%3E%20%22errorStepTragedy%22%0A%22errorStepTragedy%22%20-%3E%20%22countStepTragedy%22%0A%22pauseStepOther%22%20-%3E%20%22errorStepOther%22%0A%22errorStepOther%22%20-%3E%20%22countStepOther%22%0A%22jdbcScanner%22%20-%3E%20%22pauseStepDb%22%0A%22pauseStepDb%22%20-%3E%20%22errorStepDb%22%0A%22errorStepDb%22%20-%3E%20%22copyFileNameToCategoryStep%22%0A%7D

      // in this test we are throttling the scanners
      longPauses(plan, PAUSE_STEP_DB, (int) PAUSE_MILLIS);
      longPauses(plan, PAUSE_STEP_FILE, (int) PAUSE_MILLIS);

      // But the downstream flows are not throttled
      shortPauses(plan, PAUSE_STEP_COMEDY);
      shortPauses(plan, PAUSE_STEP_TRAGEDY);
      shortPauses(plan, PAUSE_STEP_OTHER);

      plan.activate();
      // now scanner should find all docs, attempt to index them, all marked
      // as processing...
      Thread.sleep((PAUSE_MILLIS / 3) + (2 * PAUSE_MILLIS));  //
      // the pause every 5 should have:
      //   paused for 3 sec and then
      //   let 5 through and then
      //   paused for 3 sec and then
      //   let 5 more through and then
      //   paused for 3 sec

      // During the third pause, the test thread wakes up and blocks further progress
      // deactivation can be slow so be sure nothing gets counted while deactivation is in progress
      blockCounters(plan, true);

      plan.deactivate();
      assertEquals(getCountForCategory(plan, CATEGORY_COUNTER_STEP, "other"), getDocCount(plan, COUNT_STEP_OTHER));
      assertEquals(getCountForCategory(plan, CATEGORY_COUNTER_STEP, "tragedies"), getDocCount(plan, COUNT_STEP_TRAGEDY));
      assertEquals(getCountForCategory(plan, CATEGORY_COUNTER_STEP, "comedies"), getDocCount(plan, COUNT_STEP_COMEDY));

      // which types made it through can vary, so we only can assert the net total. Two bursts of 5 documents for 3
      // destinations should yield 30 total documents making it to the final counter steps.
      int actualIndexed = sizeForCounter(plan, COUNT_STEP_OTHER) +
          sizeForCounter(plan, COUNT_STEP_TRAGEDY) +
          sizeForCounter(plan, COUNT_STEP_COMEDY);
      assertEquals(30, actualIndexed);

      checkAndClearTables(support, 15);
      ///// NOW for a full uninterrupted run as a baseline

      blockCounters(plan, false);
      shortPauses(plan, PAUSE_STEP_COMEDY);
      shortPauses(plan, PAUSE_STEP_TRAGEDY);
      shortPauses(plan, PAUSE_STEP_OTHER);
      shortPauses(plan, PAUSE_STEP_DB);
      shortPauses(plan, PAUSE_STEP_FILE);

      // Now let it run to completion and index everything.
      plan.activate();
      Thread.sleep(PAUSE_MILLIS);
      plan.deactivate();
      Thread.sleep(PAUSE_MILLIS);

      // table by docid by status (count of status occurrences)
      checkFullRun(support, plan);
      ResultSet tables;

      // NOW start up and run again, no new content to index so nothing should change
      plan.activate();
      Thread.sleep(PAUSE_MILLIS);
      plan.deactivate();
      Thread.sleep(PAUSE_MILLIS);

      checkAndClearTables(support, 44);

      // NOW turn on errors for Comedies, and we expect only the table for the comedies output step to
      // have an increase. This should be enough time that the comedies all process and all the errors are successfully
      // reprocessed.

      startErrors(plan, ERROR_STEP_COMEDY);
      plan.activate();
      Thread.sleep(PAUSE_MILLIS);
      plan.deactivate();
      Thread.sleep(PAUSE_MILLIS);

      checkFullRun(support, plan);
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
          assertEquals(44, rowCount); // one entry for each file or DB row
        } else if (tableName.endsWith("_status")) {
          ResultSet example = support.getSession().execute("select outputStepName from " + tableName + " LIMIT 1");
          @SuppressWarnings("DataFlowIssue")
          String stepName = example.one().getString(0);
          if (rowCount != 44 + 44) {
            assertEquals("Found " + rowCount + " for " + stepName, COUNT_STEP_COMEDY, stepName);
            // we can get errors for docs from either scanner
            tablesWithErrors.add(tableName);
            ///////////////////////////////////////////////////////////////////////////////////////
            //
            // THIS is the key test. It shows that we only processed the missing documents and only
            // sent them to the output steps that didn't get them the first time.
            //
            // There are 44 documents total, 3 of 17 comedies from 2 scanners should error out, for a total
            // of 7 errors on the 4th, 9th, 14th, 19th, 24th, 29th, 34th documents first pass and the second
            // pass will feed the 7 errors taking the count to 51 and erroring the 49th document. Finally, a third
            // pass will feed one error doc and as the 52nd document to arrive at the ErrorForthTestProcessor it will
            // safely pass, and be indexed.
            // so these 3 cases are events per document:
            //   - PROCESSING,INDEXED (41 * 2 * 2 events = 164 events)
            //   - PROCESSING,ERROR,PROCESSING,INDEXED (5 of 7 * 4 events = 22 events)
            //   - PROCESSING,ERROR,PROCESSING,ERROR,PROCESSING,INDEXED (1 * 6 events = 6 events)
            // So across 2 tables we should get a total of 164 + 22 + 6 = 192 events
            // It is not possible to predict which table will get hit with the second error.
            // This test has a minuscule chance of failing which would require thread scheduling to completely
            // pause one scanner and all the errors show up for the other scanner type. The build output will have
            // a line that says 'countStepComedy has 88' and another line that says 'countStepComedy has 102'
            // if the failure is indeed spurious.
            //
            rowsBothScanners += rowCount;
            if (tablesWithErrors.size() == 2) {
              int expected = 192;
              if (expected != rowsBothScanners) {
                for (String tableWithErrors : tablesWithErrors) {
                  System.out.println(tableWithErrors);
                  showTable(support, tableWithErrors);
                }
              }
              assertEquals("Wrong number of errors for " + tablesWithErrors, expected, rowsBothScanners);
            }
            System.out.println(stepName + " has " + rowCount);
          } else {
            // Anything else should be 88
            if (COUNT_STEP_COMEDY.equals(stepName)) {
              showTable(support, tableName);
            }
            assertNotEquals("But the errors... why are the errors gone?", COUNT_STEP_COMEDY, stepName);
            System.out.println(stepName + " has " + rowCount);
          }
        } else {
          assertEquals("Found unexpected table" + tableName, "jj_logging.regular", tableName);
        }
      }
      assertEquals(2, tablesWithErrors.size());
    } catch (Throwable e) {
      e.printStackTrace();
      log.error(e);
      dumpTables(support);
      throw e;
    } finally {
      Thread.sleep(100);
      System.out.println("sleeping in finally");
      System.out.flush();
//      // keep these for debugging on crave.io
//      dumpTables(support);
//      Thread.sleep(5000000);
      Cassandra.stop();
    }

  }

  private static void checkFullRun(CassandraSupport support, Plan plan) {
    Map<String, Map<String, Map<String, AtomicInteger>>> docStatusSummaryCassandra = new HashMap<>();
    countsFromCassandra(support, docStatusSummaryCassandra);

    DocumentCounter otherCount = findCounter(plan, COUNT_STEP_OTHER);
    DocumentCounter comedyCount = findCounter(plan, COUNT_STEP_COMEDY);
    DocumentCounter tragedyCount = findCounter(plan, COUNT_STEP_TRAGEDY);

    DocumentFieldMatchCounter fieldMatchCounter = findFieldMatchCounter(plan, CATEGORY_COUNTER_STEP);
    Map<String, Map<String, DocumentFieldMatchCounter.DocCounted>> scannedDocsByValue = fieldMatchCounter.getScannedDocsByValue();
    int othersCatCount = scannedDocsByValue.get("other").keySet().size();
    int comedyCatCount = scannedDocsByValue.get("comedies").keySet().size();
    int tragedyCatCount = scannedDocsByValue.get("tragedies").keySet().size();

    // we should have seen 17 comedies 10 tragedies and 17 other files from two sources for a total of 88
    int others = otherCount.getScannedDocs().size();
    int tragedies = tragedyCount.getScannedDocs().size();
    int comedies = comedyCount.getScannedDocs().size();

    int othersJdbcIndexedC = statusCountInTable(docStatusSummaryCassandra, "jdbc_countStepOther", Status.INDEXED);
    int comedyJdbcIndexedC = statusCountInTable(docStatusSummaryCassandra, "jdbc_countStepComedy", Status.INDEXED);
    int tragedyJdbcIndexedC = statusCountInTable(docStatusSummaryCassandra, "jdbc_countStepTragedy", Status.INDEXED);
    int othersFileIndexedC = statusCountInTable(docStatusSummaryCassandra, "file_countStepOther", Status.INDEXED);
    int comedyFileIndexedC = statusCountInTable(docStatusSummaryCassandra, "file_countStepComedy", Status.INDEXED);
    int tragedyFileIndexedC = statusCountInTable(docStatusSummaryCassandra, "file_countStepTragedy", Status.INDEXED);

    // this should turn up empty since we let it run to completion and there aren't enough errors to cause anything
    // to error 3 times (and thus become DEAD status)!
    Map<String, List<String>> filesWithoutIndexedStatus = filesWithoutStats(docStatusSummaryCassandra, List.of(Status.INDEXED.toString(), Status.DROPPED.toString()));
    Map<String, List<String>> filesMultipleIndexedStatus = filesWithoutMoreThanNofStat(docStatusSummaryCassandra, Status.INDEXED, 1);

    String message = String.format("" +
            "\nC_OJI:%s" +
            "\nC_CJI:%s" +
            "\nC_TJI:%s" +
            "\nC_OFI:%s" +
            "\nC_CFI:%s" +
            "\nC_TFI:%s" +
            "\nJO_CC:%s" +
            "\nJC_CC:%s" +
            "\nJT_CC:%s" +
            "\nJO:%s" +
            "\nJC:%s" +
            "\nJT:%s" +
            "\nNo Index Stat C*:%s" +
            "\nMultiple Index Stat C*:%s"
        ,
        othersJdbcIndexedC, comedyJdbcIndexedC, tragedyJdbcIndexedC, othersFileIndexedC, comedyFileIndexedC,
        tragedyFileIndexedC, othersCatCount, comedyCatCount, tragedyCatCount, others, comedies, tragedies,
        filesWithoutIndexedStatus, filesMultipleIndexedStatus
    );

    assertEquals("Should only have one indexed or dropped status event per document!" + message, 0,filesMultipleIndexedStatus.values().stream().mapToInt(List::size).sum());
    assertEquals("Should have at least one indexed status event per file!" + message, 0,filesWithoutIndexedStatus.values().stream().mapToInt(List::size).sum());

    assertEquals("Incorrect Java Others!" + message, 34, others);
    assertEquals("Incorrect Java Tragedies!" + message, 20, tragedies);
    assertEquals("Incorrect Java Comedies!" + message, 34, comedies);
    assertEquals("Incorrect categoryCount other!" + message,34,othersCatCount);
    assertEquals("Incorrect categoryCount tragedies!" + message,20,tragedyCatCount);
    assertEquals("Incorrect categoryCount comedies!" + message,34,comedyCatCount);
  }

  @NotNull
  private static Map<String, List<String>> filesWithoutStats(Map<String, Map<String, Map<String, AtomicInteger>>> docStatusSummaryCassandra, List<String> statuses) {
    Map<String, List<String>> filesWithoutIndexedStatus = new HashMap<>();
    for (String s : docStatusSummaryCassandra.keySet()) {
      List<String> noIndexStatus = docStatusSummaryCassandra.get(s)
          .entrySet().stream()
          .filter(e -> e.getValue().entrySet().stream().
              noneMatch(ent -> statuses.contains(ent.getKey())))
          .map(Map.Entry::getKey)
          .collect(Collectors.toList());
      filesWithoutIndexedStatus.put(s, noIndexStatus);
    }
    return filesWithoutIndexedStatus;
  }

  @NotNull
  private static Map<String, List<String>> filesWithoutMoreThanNofStat(Map<String, Map<String, Map<String, AtomicInteger>>> docStatusSummaryCassandra, Status indexed, int maxAllowed) {
    Map<String, List<String>> filesWithoutIndexedStatus = new HashMap<>();
    for (String s : docStatusSummaryCassandra.keySet()) {
      List<String> noIndexStatus = docStatusSummaryCassandra.get(s)
          .entrySet().stream()
          .filter(e -> e.getValue().entrySet().stream().
              anyMatch(ent -> ent.getKey().equals(String.valueOf(indexed)) && ent.getValue().intValue() > maxAllowed))
          .map(Map.Entry::getKey)
          .collect(Collectors.toList());
      filesWithoutIndexedStatus.put(s, noIndexStatus);
    }
    return filesWithoutIndexedStatus;
  }

  private static int statusCountInTable(Map<String, Map<String, Map<String, AtomicInteger>>> docStatusSummaryCassandra, String table, Status status) {

    Map<String, Map<String, AtomicInteger>> stringMapMap = docStatusSummaryCassandra.get(table);
    System.out.println( table);
    System.out.println(stringMapMap);
    return stringMapMap
        .entrySet().stream()
        .flatMap(e -> e.getValue().entrySet().stream()
            .filter(ent -> ent.getKey().equals(String.valueOf(status))))
        .map(Map.Entry::getValue)
        .mapToInt(AtomicInteger::intValue)
        .sum();
  }

  private static void countsFromCassandra(CassandraSupport support, Map<String, Map<String, Map<String, AtomicInteger>>> docStatusSummary) {
    ResultSet tables = support.getSession().execute("SELECT table_name, keyspace_name from system_schema.tables");
    for (Row table : tables) {
      String keyspaceName = table.getString("keyspace_name");
      if (keyspaceName == null || !keyspaceName.startsWith("jj_") ) {
        continue;
      }
      String tableName = keyspaceName + "." + table.getString("table_name");
      if (tableName.endsWith("_hash") || tableName.endsWith("regular")) {
        continue;
      }
      ResultSet rows = support.getSession().execute("select docid,status,outputStepName from " + tableName);

      for (Row row : rows) {
        String id = row.getString(0);
        String status = row.getString(1);
        String outputStep = row.getString(2);
        String tableDecoded = (id.startsWith("file") ? "file_" : "jdbc_") + outputStep;
        docStatusSummary.computeIfAbsent(tableDecoded, k -> new HashMap<>()).computeIfAbsent(id, k -> new HashMap<>()).computeIfAbsent(status, k -> new AtomicInteger(0)).incrementAndGet();
      }
    }
  }

  private static void checkAndClearTables(CassandraSupport support, int numCountedExpected) {
    ResultSet tables;
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
        assertEquals(44, rowCount); // one entry for each file or DB row
      } else if (tableName.endsWith("_status")) {
        assertEquals(44 + numCountedExpected, rowCount); // initial processing, and all of them got to indexed
      } else {
        assertEquals("Found unexpected table" + tableName, "jj_logging.regular", tableName);
      }
      support.getSession().execute("truncate table " + tableName);
      count = support.getSession().execute("select count(*) from " + tableName);
      //noinspection DataFlowIssue
      assertEquals(0L, count.one().getLong(0));
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
  private Plan getTwoIntoThreePlan(String name) {

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
            .named("CategoryRouter")
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
