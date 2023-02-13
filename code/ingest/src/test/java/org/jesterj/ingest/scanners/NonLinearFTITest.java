package org.jesterj.ingest.scanners;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.io.Files;
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
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class NonLinearFTITest extends ScannerImplTest {

  @SuppressWarnings("unused")
  private static final Logger log = LogManager.getLogger();
  public static final int PAUSE_MILLIS = 3000;
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
    loadShakespeareToHSQL();
    @SuppressWarnings({"deprecation", "UnstableApiUsage"})
    File tempDir = Files.createTempDir();
    Cassandra.start(tempDir, "127.0.0.1");
    try {

      Plan plan = getPlan("testScan");
      //System.out.println(plan.visualize(Format.DOT).toString());
      // http://magjac.com/graphviz-visual-editor/?dot=digraph%20%22visualize%22%20%7B%0A%22fileScanner%22%20%5B%22color%22%3D%22blue%22%2C%22penwidth%22%3D%223.0%22%5D%0A%22jdbcScanner%22%20%5B%22color%22%3D%22blue%22%2C%22penwidth%22%3D%223.0%22%5D%0A%22fileScanner%22%20-%3E%20%22pauseStepFile%22%0A%22pauseStepFile%22%20-%3E%20%22errorStepFile%22%0A%22errorStepFile%22%20-%3E%20%22copyFileNameToCategoryStep%22%0A%22copyFileNameToCategoryStep%22%20-%3E%20%22copyIdToCategoryStep%22%0A%22copyIdToCategoryStep%22%20-%3E%20%22editCategory%22%0A%22editCategory%22%20-%3E%20%22defaultCategoryToOtherStep%22%0A%22defaultCategoryToOtherStep%22%20-%3E%20%22pauseStepComedy%22%0A%22defaultCategoryToOtherStep%22%20-%3E%20%22pauseStepTragedy%22%0A%22defaultCategoryToOtherStep%22%20-%3E%20%22pauseStepOther%22%0A%22pauseStepComedy%22%20-%3E%20%22errorStepComedy%22%0A%22errorStepComedy%22%20-%3E%20%22countStepComedy%22%0A%22pauseStepTragedy%22%20-%3E%20%22errorStepTragedy%22%0A%22errorStepTragedy%22%20-%3E%20%22countStepTragedy%22%0A%22pauseStepOther%22%20-%3E%20%22errorStepOther%22%0A%22errorStepOther%22%20-%3E%20%22countStepOther%22%0A%22jdbcScanner%22%20-%3E%20%22pauseStepDb%22%0A%22pauseStepDb%22%20-%3E%20%22errorStepDb%22%0A%22errorStepDb%22%20-%3E%20%22copyFileNameToCategoryStep%22%0A%7D

      shortPauses(plan, PAUSE_STEP_COMEDY);
      shortPauses(plan, PAUSE_STEP_TRAGEDY);
      shortPauses(plan, PAUSE_STEP_OTHER);
      plan.activate();
      // now scanner should find all docs, attempt to index them, all marked
      // as processing...
      Thread.sleep(PAUSE_MILLIS / 3 + 2 * PAUSE_MILLIS);
      blockCounters(plan);
      // the pause ever 5 should have let 5 through and then paused for 30 sec
      plan.deactivate();
      assertEquals(30, getScannedDocs(plan, COUNT_STEP_OTHER).size() +
          getScannedDocs(plan, COUNT_STEP_TRAGEDY).size() +
          getScannedDocs(plan, COUNT_STEP_COMEDY).size());
      assertEquals(getCountForCategory(plan, CATEGORY_COUNTER_STEP, "other"), getDocCount(plan, COUNT_STEP_OTHER));
      assertEquals(getCountForCategory(plan, CATEGORY_COUNTER_STEP, "tragedies"), getDocCount(plan, COUNT_STEP_TRAGEDY));
      assertEquals(getCountForCategory(plan, CATEGORY_COUNTER_STEP, "comedies"), getDocCount(plan, COUNT_STEP_COMEDY));
      assertEquals(2, getDocCount(plan, COUNT_STEP_OTHER));
      assertEquals(7, getDocCount(plan, COUNT_STEP_TRAGEDY));
      assertEquals(21, getDocCount(plan, COUNT_STEP_COMEDY));

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

    } finally {
      Cassandra.stop();
    }

  }

  Map<String, DocumentFieldMatchCounter.DocCounted> getScannedDocsForCategory(Plan plan, String stepName, String category) {
    StepImpl test2 = (StepImpl) plan.findStep(stepName);
    Map<String, DocumentFieldMatchCounter.DocCounted> stringDocCountedMap = ((DocumentFieldMatchCounter) test2.getProcessor()).getScannedDocs().get(category);
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
