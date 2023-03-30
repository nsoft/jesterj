package org.jesterj.ingest.indexing;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import guru.nidi.graphviz.engine.Format;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.model.Status;
import org.jesterj.ingest.model.impl.PlanImpl;
import org.jesterj.ingest.model.impl.StepImpl;
import org.jesterj.ingest.persistence.Cassandra;
import org.jesterj.ingest.persistence.CassandraSupport;
import org.jesterj.ingest.processors.DocumentCounter;
import org.jesterj.ingest.processors.ErrorPatternTestProcessor;
import org.jesterj.ingest.scanners.ScannerImplTest;
import org.jesterj.ingest.scanners.SimpleFileScanner;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("SameParameterValue")
public class HeuristicFatalFTITest extends ScannerImplTest {

  @SuppressWarnings("unused")
  private static final Logger log = LogManager.getLogger();

  public static final String ERROR_STEP_FILE = "errorStepFile";
  public static final String COUNT_STEP_OTHER = "countStepOther";
  public static final String FILE_SCANNER = "fileScanner";


  @Test
  public void test3errorsMarkedDead() throws Exception {
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

      Plan plan = getSimplePlan("testScan");
      System.out.println(plan.visualize(Format.DOT).toString());

      plan.activate();

      // Expected plan behavior:
      //
      // Feed 44 documents, Error odd numbered documents --> 22 errors, 22 indexed
      // Re-feed 22 errors, again erroring odd numbered docs --> 11 errors, 11 indexed (total 33)
      // Re-feed 11 errors, again erroring odd docs --> 6 errors, 5 indexed (total 38 indexed)
      // Discover 3 consecutive errors for remaining 6 docs, mark 6 docs dead.

      Thread.sleep(3000);  //
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

      Map<String, Map<String, Map<String, AtomicInteger>>> docStatusSummaryCassandra = new HashMap<>();
      countsFromCassandra(support, docStatusSummaryCassandra);

      DocumentCounter otherCount = findCounter(plan, COUNT_STEP_OTHER);

      // we should have seen 17 comedies 10 tragedies and 17 other files from two sources for a total of 88
      int others = otherCount.getScannedDocs().size();

      int othersFileIndexedC = statusCountInTable(docStatusSummaryCassandra, "file_countStepOther", Status.INDEXED);
      int othersFileDeadC = statusCountInTable(docStatusSummaryCassandra, "file_countStepOther", Status.DEAD);

      // this should turn up empty since we let it run to completion and there aren't enough errors to cause anything
      // to error 3 times (and thus become DEAD status)!
      Map<String, List<String>> filesWithoutIndexedStatus = filesWithoutStats(docStatusSummaryCassandra, List.of(Status.INDEXED.toString(), Status.DROPPED.toString()));
      Map<String, List<String>> filesMultipleIndexedStatus = filesWithoutMoreThanNofStat(docStatusSummaryCassandra, Status.INDEXED, 1);

      String message = String.format("" +
              "\nC_OFI:%s" +
              "\nC_OFD:%s" +
              "\nJO:%s" +
              "\nNo Index Stat C*:%s" +
              "\nMultiple Index Stat C*:%s" +
              "\n"
          ,
          othersFileIndexedC, othersFileDeadC, others,
          filesWithoutIndexedStatus, filesMultipleIndexedStatus
      );

      assertEquals("Wrong number INDEXED!" + message,38,othersFileIndexedC);
      assertEquals("Wrong number DEAD!" + message,6,othersFileDeadC);
      assertEquals("Should only have one indexed or dropped status event per document!" + message, 0, filesMultipleIndexedStatus.values().stream().mapToInt(List::size).sum());
      assertEquals("Should have at least one indexed status event per file!" + message, 6, filesWithoutIndexedStatus.values().stream().mapToInt(List::size).sum());

      assertEquals("Incorrect Java Others!" + message, 38, others);

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
    System.out.println(table);
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
      if (keyspaceName == null || !keyspaceName.startsWith("jj_")) {
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

  @SuppressWarnings("SameParameterValue")
  private Plan getSimplePlan(String name) {

    // set up two sources each of which can be paused or have errors before routing
    SimpleFileScanner.Builder fileScannerBuilder = new SimpleFileScanner.Builder();
    StepImpl.Builder fileError = new StepImpl.Builder();


    // Three destinations all of which can be paused or errored

    StepImpl.Builder otherCounter = new StepImpl.Builder();

    // configure our steps

    File tragedies = new File("src/test/resources/test-data");
    fileScannerBuilder.named(FILE_SCANNER)
        .withRoot(tragedies)
        .rememberScannedIds(true)
        .detectChangesViaHashing(true)
        .scanFreqMS(1000);


    String[] fileErrorHolder = new String[1];

    fileError.named(ERROR_STEP_FILE).withProcessor(new ErrorPatternTestProcessor.Builder()
        .named("errorProcFile")
        .withErrorReporter(fileErrorHolder)
        .withErrorPattern(Arrays.asList(TRUE, FALSE)) // error first, succeed second, error third, succeed fourth etc.
        .erroringFromStart(true));
    otherCounter.named(COUNT_STEP_OTHER).withProcessor(getScannedDocRecorder("recordedOther"));




    PlanImpl.Builder planBuilder = new PlanImpl.Builder();
    planBuilder
        .named(name)
        .addStep(fileScannerBuilder)
        .addStep(fileError, FILE_SCANNER)
        .addStep(otherCounter, ERROR_STEP_FILE)
        .withIdField("id");

    return planBuilder.build();
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
