package org.jesterj.ingest.indexing;

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
import org.jesterj.ingest.routers.DuplicateToAll;
import org.jesterj.ingest.routers.RoundRobinRouter;
import org.jesterj.ingest.scanners.ScannerImplTest;
import org.jesterj.ingest.scanners.SimpleFileScanner;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class NonNonDeterministicRouterFTITest extends ScannerImplTest {

  @SuppressWarnings("unused")
  private static final Logger log = LogManager.getLogger();

  // GitHub actions run on VERY slow machines, need to give lots of time for sleeping threads to resume.
  // Sleeping is a test only behavior meant to give the plan time to process a clearly defined number of documents
  // before turning it off again without hard coding that limit into the plan (so that if documents do flow down
  // incorrect paths or statuses are set multiple times we can still see it). Been working fine on my local
  // machine (1950x processor), but overshooting on GitHub, presumably due to the main test thread not resuming
  // promptly and allowing more pause periods to expire before shutting down the plan. Thus, this ugly hack...
  public static final long PAUSE_MILLIS = 6000L;
  public static final String PAUSE_STEP_DB = "pauseStepDb";
  public static final String PAUSE_STEP_FILE = "pauseStepFile";
  public static final String PAUSE_STEP_CounterB = "pauseStepCounterB";
  public static final String PAUSE_STEP_CounterA = "pauseStepCounterA";
  public static final String PAUSE_STEP_CounterC = "pauseStepCounterC";
  public static final String ERROR_STEP_FILE = "errorStepFile";
  public static final String ERROR_STEP_DB = "errorStepDb";
  public static final String ERROR_STEP_CounterB = "errorStepCounterB";
  public static final String ERROR_STEP_CounterA = "errorStepCounterA";
  public static final String ERROR_STEP_CounterC = "errorStepCounterC";
  public static final String COUNT_STEP_CounterA = "countStepCounterA";
  public static final String COUNT_STEP_CounterB = "countStepCounterB";
  public static final String COUNT_STEP_CounterC = "countStepCounterC";
  public static final String COPY_ID_TO_CATEGORY_STEP = "copyIdToCategoryStep";
  public static final String EDIT_CATEGORY = "editCategory";
  public static final String DEFAULT_CATEGORY_TO_OTHER_STEP = "defaultCategoryToOther";
  public static final String CATEGORY_COUNTER_STEP = "categoryCounterStep";
  public static final String FILE_SCANNER = "fileScanner";
  public static final String NO_OP_A_STEP = "noOpAStep";
  public static final String NO_OP_B_STEP = "noOpBStep";


  @Test
  public void testScanWithMemoryRoundRobinIntoThree() throws InterruptedException {

    File tempDir = getUniqueTempDir();
    Cassandra.start(tempDir, "127.0.0.1");
    CassandraSupport support = new CassandraSupport();

    try {
      Plan plan = getPlan();
      System.out.println(plan.visualize(Format.DOT).toString());
      // http://magjac.com/graphviz-visual-editor/?dot=digraph%20%22visualize%22%20%7B%0A%22fileScanner%22%20%5B%22color%22%3D%22blue%22%2C%22penwidth%22%3D%222.0%22%2C%22style%22%3D%22filled%22%2C%22fillcolor%22%3D%22white%22%5D%0A%22pauseStepFile%22%20%5B%22color%22%3D%22black%22%2C%22penwidth%22%3D%222.0%22%2C%22style%22%3D%22filled%22%2C%22fillcolor%22%3D%22white%22%5D%0A%22errorStepFile%22%20%5B%22color%22%3D%22black%22%2C%22penwidth%22%3D%222.0%22%2C%22style%22%3D%22filled%22%2C%22fillcolor%22%3D%22white%22%5D%0A%22copyIdToCategoryStep%22%20%5B%22color%22%3D%22black%22%2C%22penwidth%22%3D%222.0%22%2C%22style%22%3D%22filled%22%2C%22fillcolor%22%3D%22white%22%5D%0A%22editCategory%22%20%5B%22color%22%3D%22black%22%2C%22penwidth%22%3D%222.0%22%2C%22style%22%3D%22filled%22%2C%22fillcolor%22%3D%22white%22%5D%0A%22defaultCategoryToOther%5Cn%28DuplicateToAll%29%22%20%5B%22color%22%3D%22black%22%2C%22penwidth%22%3D%222.0%22%2C%22style%22%3D%22filled%22%2C%22fillcolor%22%3D%22%23AAFFAA%22%5D%0A%22noOpBStep%22%20%5B%22color%22%3D%22black%22%2C%22penwidth%22%3D%222.0%22%2C%22style%22%3D%22filled%22%2C%22fillcolor%22%3D%22white%22%5D%0A%22categoryCounterStep%5Cn%28RoundRobinRouter%29%22%20%5B%22color%22%3D%22red%22%2C%22penwidth%22%3D%222.0%22%2C%22style%22%3D%22filled%22%2C%22fillcolor%22%3D%220.7%200.3%201.0%22%5D%0A%22pauseStepCounterA%22%20%5B%22color%22%3D%22black%22%2C%22penwidth%22%3D%222.0%22%2C%22style%22%3D%22filled%22%2C%22fillcolor%22%3D%22white%22%5D%0A%22errorStepCounterA%22%20%5B%22color%22%3D%22black%22%2C%22penwidth%22%3D%222.0%22%2C%22style%22%3D%22filled%22%2C%22fillcolor%22%3D%22white%22%5D%0A%22countStepCounterA%22%20%5B%22color%22%3D%22red%22%2C%22penwidth%22%3D%222.0%22%2C%22style%22%3D%22filled%22%2C%22fillcolor%22%3D%22white%22%5D%0A%22pauseStepCounterB%22%20%5B%22color%22%3D%22black%22%2C%22penwidth%22%3D%222.0%22%2C%22style%22%3D%22filled%22%2C%22fillcolor%22%3D%22white%22%5D%0A%22errorStepCounterB%22%20%5B%22color%22%3D%22black%22%2C%22penwidth%22%3D%222.0%22%2C%22style%22%3D%22filled%22%2C%22fillcolor%22%3D%22white%22%5D%0A%22countStepCounterB%22%20%5B%22color%22%3D%22red%22%2C%22penwidth%22%3D%222.0%22%2C%22style%22%3D%22filled%22%2C%22fillcolor%22%3D%22white%22%5D%0A%22pauseStepCounterC%22%20%5B%22color%22%3D%22black%22%2C%22penwidth%22%3D%222.0%22%2C%22style%22%3D%22filled%22%2C%22fillcolor%22%3D%22white%22%5D%0A%22errorStepCounterC%22%20%5B%22color%22%3D%22black%22%2C%22penwidth%22%3D%222.0%22%2C%22style%22%3D%22filled%22%2C%22fillcolor%22%3D%22white%22%5D%0A%22countStepCounterC%22%20%5B%22color%22%3D%22red%22%2C%22penwidth%22%3D%222.0%22%2C%22style%22%3D%22filled%22%2C%22fillcolor%22%3D%22white%22%5D%0A%22noOpAStep%22%20%5B%22color%22%3D%22black%22%2C%22penwidth%22%3D%222.0%22%2C%22style%22%3D%22filled%22%2C%22fillcolor%22%3D%22white%22%5D%0A%22fileScanner%22%20-%3E%20%22pauseStepFile%22%0A%22pauseStepFile%22%20-%3E%20%22errorStepFile%22%0A%22errorStepFile%22%20-%3E%20%22copyIdToCategoryStep%22%0A%22copyIdToCategoryStep%22%20-%3E%20%22editCategory%22%0A%22editCategory%22%20-%3E%20%22defaultCategoryToOther%5Cn%28DuplicateToAll%29%22%0A%22defaultCategoryToOther%5Cn%28DuplicateToAll%29%22%20-%3E%20%22noOpBStep%22%0A%22defaultCategoryToOther%5Cn%28DuplicateToAll%29%22%20-%3E%20%22noOpAStep%22%0A%22noOpBStep%22%20-%3E%20%22categoryCounterStep%5Cn%28RoundRobinRouter%29%22%0A%22categoryCounterStep%5Cn%28RoundRobinRouter%29%22%20-%3E%20%22pauseStepCounterA%22%0A%22categoryCounterStep%5Cn%28RoundRobinRouter%29%22%20-%3E%20%22pauseStepCounterB%22%0A%22categoryCounterStep%5Cn%28RoundRobinRouter%29%22%20-%3E%20%22pauseStepCounterC%22%0A%22pauseStepCounterA%22%20-%3E%20%22errorStepCounterA%22%0A%22errorStepCounterA%22%20-%3E%20%22countStepCounterA%22%0A%22pauseStepCounterB%22%20-%3E%20%22errorStepCounterB%22%0A%22errorStepCounterB%22%20-%3E%20%22countStepCounterB%22%0A%22pauseStepCounterC%22%20-%3E%20%22errorStepCounterC%22%0A%22errorStepCounterC%22%20-%3E%20%22countStepCounterC%22%0A%22noOpAStep%22%20-%3E%20%22categoryCounterStep%5Cn%28RoundRobinRouter%29%22%0A%7D

      stopErrors(plan, ERROR_STEP_CounterB);
      shortPauses(plan, PAUSE_STEP_FILE);
      shortPauses(plan, PAUSE_STEP_CounterC);
      shortPauses(plan, PAUSE_STEP_CounterB);
      longPauses(plan, PAUSE_STEP_CounterA, (int) PAUSE_MILLIS);
      plan.activate();
      Thread.sleep(3 * (PAUSE_MILLIS / 2));
      plan.deactivate();

      // at this point most of the documents routed to CounterA will not have made it and get picked up on restart

      Thread.sleep(PAUSE_MILLIS);

      shortPauses(plan, PAUSE_STEP_CounterA);

      plan.activate();
      Thread.sleep(3 * PAUSE_MILLIS);
      plan.deactivate();

      Thread.sleep(PAUSE_MILLIS);

      DocumentFieldMatchCounter fieldMatchCounter = findFieldMatchCounter(plan, CATEGORY_COUNTER_STEP);
      DocumentFieldMatchCounter fieldMatchCounterA = findFieldMatchCounter(plan, COUNT_STEP_CounterA);
      DocumentFieldMatchCounter fieldMatchCounterB = findFieldMatchCounter(plan, COUNT_STEP_CounterB);
      DocumentFieldMatchCounter fieldMatchCounterC = findFieldMatchCounter(plan, COUNT_STEP_CounterC);

      Map<String, Map<String, DocumentFieldMatchCounter.DocCounted>> scannedDocsByValue = fieldMatchCounter.getScannedDocsByValue();
      int preRoundRobinDocCount =
          countMap(scannedDocsByValue, "comedies") +
              countMap(scannedDocsByValue, "tragedies") +
              countMap(scannedDocsByValue, "other");


      Map<String, Map<String, DocumentFieldMatchCounter.DocCounted>> scannedDocsByValueA = fieldMatchCounterA.getScannedDocsByValue();
      int ACount =
          countMap(scannedDocsByValueA, "comedies") +
              countMap(scannedDocsByValueA, "tragedies") +
              countMap(scannedDocsByValueA, "other");
      Map<String, Map<String, DocumentFieldMatchCounter.DocCounted>> scannedDocsByValueB = fieldMatchCounterB.getScannedDocsByValue();
      int BCount =
          countMap(scannedDocsByValueB, "comedies") +
              countMap(scannedDocsByValueB, "tragedies") +
              countMap(scannedDocsByValueB, "other");
      Map<String, Map<String, DocumentFieldMatchCounter.DocCounted>> scannedDocsByValueC = fieldMatchCounterC.getScannedDocsByValue();
      int CCount =
          countMap(scannedDocsByValueC, "comedies") +
              countMap(scannedDocsByValueC, "tragedies") +
              countMap(scannedDocsByValueC, "other");

      int destinationDocs = ACount + BCount + CCount;
      assertEquals(preRoundRobinDocCount, destinationDocs);

      ResultSet tables = support.getSession().execute("SELECT table_name, keyspace_name from system_schema.tables");
      int totalRows = 0;
      Set<String> counterTables = new HashSet<>();
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
          assertEquals("Wrong hashing count for " + tableName, 44, rowCount); // one entry for each file or DB row
        } else if (tableName.endsWith("_status")) {
          ResultSet example = support.getSession().execute("select outputStepName from " + tableName + " LIMIT 1");
          @SuppressWarnings("DataFlowIssue")
          String stepName = example.one().getString(0);
          // we are interested in the leaf (final) tables in this block
          counterTables.add(tableName);

          totalRows += rowCount;

          System.out.println(stepName + " has " + rowCount);
        } else {
          assertEquals("Found unexpected table" + tableName, "jj_logging.regular", tableName);
        }
      }
      if (counterTables.size() == 8) {

        // We have 4 destinations via 2 routes, each document will reach 2 and be dropped for 2 in the event
        // The processing is round robin among 3 destinations for 44 docs that get duplicated (88 total docs)
        // That means the 3 destinations will get 30, 29 and 29 documents (note that the destination prior to
        // the round robin sees all 88 docs.) In the case that the document is routed to B or C or passes
        // the pause on A during the first run (29+29+10 docs) they will complete and should not be reprocessed
        // This implies that there will be 68 docs (each with half the original 8 dests) that produce the following statuses
        // Docs that succeed on the first try 4P + 2I + 2D  ====> 8 total status events for a total of 544 events


        // In the blocked case
        // Docs that get blocked --> (4P (other half may not be dropped) + 1I + 2D   <<then>>   1P + 1I ) ====> 9 totals status events
        // That leads to a total of 180 events

        int expectedStalledDocs = 20;
        int expected = (88 - expectedStalledDocs) * 8 + expectedStalledDocs * 9;
        if (expected != totalRows) {
          for (String leafTable : counterTables) {
            System.out.println(leafTable);
            showTable(support, leafTable);
          }
        }
        assertEquals(expected, totalRows);
      }
      assertEquals(8, counterTables.size());

    } catch (Throwable e) {
      e.printStackTrace();
      log.error(e);
      dumpTables(support);
      throw e;
    } finally {
      //Thread.sleep(5000000);
      Cassandra.stop();
    }
  }

  private static int countMap(Map<String, Map<String, DocumentFieldMatchCounter.DocCounted>> scannedDocsByValueC, String other) {
    return emptyIfNullMap(scannedDocsByValueC, other).values().stream().mapToInt(dc -> dc.timesSeen.get()).sum();
  }

  private static Map<String, DocumentFieldMatchCounter.DocCounted> emptyIfNullMap(Map<String, Map<String, DocumentFieldMatchCounter.DocCounted>> scannedDocsByValue, String key) {
    Map<String, DocumentFieldMatchCounter.DocCounted> map = scannedDocsByValue.get(key);
    if (map == null) {
      return Collections.emptyMap();
    }
    System.out.println("For " + key + " got " + map.size());
    return map;
  }


  private Plan getPlan() {

    // set up two sources each of which can be paused or have errors before routing
    SimpleFileScanner.Builder fileScannerBuilder = new SimpleFileScanner.Builder();
    StepImpl.Builder filePause = new StepImpl.Builder();
    StepImpl.Builder dbPause = new StepImpl.Builder();
    StepImpl.Builder fileError = new StepImpl.Builder();
    StepImpl.Builder dbError = new StepImpl.Builder();

    // two steps for which all documents flow through
    StepImpl.Builder defaultCategoryOther = new StepImpl.Builder();
    StepImpl.Builder noOpA = new StepImpl.Builder();
    StepImpl.Builder noOpB = new StepImpl.Builder();
    StepImpl.Builder copyIdToCategory = new StepImpl.Builder();
    StepImpl.Builder editCategory = new StepImpl.Builder();
    StepImpl.Builder categoryCounter = new StepImpl.Builder();

    // Three destinations all of which can be paused or errored
    StepImpl.Builder counterAPause = new StepImpl.Builder();
    StepImpl.Builder counterBPause = new StepImpl.Builder();
    StepImpl.Builder counterCPause = new StepImpl.Builder();
    StepImpl.Builder counterAError = new StepImpl.Builder();
    StepImpl.Builder counterBError = new StepImpl.Builder();
    StepImpl.Builder counterCError = new StepImpl.Builder();
    StepImpl.Builder counterACounter = new StepImpl.Builder();
    StepImpl.Builder counterBCounter = new StepImpl.Builder();
    StepImpl.Builder counterCCounter = new StepImpl.Builder();

    // configure our steps

    File tragedies = new File("src/test/resources/test-data");
    fileScannerBuilder.named(FILE_SCANNER)
        .withRoot(tragedies)
        .rememberScannedIds(true)
        .detectChangesViaHashing(true)
        .scanFreqMS(PAUSE_MILLIS / 4);

    dbPause.named(PAUSE_STEP_DB)
        .withProcessor(new PauseEveryFiveTestProcessor.Builder()
            .pausingFor(PAUSE_MILLIS).named("pauseProcDb"));

    filePause.named(PAUSE_STEP_FILE)
        .withProcessor(new PauseEveryFiveTestProcessor.Builder()
            .pausingFor(PAUSE_MILLIS).named("pauseProcFile"));

    counterBPause.named(PAUSE_STEP_CounterB)
        .withProcessor(new PauseEveryFiveTestProcessor.Builder()
            .pausingFor(PAUSE_MILLIS).named("pauseProcCounterB"));

    counterAPause.named(PAUSE_STEP_CounterA)
        .withProcessor(new PauseEveryFiveTestProcessor.Builder()
            .pausingFor(PAUSE_MILLIS).named("pauseProcCounterA"));

    counterCPause.named(PAUSE_STEP_CounterC)
        .withProcessor(new PauseEveryFiveTestProcessor.Builder()
            .pausingFor(PAUSE_MILLIS).named("pauseProcCounter"));

    String[] fileErrorHolder = new String[1];
    String[] dbErrorHolder = new String[1];
    String[] counterBErrorHolder = new String[1];
    String[] counterAErrorHolder = new String[1];
    String[] otherErrorHolder = new String[1];

    fileError.named(ERROR_STEP_FILE).withProcessor(errorProc(fileErrorHolder, "errorProcFile"));
    dbError.named(ERROR_STEP_DB).withProcessor(errorProc(dbErrorHolder, "errorProcDb"));
    counterBError.named(ERROR_STEP_CounterB).withProcessor(errorProc(counterBErrorHolder, "errorProcCounterB"));
    counterAError.named(ERROR_STEP_CounterA).withProcessor(errorProc(counterAErrorHolder, "errorProcCounterA"));
    counterCError.named(ERROR_STEP_CounterC).withProcessor(errorProc(otherErrorHolder, "errorProcCounterC"));

    counterACounter.named(COUNT_STEP_CounterA).withProcessor(new DocumentFieldMatchCounter.Builder()
        .named("categoryCounterProcA")
        .matchesForField("category"));
    counterBCounter.named(COUNT_STEP_CounterB).withProcessor(new DocumentFieldMatchCounter.Builder()
        .named("categoryCounterProcB")
        .matchesForField("category"));
    counterCCounter.named(COUNT_STEP_CounterC).withProcessor(new DocumentFieldMatchCounter.Builder()
        .named("categoryCounterProcC")
        .matchesForField("category"));

    copyIdToCategory.named(COPY_ID_TO_CATEGORY_STEP)
        .withProcessor(new CopyField.Builder()
            .named("copyIdToCategoryProc")
            .from("id").into("category")
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
            .withValue("other"))

        // top of diamond (split) here
        .routingBy(new DuplicateToAll.Builder().named("DuplicateToAll"));

    // branches of the diamond
    noOpA.named(NO_OP_A_STEP).withProcessor(new NoOpProcessor.Builder().named("noOpAProc").turnOffWarning());
    noOpB.named(NO_OP_B_STEP).withProcessor(new NoOpProcessor.Builder().named("noOpBProc").turnOffWarning());

    // will rejoin the streams here.
    categoryCounter.named(CATEGORY_COUNTER_STEP)
        .withProcessor(new DocumentFieldMatchCounter.Builder()
            .named("categoryCounterProc")
            .matchesForField("category")
        )
        .routingBy(new RoundRobinRouter.Builder().named("RoundRobinRouter"));

    PlanImpl.Builder planBuilder = new PlanImpl.Builder();
    planBuilder
        .named("NDRouterTest")

        .addStep(fileScannerBuilder)
        .addStep(filePause, FILE_SCANNER)
        .addStep(fileError, PAUSE_STEP_FILE)

        .addStep(copyIdToCategory, ERROR_STEP_FILE)
        .addStep(editCategory, COPY_ID_TO_CATEGORY_STEP)
        .addStep(defaultCategoryOther, EDIT_CATEGORY)
        .addStep(noOpA, DEFAULT_CATEGORY_TO_OTHER_STEP)
        .addStep(noOpB, DEFAULT_CATEGORY_TO_OTHER_STEP)
        .addStep(categoryCounter, NO_OP_A_STEP, NO_OP_B_STEP)

        // split to three destinations
        .addStep(counterBPause, CATEGORY_COUNTER_STEP)
        .addStep(counterAPause, CATEGORY_COUNTER_STEP)
        .addStep(counterCPause, CATEGORY_COUNTER_STEP)

        .addStep(counterBError, PAUSE_STEP_CounterB)
        .addStep(counterAError, PAUSE_STEP_CounterA)
        .addStep(counterCError, PAUSE_STEP_CounterC)

        .addStep(counterBCounter, ERROR_STEP_CounterB)
        .addStep(counterACounter, ERROR_STEP_CounterA)
        .addStep(counterCCounter, ERROR_STEP_CounterC)

        .withIdField("id");

    return planBuilder.build();
  }

  private static NamedBuilder<? extends DocumentProcessor> errorProc(String[] errorId, String name) {
    return new ErrorFourthTestProcessor.Builder()
        .named(name)
        .withErrorReporter(errorId)
        .erroringFromStart(false);
  }

}
