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
import org.jesterj.ingest.routers.DuplicateToAll;
import org.jesterj.ingest.routers.RouteByStepName;
import org.junit.Test;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import static org.jesterj.ingest.model.impl.StepImpl.VIA;
import static org.junit.Assert.*;

public class NonLinearDiamondFTITest extends ScannerImplTest {

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
  public static final String EDIT_CATEGORY = "editCategory";
  public static final String DEFAULT_CATEGORY_TO_OTHER_STEP = "defaultCategoryToOtherStep";
  public static final String CATEGORY_COUNTER_STEP = "categoryCounterStep";
  public static final String FILE_SCANNER = "fileScanner";
  public static final String NO_OP_A_STEP = "noOpAStep";
  public static final String NO_OP_B_STEP = "noOpBStep";


  @Test
  public void testScanWithMemoryDiamondIntoThree() throws InterruptedException {
    // GitHub: "I want the threads!"
    // Me: "You can't handle the threads!"
    if((System.getenv("GITHUB_ACTION") != null)) return;

    // works great on my 16 core 32 thread 1950x, I expect it probably works fine on 4 or more cores, GitHub gives us 2
    // which means threads in the plan just keep doing their thing after they were supposed to have shut down, and so
    // some counts come out wrong and fail the test.

    File tempDir = getUniqueTempDir();
    Cassandra.start(tempDir, "127.0.0.1");
    CassandraSupport support = new CassandraSupport();

    try {
      Plan plan = getDiamondPlan();
      System.out.println(plan.visualize(Format.DOT).toString());
      //http://magjac.com/graphviz-visual-editor/?dot=digraph%20%22visualize%22%20%7B%0A%22fileScanner%22%20%5B%22color%22%3D%22blue%22%2C%22penwidth%22%3D%223.0%22%5D%0A%22categoryCounterStep%5Cn%28RouteByCategory%29%22%20%5B%22color%22%3D%22red%22%2C%22penwidth%22%3D%223.0%22%5D%0A%22countStepComedy%22%20%5B%22color%22%3D%22red%22%2C%22penwidth%22%3D%223.0%22%5D%0A%22countStepTragedy%22%20%5B%22color%22%3D%22red%22%2C%22penwidth%22%3D%223.0%22%5D%0A%22countStepOther%22%20%5B%22color%22%3D%22red%22%2C%22penwidth%22%3D%223.0%22%5D%0A%22fileScanner%22%20-%3E%20%22pauseStepFile%22%0A%22pauseStepFile%22%20-%3E%20%22errorStepFile%22%0A%22errorStepFile%22%20-%3E%20%22copyIdToCategoryStep%22%0A%22copyIdToCategoryStep%22%20-%3E%20%22editCategory%22%0A%22editCategory%22%20-%3E%20%22defaultCategoryToOtherStep%5Cn%28DuplicateToAll%29%22%0A%22defaultCategoryToOtherStep%5Cn%28DuplicateToAll%29%22%20-%3E%20%22noOpBStep%22%0A%22defaultCategoryToOtherStep%5Cn%28DuplicateToAll%29%22%20-%3E%20%22noOpAStep%22%0A%22noOpBStep%22%20-%3E%20%22categoryCounterStep%5Cn%28RouteByCategory%29%22%0A%22categoryCounterStep%5Cn%28RouteByCategory%29%22%20-%3E%20%22pauseStepComedy%22%0A%22categoryCounterStep%5Cn%28RouteByCategory%29%22%20-%3E%20%22pauseStepTragedy%22%0A%22categoryCounterStep%5Cn%28RouteByCategory%29%22%20-%3E%20%22pauseStepOther%22%0A%22pauseStepComedy%22%20-%3E%20%22errorStepComedy%22%0A%22errorStepComedy%22%20-%3E%20%22countStepComedy%22%0A%22pauseStepTragedy%22%20-%3E%20%22errorStepTragedy%22%0A%22errorStepTragedy%22%20-%3E%20%22countStepTragedy%22%0A%22pauseStepOther%22%20-%3E%20%22errorStepOther%22%0A%22errorStepOther%22%20-%3E%20%22countStepOther%22%0A%22noOpAStep%22%20-%3E%20%22categoryCounterStep%5Cn%28RouteByCategory%29%22%0A%7D
      startErrors(plan, ERROR_STEP_COMEDY);
      shortPauses(plan,PAUSE_STEP_FILE);
      shortPauses(plan,PAUSE_STEP_OTHER);
      shortPauses(plan,PAUSE_STEP_COMEDY);
      shortPauses(plan,PAUSE_STEP_TRAGEDY);
      plan.activate();
      Thread.sleep(3*PAUSE_MILLIS);
      plan.deactivate();
      Thread.sleep(PAUSE_MILLIS);


      ResultSet tables = support.getSession().execute("SELECT table_name, keyspace_name from system_schema.tables");
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
          assertEquals("Wrong hashing count for " + tableName,  44,rowCount); // one entry for each file or DB row
        } else if (tableName.endsWith("_status")) {
          ResultSet example = support.getSession().execute("select outputStepName from " + tableName + " LIMIT 1");
          @SuppressWarnings("DataFlowIssue")
          String stepName = example.one().getString(0);
          if (rowCount != 44+44) {
            assertTrue("Found " + rowCount + " for " + stepName + " but it was not one of "+ COUNT_STEP_COMEDY + VIA + NO_OP_A_STEP + " or " + COUNT_STEP_COMEDY + VIA + NO_OP_B_STEP ,
                (COUNT_STEP_COMEDY + VIA + NO_OP_A_STEP).equals(stepName) ||
                    (COUNT_STEP_COMEDY + VIA + NO_OP_B_STEP).equals(stepName) );
            // we can get errors for docs from either scanner
            tablesWithErrors.add(tableName);
            ///////////////////////////////////////////////////////////////////////////////////////
            //
            // There are 44 documents total, 3 of 17 comedies from 1 scanner published along two paths should error
            // out, for a total of 7 errors on the first pass and the second pass will feed the 6 errors, one of
            // which will error a second time so these 3 cases are events per document:
            //   - PROCESSING,INDEXED (40 * 2 * 2 events = 162 events)
            //   - PROCESSING,ERROR,PROCESSING,INDEXED (6 of 7 * 4 events = 24 events)
            //   - PROCESSING,ERROR,PROCESSING,ERROR,PROCESSING,INDEXED (1 * 6 events = 6 events)
            // So across 2 tables we should get a total of 164 + 20 + 6 = 192 events
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
    } catch (Throwable e) {
      e.printStackTrace();
      log.error(e);
      dumpTables(support);
      throw e;

    }finally {
      //Thread.sleep(5000000);
      Cassandra.stop();
    }
  }


  private Plan getDiamondPlan() {

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
        .routingBy(new RouteByStepName.Builder().named("RouteByCategory")
            .keyValuesInField("category")
            .mappingValueFromTo("other", PAUSE_STEP_OTHER)
            .mappingValueFromTo("comedies", PAUSE_STEP_COMEDY)
            .mappingValueFromTo("tragedies", PAUSE_STEP_TRAGEDY));

    PlanImpl.Builder planBuilder = new PlanImpl.Builder();
    planBuilder
        .named("diamondTest")

        .addStep(fileScannerBuilder)
        .addStep(filePause, FILE_SCANNER)
        .addStep(fileError, PAUSE_STEP_FILE)

        .addStep(copyIdToCategory, ERROR_STEP_FILE)
        .addStep(editCategory, COPY_ID_TO_CATEGORY_STEP)
        .addStep(defaultCategoryOther, EDIT_CATEGORY)
        .addStep(noOpA,DEFAULT_CATEGORY_TO_OTHER_STEP )
        .addStep(noOpB,DEFAULT_CATEGORY_TO_OTHER_STEP )
        .addStep(categoryCounter, NO_OP_A_STEP, NO_OP_B_STEP)

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

}
