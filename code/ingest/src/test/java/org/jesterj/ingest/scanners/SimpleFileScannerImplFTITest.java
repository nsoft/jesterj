/*
 * Copyright 2016 Needham Software LLC
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

package org.jesterj.ingest.scanners;

import com.google.common.io.Files;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.model.impl.NamedBuilder;
import org.jesterj.ingest.model.impl.PlanImpl;
import org.jesterj.ingest.model.impl.StepImpl;
import org.jesterj.ingest.persistence.Cassandra;
import org.jesterj.ingest.processors.ErrorFourthTestProcessor;
import org.jesterj.ingest.processors.PauseEveryFiveTestProcessor;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


//@Ignore
public class SimpleFileScannerImplFTITest extends ScannerImplTest {

  private static final String SHAKESPEAR = "Shakespear_scanner";
  private static final Logger log = LogManager.getLogger();
  public static final int PAUSE_MILLIS = 32000;

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
    System.out.println("Tearing down-xxxxxx");
  }

  // this has been segregated to it's own test because something about starting cassandra after
  // using logging without it hoses the event contexts in logging. For now, same process stop/start
  // and use without cassandra configured is not a valid use case, so punt...
  @Test
//  @Ignore(value = "seems to experience cross talk with and interfere with other tests, passes solo locally, " +
//      "expect cassandra is not playing nice here")
  public void testScanWithMemory() throws InterruptedException {
    //noinspection UnstableApiUsage
    File tempDir = Files.createTempDir();
    HashMap<String, Document> scannedDocs = new HashMap<>();
    Cassandra.start(tempDir, "127.0.0.1");

    String[] errorId = new String[1];

    NamedBuilder<? extends DocumentProcessor> scannedDocRecorder = getScannedDocRecorder(scannedDocs);
    PauseEveryFiveTestProcessor.Builder pauseEvery5 =
        new PauseEveryFiveTestProcessor.Builder()
            .pausingFor(PAUSE_MILLIS);
    PauseEveryFiveTestProcessor.Builder pauseEvery5_2 =
        new PauseEveryFiveTestProcessor.Builder()
            .pausingFor(PAUSE_MILLIS);
    NamedBuilder<? extends DocumentProcessor> error4thof5 =
        new ErrorFourthTestProcessor.Builder().named("error4").withErrorReporter(errorId);

    pauseEvery5.named("pause_plan1");
    Plan plan1 = getPlan( pauseEvery5,getScannedDocRecorder(scannedDocs));
    pauseEvery5.named("pause_plan2");
    Plan plan2 = getPlan(pauseEvery5_2,error4thof5, getScannedDocRecorder(scannedDocs));
    Plan planFinish = getPlan(scannedDocRecorder);

    try {
      plan1.activate();
      // now scanner should find all docs, attempt to index them, all marked
      // as processing...
      Thread.sleep(3*PAUSE_MILLIS/4);
      // the pause every 5 should have let 5 through and then paused for 30 sec
      assertEquals(5, scannedDocs.size());
      plan1.deactivate();

      // plan has been deactivated, leaving 5 as indexed and the rest as processing

      Thread.sleep(3*PAUSE_MILLIS/4);
      assertEquals(5, scannedDocs.size());

      plan1.activate();
      // plan should first queue all processing docs (from prior scan) and then proceed with new
      // scan, but that scan should never start because only the first 5 docs queued up will be
      // processed before pausing another 30 seconds. Since the map is keyed by ID an increase in
      // the size of the map shows that the previous documents were not processed.
      Thread.sleep(3*PAUSE_MILLIS/4);
      assertEquals(10, scannedDocs.size()); // test that 5 NEW docs were scanned
      plan1.deactivate();

      Thread.sleep(3*PAUSE_MILLIS/4);
      assertEquals(10, scannedDocs.size()); // test plan really deactivated

      plan2.activate();
      Thread.sleep(3*PAUSE_MILLIS/4);
      assertEquals(14, scannedDocs.size()); // test that 4 NEW docs were seen (a 5th will have errored but not been counted)
      plan2.deactivate();

      String eid = errorId[0];
      assertNotNull(eid);

      Thread.sleep(3*PAUSE_MILLIS/4);
      assertEquals(14, scannedDocs.size()); // test plan really deactivated

      plan1.activate();
      Thread.sleep(3*PAUSE_MILLIS/4);
      assertEquals(19, scannedDocs.size()); // test that 5 NEW docs were scanned
      plan1.deactivate();
      assertTrue(scannedDocs.containsKey(eid)); // AND the error doc was one of them

      Thread.sleep(3*PAUSE_MILLIS/4);

      planFinish.activate();
      Thread.sleep(3*PAUSE_MILLIS/4);
      planFinish.deactivate();
      assertEquals(44, scannedDocs.size());
      scannedDocs.clear();
      // the documents will have been scanned, but since they are unchanged
      // they do not get sent down the pipeline, and so the counter
      // step won't see them

      Thread.sleep(3*PAUSE_MILLIS/4);

      planFinish.activate();
      Thread.sleep(3*PAUSE_MILLIS/4);
      planFinish.deactivate();
      Thread.sleep(3*PAUSE_MILLIS/4);
      assertEquals(0, scannedDocs.size());
    } finally {
      Cassandra.stop();
    }
  }

  @SafeVarargs
  private Plan getPlan(NamedBuilder<? extends DocumentProcessor>... processors) {
    PlanImpl.Builder planBuilder = new PlanImpl.Builder();
    SimpleFileScanner.Builder scannerBuilder = new SimpleFileScanner.Builder();

    File tragedies = new File("src/test/resources/test-data");
    scannerBuilder.named("test_scanner")
        .withRoot(tragedies)
        .named(SHAKESPEAR)
        .rememberScannedIds(true)
        .scanFreqMS(PAUSE_MILLIS/4);

    planBuilder
        .named("testScan")
        .addStep(scannerBuilder)
        .withIdField("id");
    String prior = SHAKESPEAR;
    int count=0;
    for (NamedBuilder<? extends DocumentProcessor> processor : processors) {
      StepImpl.Builder testStepBuilder = new StepImpl.Builder();
      testStepBuilder.named("test" + count++)
          .withShutdownWait(50)
          .withProcessor(
              processor
          );

      planBuilder.addStep(testStepBuilder, prior);
      prior = testStepBuilder.getStepName();
    }

    return planBuilder.build();
  }
}
