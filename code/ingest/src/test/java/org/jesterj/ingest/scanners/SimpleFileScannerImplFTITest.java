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
import static org.junit.Assert.assertTrue;


public class SimpleFileScannerImplFTITest {

  private static final String SHAKESPEAR = "Shakespear_scanner";
  private static final Logger log = LogManager.getLogger();

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
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
    PauseEveryFiveTestProcessor.Builder pause30Every5 = new PauseEveryFiveTestProcessor.Builder().named("pause5");
    NamedBuilder<? extends DocumentProcessor> error4thof5 =
        new ErrorFourthTestProcessor.Builder().named("error4").withErrorReporter(errorId);

    Plan plan1 = getPlan( pause30Every5,scannedDocRecorder);
    Plan plan2 = getPlan(pause30Every5,error4thof5, scannedDocRecorder);
    Plan planFinish = getPlan(scannedDocRecorder);

    try {
      plan1.activate();
      Thread.sleep(5000);
      assertEquals(5, scannedDocs.size());
      plan1.deactivate();

      Thread.sleep(5000);

      plan1.activate();
      Thread.sleep(5000);
      assertEquals(10, scannedDocs.size()); // test that 5 NEW docs were scanned
      plan1.deactivate();

      Thread.sleep(5000);

      plan2.activate();
      Thread.sleep(5000);
      assertEquals(14, scannedDocs.size()); // test that 4 NEW docs were seen (a 5th will have errored but not been counted)
      plan2.deactivate();

      Thread.sleep(5000);

      plan1.activate();
      Thread.sleep(5000);
      assertEquals(19, scannedDocs.size()); // test that 5 NEW docs were scanned
      plan1.deactivate();
      assertTrue(scannedDocs.containsKey(errorId[0])); // AND the error doc was one of them

      Thread.sleep(5000);

      planFinish.activate();
      Thread.sleep(5000);
      planFinish.deactivate();
      assertEquals(44, scannedDocs.size());
      scannedDocs.clear();
      // the documents will have been scanned, but since they are unchanged
      // they do not get sent down the pipeline, and so the counter
      // step won't see them

      Thread.sleep(5000);

      planFinish.activate();
      Thread.sleep(5000);
      planFinish.deactivate();
      Thread.sleep(3500);
      assertEquals(0, scannedDocs.size());
    } finally {
      Cassandra.stop();
    }
  }

  @NotNull
  private NamedBuilder<DocumentProcessor> getScannedDocRecorder(HashMap<String, Document> scannedDocs) {
    return new NamedBuilder<>() {

      @Override
      public NamedBuilder<DocumentProcessor> named(String name) {
        return null;
      }

      @Override
      public DocumentProcessor build() {
        return new DocumentProcessor() {
          @Override
          public String getName() {
            return null;
          }

          @Override
          public Document[] processDocument(Document document) {
            scannedDocs.put(document.getId(), document);
            log.info("Recording {}", document.getId());
            return new Document[] {document};
          }
        };
      }
    };
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
        .scanFreqMS(3000);

    planBuilder
        .named("testScan")
        .addStep(scannerBuilder)
        .withIdField("id");
    String prior = SHAKESPEAR;
    int count=0;
    for (NamedBuilder<? extends DocumentProcessor> processor : processors) {
      StepImpl.Builder testStepBuilder = new StepImpl.Builder();
      testStepBuilder.named("test" + count++)
          .withProcessor(
              processor
          );

      planBuilder.addStep(testStepBuilder, prior);
      prior = testStepBuilder.getStepName();
    }

    return planBuilder.build();
  }
}
