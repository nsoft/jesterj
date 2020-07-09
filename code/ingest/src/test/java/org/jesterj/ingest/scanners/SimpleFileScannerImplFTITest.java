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
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.model.impl.NamedBuilder;
import org.jesterj.ingest.model.impl.PlanImpl;
import org.jesterj.ingest.model.impl.StepImpl;
import org.jesterj.ingest.persistence.Cassandra;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

@Ignore
public class SimpleFileScannerImplFTITest {

  private static final String SHAKESPEAR = "Shakespear_scanner";


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
    Cassandra.start(Files.createTempDir(), "127.0.0.1");

    PlanImpl.Builder planBuilder = new PlanImpl.Builder();
    SimpleFileScanner.Builder scannerBuilder = new SimpleFileScanner.Builder();
    StepImpl.Builder testStepBuilder = new StepImpl.Builder();

    File tragedies = new File("src/test/resources/test-data");
    scannerBuilder.named("test_scanner")
        .withRoot(tragedies)
        .named(SHAKESPEAR)
        .rememberScannedIds(true)
        .scanFreqMS(3000);

    HashMap<String, Document> scannedDocs = new HashMap<>();

    testStepBuilder.named("test")
        .batchSize(10)
        .withProcessor(
            new NamedBuilder<>() {
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
                    return new Document[]{document};
                  }
                };
              }
            }
        );

    planBuilder
        .named("testScan")
        .addStep(scannerBuilder)
        .addStep(testStepBuilder, SHAKESPEAR)
        .withIdField("id");
    Plan plan = planBuilder.build();

    try {
      plan.activate();

      Thread.sleep(5000);
      assertEquals(44, scannedDocs.size());

      scannedDocs.clear();

      // the documents will have been scanned, but since they are unchanged
      // they do not get sent down the pipeline, and so the counter
      // step won't see them
      Thread.sleep(3500);
      assertEquals(0, scannedDocs.size());
    } finally {
      plan.deactivate();
      Cassandra.stop();
    }
  }
}
