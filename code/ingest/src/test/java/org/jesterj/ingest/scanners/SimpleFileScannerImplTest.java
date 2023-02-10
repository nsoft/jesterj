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

import org.jesterj.ingest.Main;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.model.impl.NamedBuilder;
import org.jesterj.ingest.model.impl.PlanImpl;
import org.jesterj.ingest.model.impl.ScannerImpl;
import org.jesterj.ingest.model.impl.StepImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class SimpleFileScannerImplTest {

  private static final String SHAKESPEARE = "Shakespeare_scanner";


  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
  }

  @Test
  public void testBuild() {
    SimpleFileScanner.Builder builder = new SimpleFileScanner.Builder();
    ScannerImpl build = builder.build();
    assertEquals(SimpleFileScanner.class, build.getClass());
  }

  @Test
  public void testScan() throws InterruptedException {
    PlanImpl.Builder planBuilder = new PlanImpl.Builder();
    SimpleFileScanner.Builder scannerBuilder = new SimpleFileScanner.Builder();
    scannerBuilder.scanFreqMS(1000);
    StepImpl.Builder testStepBuilder = new StepImpl.Builder();

    File tragedies = new File("src/test/resources/test-data");
    scannerBuilder.named("test_scanner").withRoot(tragedies).named(SHAKESPEARE).scanFreqMS(1000);

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
        .addStep(testStepBuilder, SHAKESPEARE)
        .withIdField("id");
    Plan plan = planBuilder.build();

    try {
      Main.registerPlan(plan); // hack ugly fix
      plan.activate();

      Thread.sleep(2000);
      assertEquals(44, scannedDocs.size());

      scannedDocs.clear();

      Thread.sleep(2000);
      assertEquals(44, scannedDocs.size());
    } finally {
      plan.deactivate();
      Main.deregisterPlan(plan);
    }
  }


}
