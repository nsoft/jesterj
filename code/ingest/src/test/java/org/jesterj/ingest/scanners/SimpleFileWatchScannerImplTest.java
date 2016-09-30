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
/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 3/17/16
 */

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

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class SimpleFileWatchScannerImplTest {

  private static final String SHAKESPEAR = "Shakespear scanner";


  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
  }

  @Test
  public void testBuild() {
    SimpleFileWatchScanner.Builder builder = new SimpleFileWatchScanner.Builder();
    ScannerImpl build = builder.build();
    assertEquals(SimpleFileWatchScanner.class, build.getClass());
  }

  @Test
  public void testScan() throws InterruptedException {
    PlanImpl.Builder planBuilder = new PlanImpl.Builder();
    SimpleFileWatchScanner.Builder scannerBuilder = new SimpleFileWatchScanner.Builder();
    StepImpl.Builder testStepBuilder = new StepImpl.Builder();

    File tragedies = new File("src/test/resources/test-data/tragedies");
    scannerBuilder.withRoot(tragedies).named(SHAKESPEAR).scanFreqMS(100);

    HashMap<String, Document> scannedDocs = new HashMap<>();

    testStepBuilder.named("test")
        .batchSize(10)
        .withProcessor(
        new NamedBuilder<DocumentProcessor>() {
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

    plan.activate();

    Thread.sleep(5500);
    assertEquals(10, scannedDocs.size());

    scannedDocs.clear();
    File hamlet = new File(tragedies, "hamlet");
    assertTrue(hamlet.setLastModified(System.currentTimeMillis()));

    Thread.sleep(2000);
    assertEquals(1, scannedDocs.size());

  }
}
