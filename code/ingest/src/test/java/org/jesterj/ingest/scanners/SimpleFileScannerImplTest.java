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

import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.model.impl.PlanImpl;
import org.jesterj.ingest.model.impl.ScannerImpl;
import org.jesterj.ingest.model.impl.StepImpl;
import org.jesterj.ingest.persistence.Cassandra;
import org.jesterj.ingest.processors.DocumentCounter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.time.Instant;

import static org.junit.Assert.assertEquals;

public class SimpleFileScannerImplTest extends ScannerImplTest {

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
  public void testFilter() throws InterruptedException {
    File tempDir = getUniqueTempDir();
    Cassandra.start(tempDir, "127.0.0.1");

    PlanImpl.Builder planBuilder = new PlanImpl.Builder();
    SimpleFileScanner.Builder scannerBuilder = new SimpleFileScanner.Builder();
    scannerBuilder.scanFreqMS(1000);
    StepImpl.Builder testStepBuilder = new StepImpl.Builder();

    File tragedies = new File("src/test/resources/test-data");
    scannerBuilder.named(SHAKESPEARE)
        .withRoot(tragedies)
        .acceptOnly(pathname -> !pathname.getName().contains("README"))
        .scanFreqMS(1000)
    ;

    testStepBuilder.named("counterStep")
        .batchSize(10)
        .withProcessor(new DocumentCounter.Builder().named("counterProc"));

    planBuilder
        .named("testScan")
        .addStep(scannerBuilder)
        .addStep(testStepBuilder, SHAKESPEARE)
        .withIdField("id");
    Plan plan = planBuilder.build();

    try {
      plan.activate();

      Thread.sleep(2000);

      assertEquals(43, sizeForCounter(plan, "counterStep"));

      clearCounter(plan, "counterStep");

      Thread.sleep(2000);
      assertEquals(43, sizeForCounter(plan, "counterStep"));
    } finally {
      plan.deactivate();
    }

  }
  @Test
  public void testLineByLine() throws InterruptedException {
    File tempDir = getUniqueTempDir();
    Cassandra.start(tempDir, "127.0.0.1");

    PlanImpl.Builder planBuilder = new PlanImpl.Builder();
    SimpleFileScanner.Builder scannerBuilder = new SimpleFileScanner.Builder();
    scannerBuilder.scanFreqMS(1000);
    StepImpl.Builder testStepBuilder = new StepImpl.Builder();

    File tragedies = new File("src/test/resources/test-data");
    scannerBuilder.named(SHAKESPEARE)
        .withRoot(tragedies)
        .acceptOnly(pathname -> !pathname.getName().contains("README"))
        .docPerLineIfMatches(pathname -> pathname.getName().equals("glossary"))
        .scanFreqMS(1000)
    ;

    testStepBuilder.named("counterStep")
        .batchSize(10)
        .withProcessor(new DocumentCounter.Builder().named("counterProc"));

    planBuilder
        .named("testScan")
        .addStep(scannerBuilder)
        .addStep(testStepBuilder, SHAKESPEARE)
        .withIdField("id");
    Plan plan = planBuilder.build();

    try {
      Instant start = Instant.now();
      plan.activate();

      Thread.sleep(10000);
      Instant end = Instant.now();

      // glossary has 2428 lines, but glossary will no longer be a document as a whole so - 1
      assertEquals("Wrong count (processing elapsed time = " + (end.toEpochMilli() - start.toEpochMilli()),
          43 + 2428 - 1, sizeForCounter(plan, "counterStep"));

    } finally {
      plan.deactivate();
    }

  }

  @Test
  public void testScan() throws InterruptedException {
    File tempDir = getUniqueTempDir();
    Cassandra.start(tempDir, "127.0.0.1");

    PlanImpl.Builder planBuilder = new PlanImpl.Builder();
    SimpleFileScanner.Builder scannerBuilder = new SimpleFileScanner.Builder();
    scannerBuilder.scanFreqMS(1000);
    StepImpl.Builder testStepBuilder = new StepImpl.Builder();

    File tragedies = new File("src/test/resources/test-data");
    scannerBuilder.named("test_scanner").withRoot(tragedies).named(SHAKESPEARE).scanFreqMS(1000);

    testStepBuilder.named("counterStep")
        .batchSize(10)
        .withProcessor(new DocumentCounter.Builder().named("counterProc")
        );

    planBuilder
        .named("testScan")
        .addStep(scannerBuilder)
        .addStep(testStepBuilder, SHAKESPEARE)
        .withIdField("id");
    Plan plan = planBuilder.build();

    try {
      plan.activate();

      Thread.sleep(2000);

      assertEquals(44, sizeForCounter(plan, "counterStep"));

      clearCounter(plan, "counterStep");

      Thread.sleep(2000);
      assertEquals(44, sizeForCounter(plan, "counterStep"));
    } finally {
      plan.deactivate();
    }
  }

  @SuppressWarnings("SameParameterValue")
  private void clearCounter(Plan plan, String counterStep) {
    DocumentCounter counter = findCounter(plan, counterStep);
    counter.getScannedDocs().clear();
  }


}
