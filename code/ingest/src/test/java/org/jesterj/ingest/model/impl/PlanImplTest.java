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

package org.jesterj.ingest.model.impl;
/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 3/18/16
 */

import org.apache.logging.log4j.Level;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.model.Scanner;
import org.jesterj.ingest.model.Step;
import org.jesterj.ingest.processors.LogAndDrop;
import org.jesterj.ingest.scanners.SimpleFileWatchScanner;
import org.junit.Test;

import java.io.File;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.TestCase.assertEquals;

public class PlanImplTest {

  private static final String LOG_AND_DROP = "log and drop";
  private static final String SCAN_FOO_BAR = "scan foo/bar";

  @Test
  public void testSimple2Step() {
    PlanImpl.Builder planBuilder = new PlanImpl.Builder();
    SimpleFileWatchScanner.Builder scannerBuilder = new SimpleFileWatchScanner.Builder();
    StepImpl.Builder dropStepBuilder = new StepImpl.Builder();

    scannerBuilder.withRoot(new File("/Users/gus/foo/bar")).stepName(SCAN_FOO_BAR).batchSize(10);

    dropStepBuilder.stepName(LOG_AND_DROP).batchSize(10).withProcessor(
        new LogAndDrop.Builder().withLogLevel(Level.ERROR).build()
    );

    planBuilder
        .addStep(null, scannerBuilder)
        .addStep(new String[]{SCAN_FOO_BAR}, dropStepBuilder)
        .withIdField("id");
    Plan plan = planBuilder.build();

    Step scanStep = plan.findStep(SCAN_FOO_BAR);
    Step dropStep = plan.findStep(LOG_AND_DROP);
    assertNotNull(scanStep);
    assertNotNull(dropStep);
    assertEquals(SCAN_FOO_BAR, scanStep.getStepName());
    assertEquals(LOG_AND_DROP, dropStep.getStepName());
    Step foo = scanStep.getNext(new DocumentImpl(null, "foo", plan, Document.Operation.NEW, (Scanner) scanStep));
    assertEquals(LOG_AND_DROP, foo.getStepName());

  }

}
