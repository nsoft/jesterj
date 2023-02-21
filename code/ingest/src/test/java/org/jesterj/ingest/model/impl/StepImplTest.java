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
 * Date: 3/17/16
 */

import com.copyright.easiertest.Mock;
import com.copyright.easiertest.ObjectUnderTest;
import org.jesterj.ingest.model.ConfiguredBuildable;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.model.Step;
import org.jesterj.ingest.processors.CopyField;
import org.jesterj.ingest.processors.LogAndDrop;
import org.jesterj.ingest.processors.SendToSolrCloudProcessor;
import org.jesterj.ingest.processors.SimpleDateTimeReformatter;
import org.jesterj.ingest.processors.TikaProcessor;
import org.jesterj.ingest.routers.DuplicateToAll;
import org.jesterj.ingest.scanners.SimpleFileScanner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static com.copyright.easiertest.EasierMocks.*;
import static org.junit.Assert.*;

public class StepImplTest {
  private static final String ACCESSED = "format_accessed_date";
  private static final String CREATED = "format_created_date";
  private static final String MODIFIED = "format_modified_date";
  private static final String SIZE_TO_INT = "size_to_int_step";
  private static final String TIKA = "tika_step";
  private static final String SHAKESPEARE = "Shakespeare_scanner";


  @SuppressWarnings("unused")
  @ObjectUnderTest StepImpl step;

  private Step testStep;
  @Mock private ConfiguredBuildable<? extends DocumentProcessor> mockProcessorBuilder;
  @Mock private DocumentProcessor mockProcessor;

  public StepImplTest() {
    prepareMocks(this);
  }

  @Before
  public void setUp() {
    reset();
  }

  @After
  public void tearDown() {
    verify();
  }

  @Test
  public void testBuildAStep() {
    replay();
    StepImpl.Builder builder = new StepImpl.Builder();
    builder.batchSize(5);
    builder.build();
  }

  /**
   * Test the oddball case where we have a final step with no side effects
   * (i.e. a waste of time!) Bad design, but should not throw an error since
   * it could be a custom step that's actually  got side effects but perhaps is
   * idempotent and doesn't need to be tracked anyway, or is optional or
   * best effort
   */
  @Test
  public void testSideEffectsNoneLastStep() {
    replay();
    try {
      testStep = new StepImpl.Builder().withProcessor(new LogAndDrop.Builder().named("foo")).build();
      Step[] possibleSideEffects = testStep.geOutputSteps();
      assertEquals(0, possibleSideEffects.length);
    } finally {
      testStep.deactivate();
    }
  }

  @Test
  public void testSideEffectsLastStep() {
    replay();
    try {
      testStep = new StepImpl.Builder().withProcessor(new SendToSolrCloudProcessor.Builder()
          .named("foo")
          .withZookeeper("localhost:9983")).build();
      Step[] possibleSideEffects = testStep.geOutputSteps();
      assertEquals(1, possibleSideEffects.length);
    } finally {
      testStep.deactivate();
    }
  }

  @Test
  public void testShakespearePlan() {
    replay();
    Plan plan = getPlan();
    testStep = plan.findStep(SHAKESPEARE);
    Step[] possibleSideEffects = testStep.geOutputSteps();
    assertEquals(1, possibleSideEffects.length);
  }

  private Plan getPlan() {
    PlanImpl.Builder planBuilder = new PlanImpl.Builder();
    SimpleFileScanner.Builder scanner = new SimpleFileScanner.Builder();
    StepImpl.Builder formatCreated = new StepImpl.Builder();
    StepImpl.Builder formatModified = new StepImpl.Builder();
    StepImpl.Builder formatAccessed = new StepImpl.Builder();
    StepImpl.Builder renameFileszieToInteger = new StepImpl.Builder();
    StepImpl.Builder tikaBuilder = new StepImpl.Builder();
    StepImpl.Builder sendToSolrBuilder = new StepImpl.Builder();

    File testDocs = new File("data");

    scanner
        .named(SHAKESPEARE)
        .withRoot(testDocs)
        .scanFreqMS(100);
    formatCreated
        .named(CREATED)
        .withProcessor(
            new SimpleDateTimeReformatter.Builder()
                .named("format_created")
                .from("created")
                .into("created_dt")
        );
    formatModified
        .named(MODIFIED)
        .withProcessor(
            new SimpleDateTimeReformatter.Builder()
                .named("format_modified")
                .from("modified")
                .into("modified_dt")
        );
    formatAccessed
        .named(ACCESSED)
        .withProcessor(
            new SimpleDateTimeReformatter.Builder()
                .named("format_accessed")
                .from("accessed")
                .into("accessed_dt")
        );

    renameFileszieToInteger
        .named(SIZE_TO_INT)
        .withProcessor(
            new CopyField.Builder()
                .named("copy_size_to_int")
                .from("file_size")
                .into("file_size_i")
                .retainingOriginal(false)
        );
    tikaBuilder
        .named(TIKA)
        .routingBy(new DuplicateToAll.Builder()
            .named("duplicator"))
        .withProcessor(new TikaProcessor.Builder()
            .named("tika")
        );
    sendToSolrBuilder
        .named("solr_sender")
        .withProcessor(
            new SendToSolrCloudProcessor.Builder()
                .withZookeeper("localhost:9983")
                .usingCollection("jjtest")
                .placingTextContentIn("_text_")
                .withDocFieldsIn(".fields")
        );
    planBuilder
        .named("myPlan")
        .withIdField("id")
        .addStep(scanner)
        .addStep(formatCreated, SHAKESPEARE)
        .addStep(formatModified, CREATED)
        .addStep(formatAccessed, MODIFIED)
        .addStep(renameFileszieToInteger, ACCESSED)
        .addStep(tikaBuilder, SIZE_TO_INT);
    planBuilder.addStep(sendToSolrBuilder, TIKA);
//    planBuilder.addStep(sendToElasticBuilder, TIKA);
    return planBuilder.build();

  }

}
