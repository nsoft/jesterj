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
import org.jesterj.ingest.model.*;
import org.jesterj.ingest.processors.*;
import org.jesterj.ingest.routers.DuplicateToAll;
import org.jesterj.ingest.scanners.SimpleFileScanner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.copyright.easiertest.EasierMocks.*;
import static org.easymock.EasyMock.expect;
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
  @Mock private Document dockMock;
  @Mock private DocStatusChange changeMock;

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

  @Test
  public void testPushToNextIfNotDroppedNoChanges() {
    // if there are no changes, nothing interesting happens, and we just continue
    expect(dockMock.getStatusChange()).andReturn(null);
    expect(dockMock.getId()).andReturn("42").anyTimes();
    step.pushToNextIfOk(dockMock,false);
    replay();
    step.pushToNextIfNotDropped(dockMock);
  }
  @Test
  public void testPushToNextIfNotDroppedBatched() {
    // if there athe status change is not DROPPED
    expect(dockMock.getStatusChange()).andReturn(changeMock);
    expect(dockMock.getId()).andReturn("42").anyTimes();
    expect(changeMock.getStatus()).andReturn(Status.BATCHED);
    step.pushToNextIfOk(dockMock,false);
    replay();
    step.pushToNextIfNotDropped(dockMock);
  }

  @Test
  public void testPushToNextIfNotDroppedDropAll() {
    // This time there is a status change of dropped and since it has no specific steps it means drop the entire document
    expect(dockMock.getStatusChange()).andReturn(changeMock);
    expect(dockMock.getId()).andReturn("42").anyTimes();
    expect(changeMock.getStatus()).andReturn(Status.DROPPED);
    expect(changeMock.getSpecificDestinations()).andReturn(null);
    // key differnce, not pushToNext
    dockMock.reportDocStatus();
    replay();
    step.pushToNextIfNotDropped(dockMock);
  }

  @Test
  public void testPushToNextIfNotDroppedDropAll2() {
    // This time there is a status change of dropped and since it has no specific steps it means drop the entire document
    expect(dockMock.getStatusChange()).andReturn(changeMock);
    expect(dockMock.getId()).andReturn("42").anyTimes();
    expect(changeMock.getStatus()).andReturn(Status.DROPPED);
    expect(changeMock.getSpecificDestinations()).andReturn(Collections.emptyList()); // empty list treated as null
    // key differnce, not pushToNext
    dockMock.reportDocStatus();
    replay();
    step.pushToNextIfNotDropped(dockMock);
  }

  @Test
  public void testPushToNextIfNotDroppedRouterDroppedSome() {
    // This time there is a status change of dropped but it relates to a router having dropped some destinations
    // not all. This is a legal configuration, but maybe not legal at this point. I'm not 100% sure we really ever
    // should see this case, and maybe it's should error instead?
    expect(dockMock.getStatusChange()).andReturn(changeMock);
    expect(dockMock.getId()).andReturn("42").anyTimes();
    expect(changeMock.getStatus()).andReturn(Status.DROPPED);
    expect(changeMock.getSpecificDestinations()).andReturn(List.of("foo"));
    expect(dockMock.getIncompleteOutputDestinations()).andReturn(new String[]{"foo","bar"});

    // key difference, pushToNext not report status
    step.pushToNextIfOk(dockMock,false);

    replay();
    step.pushToNextIfNotDropped(dockMock);
  }

  @Test
  public void testSideEffectsLastStep() {
    replay();
    try {
      testStep = new StepImpl.Builder().withProcessor(new SendToSolrCloudProcessor.Builder()
          .named("foo")
          .withZookeeper("localhost:9983")).build();
      Set<Step> possibleSideEffects = testStep.getDownstreamOutputSteps();
      assertEquals(1, possibleSideEffects.size());
    } finally {
      testStep.deactivate();
    }
  }

  @Test
  public void testShakespearePlan() {
    replay();
    Plan plan = getPlan();
    testStep = plan.findStep(SHAKESPEARE);
    Set<Step> possibleSideEffects = testStep.getDownstreamOutputSteps();
    assertEquals(1, possibleSideEffects.size());
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
