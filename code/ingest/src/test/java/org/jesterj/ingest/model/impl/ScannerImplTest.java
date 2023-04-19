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

import com.copyright.easiertest.Mock;
import com.copyright.easiertest.ObjectUnderTest;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.easymock.Capture;
import org.jesterj.ingest.model.*;
import org.jesterj.ingest.persistence.CassandraSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.*;

import static com.copyright.easiertest.EasierMocks.prepareMocks;
import static com.copyright.easiertest.EasierMocks.replay;
import static com.copyright.easiertest.EasierMocks.reset;
import static com.copyright.easiertest.EasierMocks.verify;
import static org.easymock.EasyMock.*;
import static org.jesterj.ingest.model.Status.*;
import static org.jesterj.ingest.model.impl.ScannerImpl.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ScannerImplTest {

  @ObjectUnderTest ScannerImpl scanner;
  @Mock private DocumentImpl docMock;
  @Mock private BoundStatement bsMock;
  @Mock private CqlSession sessionMock;
  @Mock private ResultSet rsMock;
  @Mock private Row rowMock;
  @Mock private CassandraSupport supportMock;
  @Mock private PreparedStatement statementMock;
  @Mock private BatchStatement batchMock;
  @Mock private Iterator<Row> iterMock;
  @Mock private ExecutionInfo infoMock;
  @Mock private ExecutionInfo execInfo;
  @Mock private Plan planMock;
  @Mock private Step stepMock1;
  @Mock private Step stepMock2;
  @Mock private ScannerImpl.LatestStatus lstatMock;
  @Mock private Map.Entry<String, Set<LatestStatus>> entryMock;
  @Mock private LatestStatus lstatMock2;
  @Mock private Router routerMock;

  public ScannerImplTest() {
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
  public void testDocFoundNoStatus() {
    String scannerName = "Dent, Aurthur Dent";
    docMock.stepStarted(scanner);
    docMock.setStatus(PROCESSING,"{} found doc:{}", scannerName, "42" );
    Set<String> dests = new HashSet<>();
    dests.add("fooStep");
    docMock.initDestinations(dests, scannerName);

    expect(scanner.isHashing()).andReturn(true).anyTimes();
    expect(docMock.isForceReprocess()).andReturn(false);
    expect(scanner.isFreshContent(docMock,scannerName,"42",sessionMock)).andReturn(true);
    expect(scanner.getName()).andReturn(scannerName).anyTimes();
    expect(scanner.isRemembering()).andReturn(true).anyTimes();
    expect(docMock.getId()).andReturn("42").anyTimes();
    expect(scanner.getIdFunction()).andReturn((foo) -> foo);
    expect(scanner.getCassandra()).andReturn(supportMock).anyTimes();
    expect(supportMock.getSession()).andReturn(sessionMock);
    expect(rsMock.getAvailableWithoutFetching()).andReturn(0).anyTimes();
    expect(docMock.getIdField()).andReturn("id");
    expect(docMock.removeAll("id")).andReturn(null);
    expect(docMock.put("id", "42")).andReturn(true);
    Set<Step> steps = new HashSet<>();
    steps.add(stepMock1);
    expect(scanner.getDownstreamOutputSteps()).andReturn(steps).anyTimes();
    expect(docMock.alreadyHasIncompleteStepList()).andReturn(false); //<< key test case
    expect(docMock.getStatus("fooStep")).andReturn(null);  //<< key test case

    expect(scanner.getOutputDestinationNames()).andReturn(dests).anyTimes();

    scanner.sendToNext(docMock);

    replay();
    scanner.docFound(docMock);
  }

  // FTI finds a doc marked dirty remembering but not hashing
  @Test
  public void testDocFoundDirtyStatus() {
    String scannerName = "Dent, Aurthur Dent";
    docMock.stepStarted(scanner);
    docMock.setStatus(PROCESSING,"{} found doc:{}", scannerName, "42" );
    expect(docMock.isForceReprocess()).andReturn(false);
    expect(scanner.seenPreviously(scannerName,"42",sessionMock)).andReturn(true);

    expect(scanner.getName()).andReturn(scannerName).anyTimes();
    expect(scanner.isRemembering()).andReturn(true).anyTimes();
    expect(scanner.isHashing()).andReturn(false).anyTimes();
    expect(docMock.getId()).andReturn("42").anyTimes();
    expect(scanner.getIdFunction()).andReturn((foo) -> foo);
    expect(scanner.getCassandra()).andReturn(supportMock).anyTimes();

    expect(supportMock.getSession()).andReturn(sessionMock);
    expect(docMock.getIdField()).andReturn("id");
    expect(docMock.put("id", "42")).andReturn(true);
    expect(docMock.removeAll("id")).andReturn(null);

    Set<Step> steps = new HashSet<>();
    steps.add(stepMock1);
    expect(scanner.getDownstreamOutputSteps()).andReturn(steps).anyTimes();

    expect(docMock.getStatus("dest1")).andReturn(DIRTY);
    expect(docMock.getStatus("dest2")).andReturn(DIRTY);
    expect(scanner.seenPreviously(scannerName,"42",sessionMock)).andReturn(false);
    expect(docMock.alreadyHasIncompleteStepList()).andReturn(true);
    Set<String> dests = new HashSet<>();
    dests.add("dest1");
    dests.add("dest2");
    expect(scanner.getOutputDestinationNames()).andReturn(dests);

    scanner.sendToNext(docMock);
    replay();
    scanner.docFound(docMock);
  }

  // the case where the doc was found and the status was "processing" but
  // hashing is not turned on, and somehow forceReprocess is not set
  @Test
  public void testDocFoundProcessingStatusPreviouslySeen() {
    String scannerName = "Dent, Aurthur Dent";
    // Note: this is an atypical case normally this would be set if processing status was found by FTI
    docMock.stepStarted(scanner);
    docMock.setStatus(PROCESSING,"{} found doc:{}", scannerName, "42" );
    expect(docMock.isForceReprocess()).andReturn(false);

    expect(scanner.getName()).andReturn(scannerName).anyTimes();
    expect(scanner.isRemembering()).andReturn(true).anyTimes();   // remembering
    expect(scanner.isHashing()).andReturn(false).anyTimes();      // but not hashing
    expect(docMock.getId()).andReturn("42").anyTimes();
    expect(scanner.getIdFunction()).andReturn((foo) -> foo);
    expect(scanner.getCassandra()).andReturn(supportMock).anyTimes();
    expect(supportMock.getSession()).andReturn(sessionMock);
    expect(docMock.getIdField()).andReturn("id");
    expect(docMock.put("id", "42")).andReturn(true);
    expect(docMock.removeAll("id")).andReturn(null);
    expect(scanner.seenPreviously(scannerName,"42",sessionMock)).andReturn(true);
    expect(scanner.isHeuristicallyDirty(docMock)).andReturn(false);
    Set<String> dests = new HashSet<>();
    dests.add("fooStep");
    expect(scanner.getOutputDestinationNames()).andReturn(dests);

    Set<Step> steps = new HashSet<>();
    steps.add(stepMock1);
    expect(scanner.getDownstreamOutputSteps()).andReturn(steps).anyTimes();
    expect(docMock.getStatus("fooStep")).andReturn(PROCESSING);
    replay();
    scanner.docFound(docMock);
  }

  @Test
  public void testDocFoundProcessingStatusButHeuristicDirty() {
    expect(docMock.isForceReprocess()).andReturn(false);
    String scannerName = "Dent, Aurthur Dent";
    docMock.stepStarted(scanner);
    docMock.setStatus(PROCESSING,"{} found doc:{}", scannerName, "42" );
    expect(scanner.getName()).andReturn(scannerName).anyTimes();
    expect(scanner.isRemembering()).andReturn(true).anyTimes();
    expect(scanner.isHashing()).andReturn(false).anyTimes();
    expect(docMock.getId()).andReturn("42").anyTimes();
    expect(scanner.getIdFunction()).andReturn((foo) -> foo);
    expect(scanner.getCassandra()).andReturn(supportMock).anyTimes();
    expect(supportMock.getSession()).andReturn(sessionMock);
    expect(scanner.isHeuristicallyDirty(docMock)).andReturn(true);
    expect(docMock.getIdField()).andReturn("id");
    expect(docMock.removeAll("id")).andReturn(null);
    expect(docMock.put("id", "42")).andReturn(true);
    scanner.sendToNext(docMock);
    expect(scanner.seenPreviously(scannerName,"42",sessionMock)).andReturn(true);

    Set<Step> steps = new HashSet<>();
    steps.add(stepMock1);
    expect(scanner.getDownstreamOutputSteps()).andReturn(steps).anyTimes();    expect(docMock.alreadyHasIncompleteStepList()).andReturn(true); //<< (if processing already set)
    expect(docMock.getStatus("fooStep")).andReturn(PROCESSING);
    Set<String> dests = new HashSet<>();
    dests.add("fooStep");
    expect(scanner.getOutputDestinationNames()).andReturn(dests);

    replay();
    scanner.docFound(docMock);
  }

  // Tests the case where a scanner not configured to remember anything finds a document
  @Test
  public void testDocFoundNoMemory() {
    String scannerName = "Dent, Aurthur Dent";
    docMock.stepStarted(scanner);
    docMock.setStatus(PROCESSING,"{} found doc:{}", scannerName, "42" );
    Set<String> dests = new HashSet<>();
    dests.add("fooStep");
    docMock.initDestinations(dests, scannerName);
    expect(scanner.getName()).andReturn(scannerName).anyTimes();
    expect(scanner.isRemembering()).andReturn(false).anyTimes();  // << key test case
    expect(docMock.getId()).andReturn("42").anyTimes();
    expect(scanner.getIdFunction()).andReturn((foo) -> foo);
    expect(docMock.getIdField()).andReturn("id");
    expect(docMock.removeAll("id")).andReturn(null);
    expect(docMock.put("id", "42")).andReturn(true);
    expect(docMock.isForceReprocess()).andReturn(false);
    Set<Step> steps = new HashSet<>();
    steps.add(stepMock1);
    expect(scanner.getDownstreamOutputSteps()).andReturn(steps).anyTimes();
    expect(stepMock1.getName()).andReturn("fooStep").anyTimes();

    scanner.sendToNext(docMock);

    expect(scanner.getOutputDestinationNames()).andReturn(dests);

    replay();
    scanner.docFound(docMock);
  }

  @Test
  public void testDocFoundProcessingStatusButHashChange() {
    String scannerName = "Dent, Aurthur Dent";
    docMock.stepStarted(scanner);
    docMock.setStatus(PROCESSING,"{} found doc:{}", scannerName, "42" );
    expect(scanner.isHashing()).andReturn(true).anyTimes();
    expect(docMock.isForceReprocess()).andReturn(false);
    expect(scanner.isFreshContent(docMock,scannerName,"42",sessionMock)).andReturn(true);
    expect(scanner.getName()).andReturn("Dent, Aurthur Dent").anyTimes();
    expect(scanner.isRemembering()).andReturn(true).anyTimes();
    expect(docMock.getId()).andReturn("42").anyTimes();
    expect(scanner.getIdFunction()).andReturn((foo) -> foo);
    expect(scanner.getCassandra()).andReturn(supportMock).anyTimes();
    expect(supportMock.getSession()).andReturn(sessionMock);
    expect(docMock.removeAll("id")).andReturn(null);
    expect(docMock.getIdField()).andReturn("id");
    expect(docMock.put("id", "42")).andReturn(true);
    scanner.sendToNext(docMock);

    Set<Step> steps = new HashSet<>();
    steps.add(stepMock1);
    expect(scanner.getDownstreamOutputSteps()).andReturn(steps).anyTimes();
    expect(docMock.alreadyHasIncompleteStepList()).andReturn(true); //<< (if processing already set)

    expect(docMock.getStatus("fooStep")).andReturn(PROCESSING);
    Set<String> dests = new HashSet<>();
    dests.add("fooStep");
    expect(scanner.getOutputDestinationNames()).andReturn(dests);

    replay();
    scanner.docFound(docMock);
  }

  @Test
  public void testDocFoundProcessingStatusButNoHashChange() {
    expect(docMock.isForceReprocess()).andReturn(false);
    String scannerName = "Dent, Aurthur Dent";
    docMock.stepStarted(scanner);
    docMock.setStatus(PROCESSING,"{} found doc:{}", scannerName, "42" );
    expect(scanner.isFreshContent(docMock,scannerName,"42",sessionMock)).andReturn(false);

    expect(scanner.getName()).andReturn("Dent, Aurthur Dent").anyTimes();
    expect(scanner.isRemembering()).andReturn(true).anyTimes();
    expect(scanner.isHashing()).andReturn(true).anyTimes();
    expect(docMock.getId()).andReturn("42").anyTimes();
    expect(scanner.getIdFunction()).andReturn((foo) -> foo);
    expect(scanner.getCassandra()).andReturn(supportMock).anyTimes();
    expect(supportMock.getSession()).andReturn(sessionMock);
    expect(docMock.getIdField()).andReturn("id");
    expect(docMock.put("id", "42")).andReturn(true);
    expect(docMock.removeAll("id")).andReturn(null);
    expect(scanner.isHeuristicallyDirty(docMock)).andReturn(false);
    Set<Step> steps = new HashSet<>();
    steps.add(stepMock1);
    expect(scanner.getDownstreamOutputSteps()).andReturn(steps).anyTimes();
    expect(docMock.getStatus("fooStep")).andReturn(PROCESSING);

    Set<String> dests = new HashSet<>();
    dests.add("fooStep");
    expect(scanner.getOutputDestinationNames()).andReturn(dests);

    replay();
    scanner.docFound(docMock);
  }

  @Test
  public void testDocFoundProcessingStatusButNoHash() {
    expect(docMock.isForceReprocess()).andReturn(false);
    String scannerName = "Dent, Aurthur Dent";
    docMock.stepStarted(scanner);
    docMock.setStatus(PROCESSING,"{} found doc:{}", scannerName, "42" );
    expect(scanner.isFreshContent(docMock,scannerName,"42",sessionMock)).andReturn(true);

    expect(scanner.getName()).andReturn("Dent, Aurthur Dent").anyTimes();
    expect(scanner.isRemembering()).andReturn(true).anyTimes();
    expect(scanner.isHashing()).andReturn(true).anyTimes();
    expect(docMock.getId()).andReturn("42").anyTimes();
    expect(scanner.getIdFunction()).andReturn((foo) -> foo);
    expect(scanner.getCassandra()).andReturn(supportMock).anyTimes();
    expect(supportMock.getSession()).andReturn(sessionMock);
    expect(docMock.getIdField()).andReturn("id");
    expect(docMock.removeAll("id")).andReturn(null);
    expect(docMock.put("id", "42")).andReturn(true);
    Set<Step> steps = new HashSet<>();
    steps.add(stepMock1);
    expect(scanner.getDownstreamOutputSteps()).andReturn(steps).anyTimes();
    expect(docMock.alreadyHasIncompleteStepList()).andReturn(true); //<< (if processing already set)

    expect(docMock.getStatus("fooStep")).andReturn(PROCESSING);
    Set<String> dests = new HashSet<>();
    dests.add("fooStep");
    expect(scanner.getOutputDestinationNames()).andReturn(dests);

    scanner.sendToNext(docMock);
    replay();
    scanner.docFound(docMock);
  }



  @Test
  public void testSendToNext() {
    scanner.superSendToNext(docMock);
    replay();
    scanner.sendToNext(docMock);
  }



  @SuppressWarnings("unchecked")
  @Test
  public void testActivateRemembering() {
    scanner.superActivate();
    scanner.addStepContext();
    scanner.removeStepContext();
    scanner.processPendingDocs(anyObject(FTIQueryContext.class),anyObject(List.class),eq(true));
    scanner.processPendingDocs(anyObject(FTIQueryContext.class),anyObject(List.class),eq(false));
    scanner.processErrors(anyObject(FTIQueryContext.class));
    replay();
    scanner.activate();
  }

  @Test
  public void testProcessPendingDocsByStatus() {
    scanner.ensurePersistence();
    expect(scanner.keySpace("outputStepName")).andReturn("jj_DEADBEEF");
    expect(scanner.getCassandra()).andReturn(supportMock);

    String findStrandedDocs = String.format(FIND_STRANDED_STATUS, "jj_DEADBEEF");
    expect(supportMock.getPreparedQuery(FIND_STRANDED_DOCS+"_jj_DEADBEEF",findStrandedDocs)).andReturn(statementMock).times(4);

    expect(statementMock.bind("PROCESSING")).andReturn(bsMock);
    expect(statementMock.bind("BATCHED")).andReturn(bsMock);
    expect(statementMock.bind("RESTART")).andReturn(bsMock);
    expect(statementMock.bind("FORCE")).andReturn(bsMock);
    expect(bsMock.setTimeout(Duration.ofSeconds(600))).andReturn(bsMock).times(4);
    expect(supportMock.getSession()).andReturn(sessionMock);
    expect(sessionMock.execute(bsMock)).andReturn(rsMock).times(4);
    expect(rsMock.iterator()).andReturn(iterMock).times(4);
    expect(iterMock.hasNext()).andReturn(true);
    expect(iterMock.hasNext()).andReturn(false);
    expect(iterMock.hasNext()).andReturn(true);
    expect(iterMock.hasNext()).andReturn(false);
    expect(iterMock.hasNext()).andReturn(true);
    expect(iterMock.hasNext()).andReturn(false);
    expect(iterMock.hasNext()).andReturn(true);
    expect(iterMock.hasNext()).andReturn(false);
    expect(iterMock.next()).andReturn(rowMock).times(4);
    expect(rowMock.getString(0)).andReturn("foobarId").times(4); // slightly lazy could return 4 diff ids, but this is complicated enough
    expect(scanner.isActive()).andReturn(true).anyTimes();
    Map<String,LatestStatus> cache = new HashMap<>();
    expect(scanner.findLatestSatus(findStrandedDocs, "foobarId","outputStepName", cache)).andReturn(lstatMock);
    expect(scanner.findLatestSatus(findStrandedDocs, "foobarId","outputStepName", cache)).andReturn(lstatMock);
    expect(scanner.findLatestSatus(findStrandedDocs, "foobarId","outputStepName", cache)).andReturn(lstatMock);
    expect(scanner.findLatestSatus(findStrandedDocs, "foobarId","outputStepName", cache)).andReturn(lstatMock);
    expect(lstatMock.getStatus()).andReturn(PROCESSING.toString()).times(4);

    Set<Step> steps = new HashSet<>();
    steps.add(stepMock1);
    expect(scanner.getDownstreamOutputSteps()).andReturn(steps).anyTimes();
    Set<String> sentAlready = new HashSet<>();
    Capture<Map.Entry<String, Set<LatestStatus>>> c = newCapture();
    //noinspection DataFlowIssue
    scanner.process(eq(true),eq(sentAlready),capture(c), eq(FTI_ORIGIN));
    Set<String> dests = new HashSet<>();
    dests.add("outputStepName");
    expect(scanner.getOutputDestinationNames()).andReturn(dests);

    replay();
    FTIQueryContext src = new FTIQueryContext(sentAlready);
    scanner.processPendingDocs(src, List.of(PROCESSING, BATCHED, RESTART, FORCE), true);
    Map.Entry<String, Set<LatestStatus>> captured = c.getValue();
    assertEquals("foobarId", captured.getKey());
    assertEquals(1,captured.getValue().size());
    assertEquals(lstatMock,captured.getValue().iterator().next());
  }

  @Test
  public void testProcess() {
    Set<String> sentAlready = new HashSet<>();
    Map.Entry<String, Set<LatestStatus>> toProcess = entryMock;

    expect(entryMock.getKey()).andReturn("fooId");
    expect(scanner.fetchById("fooId",FTI_ORIGIN)).andReturn(Optional.of(docMock));
    docMock.setForceReprocess(true);
    Capture<Map<String, DocDestinationStatus>> downstream = newCapture();
    docMock.setIncompleteOutputDestinations(capture(downstream));
    Set<LatestStatus> stats = new HashSet<>();
    stats.add(lstatMock);
    stats.add(lstatMock2);
    expect(entryMock.getValue()).andReturn(stats);
    expect(lstatMock.getoutputStepName()).andReturn("outputStep1").anyTimes();
    expect(lstatMock2.getoutputStepName()).andReturn("outputStep2").anyTimes();
    expect(lstatMock.getStatus()).andReturn(ERROR.toString()).anyTimes();
    expect(lstatMock2.getStatus()).andReturn(ERROR.toString()).anyTimes();
    expect(lstatMock.getTimestamp()).andReturn("whenever");
    expect(lstatMock2.getTimestamp()).andReturn("slightly later");
    expect(scanner.docFound(docMock)).andReturn(true);

    replay();
    scanner.process(true, sentAlready, toProcess, FTI_ORIGIN);

    assertEquals(1, sentAlready.size() );
    assertEquals("fooId", sentAlready.iterator().next() );
    Map<String, DocDestinationStatus> downstreamValue = downstream.getValue();
    assertEquals(2, downstreamValue.size());
    DocDestinationStatus s1 = downstreamValue.get("outputStep1");
    assertNotNull(s1);
    assertEquals("Prior status:outputStep1>ERROR@whenever", s1.getMessage());
    assertEquals(0, s1.getMessageParams().length);
    DocDestinationStatus s2 = downstreamValue.get("outputStep2");
    assertEquals("Prior status:outputStep2>ERROR@slightly later", s2.getMessage());
    assertEquals(0, s2.getMessageParams().length);
  }
}
