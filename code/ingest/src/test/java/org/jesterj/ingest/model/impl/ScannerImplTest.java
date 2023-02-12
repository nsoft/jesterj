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
import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.persistence.CassandraSupport;
import org.jesterj.ingest.model.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.copyright.easiertest.EasierMocks.prepareMocks;
import static com.copyright.easiertest.EasierMocks.replay;
import static com.copyright.easiertest.EasierMocks.reset;
import static com.copyright.easiertest.EasierMocks.verify;
import static org.easymock.EasyMock.*;
import static org.jesterj.ingest.model.Status.*;
import static org.jesterj.ingest.model.impl.ScannerImpl.*;

public class ScannerImplTest {

  @ObjectUnderTest ScannerImpl scanner;
  @Mock private Document docMock;
  @Mock private BoundStatement bsMock;
  @Mock private CqlSession sessionMock;
  @Mock private ResultSet rsMock;
  @Mock private Row rowMock;
  @Mock private CassandraSupport supportMock;
  @Mock private PreparedStatement statementMock;
  @Mock private ScannerImpl.DocKey mockKey;
  @Mock private BatchStatement batchMock;
  @Mock private Iterator<Row> iterMock;
  @Mock private ExecutionInfo infoMock;
  @Mock private ExecutionInfo execInfo;
  @Mock private Plan planMock;

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
    expect(scanner.isHashing()).andReturn(true).anyTimes();
    expect(docMock.isForceReprocess()).andReturn(false);
    String scannerName = "Dent, Aurthur Dent";
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
    expect(docMock.getStatus()).andReturn(PROCESSING).times(2);

    scanner.sendToNext(docMock);

    replay();
    scanner.docFound(docMock);
  }

  @Test
  public void testDocFoundDirtyStatus() {
    expect(docMock.isForceReprocess()).andReturn(false);
    String scannerName = "Dent, Aurthur Dent";
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
    expect(docMock.getStatus()).andReturn(DIRTY).times(2);
    expect(scanner.heuristicDirty(docMock)).andReturn(false);
    docMock.setStatus(PROCESSING);


    scanner.sendToNext(docMock);
    replay();
    scanner.docFound(docMock);
  }

  // the case where the doc was found and the status was "processing" but
  // hashing is not turned on.
  @Test
  public void testDocFoundProcessingStatus() {
    expect(docMock.isForceReprocess()).andReturn(false);
    String scannerName = "Dent, Aurthur Dent";
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
    expect(scanner.heuristicDirty(docMock)).andReturn(false);

    expect(docMock.getStatus()).andReturn(PROCESSING);

    replay();
    scanner.docFound(docMock);
  }

  @Test
  public void testDocFoundProcessingStatusButHeuristicDirty() {
    expect(docMock.isForceReprocess()).andReturn(false);
    String scannerName = "Dent, Aurthur Dent";
    expect(scanner.getName()).andReturn(scannerName).anyTimes();
    expect(scanner.isRemembering()).andReturn(true).anyTimes();
    expect(scanner.isHashing()).andReturn(false).anyTimes();
    expect(docMock.getId()).andReturn("42").anyTimes();
    expect(scanner.getIdFunction()).andReturn((foo) -> foo);
    expect(scanner.getCassandra()).andReturn(supportMock).anyTimes();
    expect(supportMock.getSession()).andReturn(sessionMock);
    expect(scanner.heuristicDirty(docMock)).andReturn(true);
    expect(docMock.getIdField()).andReturn("id");
    expect(docMock.removeAll("id")).andReturn(null);
    expect(docMock.put("id", "42")).andReturn(true);
    scanner.sendToNext(docMock);
    expect(scanner.seenPreviously(scannerName,"42",sessionMock)).andReturn(true);
    expect(docMock.getStatus()).andReturn(PROCESSING).times(2);



    replay();
    scanner.docFound(docMock);
  }

  @Test
  public void testDocFoundNoMemory() {
    expect(scanner.getName()).andReturn("Dent, Aurthur Dent").anyTimes();
    expect(scanner.isRemembering()).andReturn(false).anyTimes();
    expect(docMock.getId()).andReturn("42").anyTimes();
    expect(scanner.getIdFunction()).andReturn((foo) -> foo);
    expect(docMock.getIdField()).andReturn("id");
    expect(docMock.removeAll("id")).andReturn(null);
    expect(docMock.put("id", "42")).andReturn(true);
    expect(docMock.getStatus()).andReturn(PROCESSING);

    scanner.sendToNext(docMock);

    replay();
    scanner.docFound(docMock);
  }

  @Test
  public void testDocFoundProcessingStatusButHashChange() {
    expect(scanner.isHashing()).andReturn(true).anyTimes();
    expect(docMock.isForceReprocess()).andReturn(false);
    String scannerName = "Dent, Aurthur Dent";
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
    expect(docMock.getStatus()).andReturn(PROCESSING).times(2);

    replay();
    scanner.docFound(docMock);
  }

  @Test
  public void testDocFoundProcessingStatusButNoHashChange() {
    expect(docMock.isForceReprocess()).andReturn(false);
    String scannerName = "Dent, Aurthur Dent";
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
    expect(scanner.heuristicDirty(docMock)).andReturn(false);
    expect(docMock.getStatus()).andReturn(PROCESSING);

    replay();
    scanner.docFound(docMock);
  }

  @Test
  public void testDocFoundProcessingStatusButNoHash() {
    expect(docMock.isForceReprocess()).andReturn(false);
    String scannerName = "Dent, Aurthur Dent";
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
    expect(docMock.getStatus()).andReturn(PROCESSING).times(2);


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
    expect(scanner.keySpace()).andReturn("keyspace");
    expect(scanner.getCassandra()).andReturn(supportMock);

    String findStrandedDocs = String.format(FIND_STRANDED_STATUS, "keyspace");
    expect(supportMock.getPreparedQuery(FIND_STRANDED_DOCS,findStrandedDocs)).andReturn(statementMock).times(4);

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
    expect(scanner.fetchById("foobarId")).andReturn(Optional.of(docMock)).times(4);
    expect(scanner.isActive()).andReturn(true).anyTimes();
    expect(scanner.findLatestSatus(findStrandedDocs, "foobarId")).andReturn("PROCESSING");
    expect(scanner.findLatestSatus(findStrandedDocs, "foobarId")).andReturn("BATCHED");
    expect(scanner.findLatestSatus(findStrandedDocs, "foobarId")).andReturn("RESTART");
    expect(scanner.findLatestSatus(findStrandedDocs, "foobarId")).andReturn("FORCE");

    docMock.setForceReprocess(true);
    expectLastCall().times(3);
    docMock.setForceReprocess(true);
    expectLastCall().times(1);
    scanner.docFound(docMock);
    expectLastCall().times(4);

    replay();
    List<String> sentAlready = new ArrayList<>();
    FTIQueryContext src = new FTIQueryContext(sentAlready);
    scanner.processPendingDocs(src, List.of(PROCESSING, BATCHED, RESTART, FORCE), true);
  }
}
