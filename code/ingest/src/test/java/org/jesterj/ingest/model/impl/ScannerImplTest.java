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
import static org.easymock.EasyMock.expect;
import static org.jesterj.ingest.model.Status.*;


/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 9/26/16
 */
@SuppressWarnings("SpellCheckingInspection")
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
    expect(scanner.getName()).andReturn("Dent, Aurthur Dent").anyTimes();
    expect(scanner.isRemembering()).andReturn(true).anyTimes();
    expect(docMock.getId()).andReturn("42").anyTimes();
    expect(scanner.getIdFunction()).andReturn((foo) -> foo);
    expect(scanner.getCassandra()).andReturn(supportMock).anyTimes();
    expect(supportMock.getPreparedQuery(ScannerImpl.FTI_CHECK_DOC_HASH_Q)).andReturn(statementMock);
    expect(supportMock.getSession()).andReturn(sessionMock);
    expect(statementMock.bind("42", "Dent, Aurthur Dent")).andReturn(bsMock);
    expect(sessionMock.execute(bsMock)).andReturn(rsMock);
    expect(rsMock.getAvailableWithoutFetching()).andReturn(0).anyTimes();
    expect(docMock.getIdField()).andReturn("id");
    expect(docMock.removeAll("id")).andReturn(null);
    expect(docMock.put("id", "42")).andReturn(true);
    scanner.sendToNext(docMock);
    expect(rsMock.getExecutionInfo()).andReturn(execInfo);
    expect(execInfo.getErrors()).andReturn(new ArrayList<>());

    replay();
    scanner.docFound(docMock);
  }

  @Test
  public void testDocFoundDirtyStatus() {
    expect(scanner.getName()).andReturn("Dent, Aurthur Dent").anyTimes();
    expect(scanner.isRemembering()).andReturn(true).anyTimes();
    expect(scanner.isHashing()).andReturn(false).anyTimes();
    expect(docMock.getId()).andReturn("42").anyTimes();
    expect(scanner.getIdFunction()).andReturn((foo) -> foo);
    expect(scanner.getCassandra()).andReturn(supportMock).anyTimes();
    expect(supportMock.getPreparedQuery(ScannerImpl.FTI_CHECK_DOC_HASH_Q)).andReturn(statementMock);
    expect(supportMock.getSession()).andReturn(sessionMock);
    expect(statementMock.bind("42", "Dent, Aurthur Dent")).andReturn(bsMock);
    expect(sessionMock.execute(bsMock)).andReturn(rsMock);
    expect(rsMock.getAvailableWithoutFetching()).andReturn(1).anyTimes();
    expect(rsMock.isFullyFetched()).andReturn(true);
    List<Row> rows = new ArrayList<>();
    rows.add(rowMock);
    expect(rsMock.all()).andReturn(rows);
    expect(rowMock.getString(0)).andReturn("DIRTY");
    expect(rowMock.getInt(2)).andReturn(0);
    expect(docMock.getIdField()).andReturn("id");
    expect(docMock.put("id", "42")).andReturn(true);
    expect(docMock.removeAll("id")).andReturn(null);
    expect(rsMock.getExecutionInfo()).andReturn(execInfo);
    expect(execInfo.getErrors()).andReturn(new ArrayList<>());

    scanner.sendToNext(docMock);
    replay();
    scanner.docFound(docMock);
  }

  // the case where the doc was found and the status was "processing" but
  // hashing is not turned on.
  @Test
  public void testDocFoundProcessingStatus() {
    expect(scanner.getName()).andReturn("Dent, Aurthur Dent").anyTimes();
    expect(scanner.isRemembering()).andReturn(true).anyTimes();   // remembering
    expect(scanner.isHashing()).andReturn(false).anyTimes();      // but not hashing
    expect(docMock.getId()).andReturn("42").anyTimes();
    expect(scanner.getIdFunction()).andReturn((foo) -> foo);
    expect(scanner.getCassandra()).andReturn(supportMock).anyTimes();
    expect(supportMock.getPreparedQuery(ScannerImpl.FTI_CHECK_DOC_HASH_Q)).andReturn(statementMock);
    expect(supportMock.getSession()).andReturn(sessionMock);
    expect(statementMock.bind("42", "Dent, Aurthur Dent")).andReturn(bsMock);
    expect(sessionMock.execute(bsMock)).andReturn(rsMock);
    expect(rsMock.getAvailableWithoutFetching()).andReturn(1).anyTimes();
    expect(rsMock.isFullyFetched()).andReturn(true);
    List<Row> rows = new ArrayList<>();
    rows.add(rowMock);
    expect(rsMock.all()).andReturn(rows);
    expect(rowMock.getString(0)).andReturn("PROCESSING");
    expect(rowMock.getInt(2)).andReturn(0);
    expect(docMock.getIdField()).andReturn("id");
    expect(docMock.put("id", "42")).andReturn(true);
    expect(docMock.removeAll("id")).andReturn(null);
    expect(scanner.heuristicDirty(docMock)).andReturn(false);
    expect(rsMock.getExecutionInfo()).andReturn(execInfo);
    expect(execInfo.getErrors()).andReturn(new ArrayList<>());

    replay();
    scanner.docFound(docMock);
  }

  @Test
  public void testDocFoundProcessingStatusButHeuristicDirty() {
    expect(scanner.getName()).andReturn("Dent, Aurthur Dent").anyTimes();
    expect(scanner.isRemembering()).andReturn(true).anyTimes();
    expect(scanner.isHashing()).andReturn(false);
    expect(docMock.getId()).andReturn("42").anyTimes();
    expect(scanner.getIdFunction()).andReturn((foo) -> foo);
    expect(scanner.getCassandra()).andReturn(supportMock).anyTimes();
    expect(supportMock.getPreparedQuery(ScannerImpl.FTI_CHECK_DOC_HASH_Q)).andReturn(statementMock);
    expect(supportMock.getSession()).andReturn(sessionMock);
    expect(statementMock.bind("42", "Dent, Aurthur Dent")).andReturn(bsMock);
    expect(sessionMock.execute(bsMock)).andReturn(rsMock);
    expect(rsMock.getAvailableWithoutFetching()).andReturn(1).anyTimes();
    expect(rsMock.isFullyFetched()).andReturn(true);
    List<Row> rows = new ArrayList<>();
    rows.add(rowMock);
    expect(rsMock.all()).andReturn(rows);
    expect(rowMock.getString(0)).andReturn("PROCESSING");
    expect(rowMock.getInt(2)).andReturn(0);
    expect(scanner.heuristicDirty(docMock)).andReturn(true);
    expect(docMock.getIdField()).andReturn("id");
    expect(docMock.removeAll("id")).andReturn(null);
    expect(docMock.put("id", "42")).andReturn(true);
    scanner.sendToNext(docMock);
    expect(rsMock.getExecutionInfo()).andReturn(execInfo);
    expect(execInfo.getErrors()).andReturn(new ArrayList<>());

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
    scanner.sendToNext(docMock);
    replay();
    scanner.docFound(docMock);
  }

  @Test
  public void testDocFoundProcessingStatusButHashChange() {
    expect(scanner.getName()).andReturn("Dent, Aurthur Dent").anyTimes();
    expect(scanner.isRemembering()).andReturn(true).anyTimes();
    expect(scanner.isHashing()).andReturn(true).anyTimes();
    expect(docMock.getId()).andReturn("42").anyTimes();
    expect(scanner.getIdFunction()).andReturn((foo) -> foo);
    expect(scanner.getCassandra()).andReturn(supportMock).anyTimes();
    expect(supportMock.getPreparedQuery(ScannerImpl.FTI_CHECK_DOC_HASH_Q)).andReturn(statementMock);
    expect(supportMock.getSession()).andReturn(sessionMock);
    expect(statementMock.bind("42", "Dent, Aurthur Dent")).andReturn(bsMock);
    expect(sessionMock.execute(bsMock)).andReturn(rsMock);
    expect(rsMock.getAvailableWithoutFetching()).andReturn(1).anyTimes();
    expect(rsMock.isFullyFetched()).andReturn(true);
    List<Row> rows = new ArrayList<>();
    rows.add(rowMock);
    expect(rsMock.all()).andReturn(rows);
    expect(rowMock.getString(0)).andReturn("PROCESSING");
    expect(rowMock.getString(1)).andReturn("CAFEBABE");
    expect(rowMock.getInt(2)).andReturn(0);
    expect(scanner.heuristicDirty(docMock)).andReturn(false);
    expect(docMock.getHash()).andReturn("DEADBEEF");
    expect(docMock.removeAll("id")).andReturn(null);
    expect(docMock.getIdField()).andReturn("id");
    expect(docMock.put("id", "42")).andReturn(true);
    scanner.sendToNext(docMock);
    expect(rsMock.getExecutionInfo()).andReturn(execInfo);
    expect(execInfo.getErrors()).andReturn(new ArrayList<>());

    replay();
    scanner.docFound(docMock);
  }

  @Test
  public void testDocFoundProcessingStatusButNoHashChange() {
    expect(scanner.getName()).andReturn("Dent, Aurthur Dent").anyTimes();
    expect(scanner.isRemembering()).andReturn(true).anyTimes();
    expect(scanner.isHashing()).andReturn(true).anyTimes();
    expect(docMock.getId()).andReturn("42").anyTimes();
    expect(scanner.getIdFunction()).andReturn((foo) -> foo);
    expect(scanner.getCassandra()).andReturn(supportMock).anyTimes();
    expect(supportMock.getPreparedQuery(ScannerImpl.FTI_CHECK_DOC_HASH_Q)).andReturn(statementMock);
    expect(supportMock.getSession()).andReturn(sessionMock);
    expect(statementMock.bind("42", "Dent, Aurthur Dent")).andReturn(bsMock);
    expect(sessionMock.execute(bsMock)).andReturn(rsMock);
    expect(rsMock.getAvailableWithoutFetching()).andReturn(1).anyTimes();
    expect(rsMock.isFullyFetched()).andReturn(true);
    List<Row> rows = new ArrayList<>();
    rows.add(rowMock);
    expect(rsMock.all()).andReturn(rows);
    expect(rowMock.getString(0)).andReturn("PROCESSING");
    expect(rowMock.getString(1)).andReturn("CAFEBABE");
    expect(rowMock.getInt(2)).andReturn(0);
    expect(docMock.getIdField()).andReturn("id");
    expect(docMock.put("id", "42")).andReturn(true);
    expect(docMock.removeAll("id")).andReturn(null);
    expect(scanner.heuristicDirty(docMock)).andReturn(false);
    expect(docMock.getHash()).andReturn("CAFEBABE");
    expect(rsMock.getExecutionInfo()).andReturn(execInfo);
    expect(execInfo.getErrors()).andReturn(new ArrayList<>());

    replay();
    scanner.docFound(docMock);
  }

  @Test
  public void testDocFoundProcessingStatusButNoHash() {
    expect(scanner.getName()).andReturn("Dent, Aurthur Dent").anyTimes();
    expect(scanner.isRemembering()).andReturn(true).anyTimes();
    expect(scanner.isHashing()).andReturn(true).anyTimes();
    expect(docMock.getId()).andReturn("42").anyTimes();
    expect(scanner.getIdFunction()).andReturn((foo) -> foo);
    expect(scanner.getCassandra()).andReturn(supportMock).anyTimes();
    expect(supportMock.getPreparedQuery(ScannerImpl.FTI_CHECK_DOC_HASH_Q)).andReturn(statementMock);
    expect(supportMock.getSession()).andReturn(sessionMock);
    expect(statementMock.bind("42", "Dent, Aurthur Dent")).andReturn(bsMock);
    expect(sessionMock.execute(bsMock)).andReturn(rsMock);
    expect(rsMock.getAvailableWithoutFetching()).andReturn(1).anyTimes();
    expect(rsMock.isFullyFetched()).andReturn(true);
    List<Row> rows = new ArrayList<>();
    rows.add(rowMock);
    expect(rsMock.all()).andReturn(rows);
    expect(rowMock.getString(0)).andReturn("PROCESSING");
    expect(rowMock.getString(1)).andReturn(null);
    expect(rowMock.getInt(2)).andReturn(0);
    expect(scanner.heuristicDirty(docMock)).andReturn(false);
    expect(docMock.getIdField()).andReturn("id");
    expect(docMock.removeAll("id")).andReturn(null);
    expect(docMock.put("id", "42")).andReturn(true);
    expect(rsMock.getExecutionInfo()).andReturn(execInfo);
    expect(execInfo.getErrors()).andReturn(new ArrayList<>());

    scanner.sendToNext(docMock);
    replay();
    scanner.docFound(docMock);
  }

  @Test(expected = RuntimeException.class)
  public void testMultiplePrimaryKeyError1() {
    expect(scanner.getName()).andReturn("Dent, Aurthur Dent").anyTimes();
    expect(docMock.getIdField()).andReturn("id");
    expect(docMock.removeAll("id")).andReturn(null);
    expect(docMock.put("id", "42")).andReturn(true);
    expect(scanner.isRemembering()).andReturn(true).anyTimes();
    expect(docMock.getId()).andReturn("42").anyTimes();
    expect(scanner.getIdFunction()).andReturn((foo) -> foo);
    expect(scanner.getCassandra()).andReturn(supportMock).anyTimes();
    expect(supportMock.getPreparedQuery(ScannerImpl.FTI_CHECK_DOC_HASH_Q)).andReturn(statementMock);
    expect(supportMock.getSession()).andReturn(sessionMock);
    expect(statementMock.bind("42", "Dent, Aurthur Dent")).andReturn(bsMock);
    expect(sessionMock.execute(bsMock)).andReturn(rsMock);
    expect(rsMock.getAvailableWithoutFetching()).andReturn(2).anyTimes();
    expect(rsMock.getExecutionInfo()).andReturn(execInfo);
    expect(execInfo.getErrors()).andReturn(new ArrayList<>());
    replay();
    scanner.docFound(docMock);
  }


  @Test(expected = RuntimeException.class)
  public void testMultiplePrimaryKeyError2() {
    expect(scanner.getName()).andReturn("Dent, Aurthur Dent").anyTimes();
    expect(docMock.getIdField()).andReturn("id");
    expect(docMock.removeAll("id")).andReturn(null);
    expect(docMock.put("id", "42")).andReturn(true);
    expect(scanner.isRemembering()).andReturn(true).anyTimes();
    expect(docMock.getId()).andReturn("42").anyTimes();
    expect(scanner.getIdFunction()).andReturn((foo) -> foo);
    expect(scanner.getCassandra()).andReturn(supportMock).anyTimes();
    expect(supportMock.getPreparedQuery(ScannerImpl.FTI_CHECK_DOC_HASH_Q)).andReturn(statementMock);
    expect(supportMock.getSession()).andReturn(sessionMock);
    expect(statementMock.bind("42", "Dent, Aurthur Dent")).andReturn(bsMock);
    expect(sessionMock.execute(bsMock)).andReturn(rsMock);
    expect(rsMock.getAvailableWithoutFetching()).andReturn(1).anyTimes();
    expect(rsMock.isFullyFetched()).andReturn(false);
    expect(rsMock.getExecutionInfo()).andReturn(execInfo);
    expect(execInfo.getErrors()).andReturn(new ArrayList<>());

    replay();
    scanner.docFound(docMock);
  }

  @Test
  public void testSendToNext() {
    expect(scanner.isRemembering()).andReturn(true);
    expect(scanner.getCassandra()).andReturn(supportMock).anyTimes();
    expect(supportMock.getSession()).andReturn(sessionMock);
    expect(docMock.getHash()).andReturn("DEADBEEF");
    expect(docMock.getId()).andReturn("42").anyTimes();
    expect(docMock.getSourceScannerName()).andReturn("Arthur Dent");
    expect(statementMock.bind("DEADBEEF", "42", "Arthur Dent")).andReturn(bsMock);
    expect(sessionMock.execute(bsMock)).andReturn(null);
    //expect(bsMock.setTimeout(Duration.ofSeconds(600))).andReturn(bsMock);
    scanner.superSendToNext(docMock);
    replay();
    scanner.sendToNext(docMock);
  }



  @Test
  public void testActivateRemembering() {
    expect(scanner.isRemembering()).andReturn(true);
    expect(scanner.getCassandra()).andReturn(supportMock).anyTimes();
    expect(supportMock.getSession()).andReturn(sessionMock);
    List<ScannerImpl.DocKey> docKeys = new ArrayList<>();
    expect(scanner.createList()).andReturn(docKeys);

    docKeys.add(mockKey); // would happen during an adToDirtyList as side effect
    expect(scanner.createCassandraBatch()).andReturn(batchMock);
    expect(mockKey.getDocid()).andReturn("foo");
    expect(mockKey.getScanner()).andReturn("bar");
    expect(statementMock.bind("foo","bar")).andReturn(bsMock);
    expect(bsMock.setTimeout(Duration.ofSeconds(600))).andReturn(bsMock);
    List<BoundStatement> boundStatements = new ArrayList<>();
    expect(scanner.createListBS()).andReturn(boundStatements);
    boundStatements.add(bsMock);
    expect(batchMock.addAll(boundStatements)).andReturn(batchMock);
    expect(batchMock.setTimeout(Duration.ofSeconds(600))).andReturn(batchMock);

    expect(sessionMock.execute(batchMock)).andReturn(null); // unused
    scanner.superActivate();
    replay();
    scanner.activate();
  }

  @Test
  public void testProcessDocsByStatus() {
    expect(supportMock.getPreparedQuery("somequery")).andReturn(statementMock);
    expect(scanner.getName()).andReturn("foo");
    expect(statementMock.bind("foo")).andReturn(bsMock);
    expect(bsMock.setTimeout(Duration.ofSeconds(600))).andReturn(bsMock);
    expect(supportMock.getSession()).andReturn(sessionMock);
    expect(sessionMock.execute(bsMock)).andReturn(rsMock);
    expect(rsMock.iterator()).andReturn(iterMock);
    expect(iterMock.hasNext()).andReturn(true).times(1);
    expect(iterMock.hasNext()).andReturn(false).times(1);
    expect(iterMock.next()).andReturn(rowMock);
    expect(rowMock.getString(0)).andReturn("foobarId");
    expect(scanner.fetchById("foobarId")).andReturn(Optional.of(docMock));
    expect(scanner.isActive()).andReturn(true).anyTimes();
    scanner.docFound(docMock);

    replay();
    List<String> sentAlready = new ArrayList<>();
    FTIQueryContext src = new FTIQueryContext(sentAlready);
    scanner.processPendingDocs(src, List.of(PROCESSING, BATCHED, RESTART, DIRTY));
  }
}
