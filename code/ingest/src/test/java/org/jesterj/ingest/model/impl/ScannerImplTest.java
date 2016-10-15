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
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.jesterj.ingest.logging.CassandraSupport;
import org.jesterj.ingest.model.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.copyright.easiertest.EasierMocks.prepareMocks;
import static com.copyright.easiertest.EasierMocks.replay;
import static com.copyright.easiertest.EasierMocks.reset;
import static com.copyright.easiertest.EasierMocks.verify;
import static org.easymock.EasyMock.expect;
import static org.jesterj.ingest.model.impl.ScannerImpl.UPDATE_HASH_U;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 9/26/16
 */
public class ScannerImplTest {

  @ObjectUnderTest ScannerImpl scanner;
  @Mock private Document docMock;
  @Mock private BoundStatement bsMock;
  @Mock private Session sessionMock;
  @Mock private ResultSet rsMock;
  @Mock private Row rowMock;
  @Mock private CassandraSupport supportMock;
  @Mock private PreparedStatement statementMock;

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
    expect(scanner.isRemembering()).andReturn(true).anyTimes();
    expect(docMock.getId()).andReturn("42").anyTimes();
    expect(scanner.getIdFunction()).andReturn((foo) -> foo);
    expect(scanner.createBoundStatement(statementMock)).andReturn(bsMock);
    expect(scanner.getCassandra()).andReturn(supportMock).anyTimes();
    expect(supportMock.getPreparedQuery(ScannerImpl.FTI_CHECK_Q)).andReturn(statementMock);
    expect(supportMock.getSession()).andReturn(sessionMock);
    expect(scanner.getName()).andReturn("Dent, Aurthur Dent");
    expect(bsMock.bind("42", "Dent, Aurthur Dent")).andReturn(bsMock);
    expect(sessionMock.execute(bsMock)).andReturn(rsMock);
    expect(rsMock.getAvailableWithoutFetching()).andReturn(0).anyTimes();
    expect(docMock.getIdField()).andReturn("id");
    expect(docMock.removeAll("id")).andReturn(null);
    expect(docMock.put("id", "42")).andReturn(true);
    scanner.sendToNext(docMock);
    replay();
    scanner.docFound(docMock);
  }

  @Test
  public void testDocFoundDirtyStatus() {
    expect(scanner.isRemembering()).andReturn(true).anyTimes();
    expect(scanner.isHashing()).andReturn(false).anyTimes();
    expect(docMock.getId()).andReturn("42").anyTimes();
    expect(scanner.getIdFunction()).andReturn((foo) -> foo);
    expect(scanner.createBoundStatement(statementMock)).andReturn(bsMock);
    expect(scanner.getCassandra()).andReturn(supportMock).anyTimes();
    expect(supportMock.getPreparedQuery(ScannerImpl.FTI_CHECK_Q)).andReturn(statementMock);
    expect(supportMock.getSession()).andReturn(sessionMock);
    expect(scanner.getName()).andReturn("Dent, Aurthur Dent");
    expect(bsMock.bind("42", "Dent, Aurthur Dent")).andReturn(bsMock);
    expect(sessionMock.execute(bsMock)).andReturn(rsMock);
    expect(rsMock.getAvailableWithoutFetching()).andReturn(1).anyTimes();
    expect(rsMock.isFullyFetched()).andReturn(true);
    List<Row> rows = new ArrayList<>();
    rows.add(rowMock);
    expect(rsMock.all()).andReturn(rows);
    expect(rowMock.getString(0)).andReturn("DIRTY");
    expect(docMock.getIdField()).andReturn("id");
    expect(docMock.put("id", "42")).andReturn(true);
    expect(docMock.removeAll("id")).andReturn(null);

    scanner.sendToNext(docMock);
    replay();
    scanner.docFound(docMock);
  }

  // the case where the doc was found and the status was "proscessing" but 
  // hashing is not turned on.
  @Test
  public void testDocFoundProcessingStatus() {
    expect(scanner.isRemembering()).andReturn(true).anyTimes();
    expect(scanner.isHashing()).andReturn(false).anyTimes();
    expect(docMock.getId()).andReturn("42").anyTimes();
    expect(scanner.getIdFunction()).andReturn((foo) -> foo);
    expect(scanner.createBoundStatement(statementMock)).andReturn(bsMock);
    expect(scanner.getCassandra()).andReturn(supportMock).anyTimes();
    expect(supportMock.getPreparedQuery(ScannerImpl.FTI_CHECK_Q)).andReturn(statementMock);
    expect(supportMock.getSession()).andReturn(sessionMock);
    expect(scanner.getName()).andReturn("Dent, Aurthur Dent");
    expect(bsMock.bind("42", "Dent, Aurthur Dent")).andReturn(bsMock);
    expect(sessionMock.execute(bsMock)).andReturn(rsMock);
    expect(rsMock.getAvailableWithoutFetching()).andReturn(1).anyTimes();
    expect(rsMock.isFullyFetched()).andReturn(true);
    List<Row> rows = new ArrayList<>();
    rows.add(rowMock);
    expect(rsMock.all()).andReturn(rows);
    expect(rowMock.getString(0)).andReturn("PROCESSING");
    expect(docMock.getIdField()).andReturn("id");
    expect(docMock.put("id", "42")).andReturn(true);
    expect(docMock.removeAll("id")).andReturn(null);
    expect(scanner.heuristicDirty(docMock)).andReturn(false);
    replay();
    scanner.docFound(docMock);
  }

  @Test
  public void testDocFoundProcessingStatusButHeuristicDirty() {
    expect(scanner.isRemembering()).andReturn(true).anyTimes();
    expect(scanner.isHashing()).andReturn(false);
    expect(docMock.getId()).andReturn("42").anyTimes();
    expect(scanner.getIdFunction()).andReturn((foo) -> foo);
    expect(scanner.createBoundStatement(statementMock)).andReturn(bsMock);
    expect(scanner.getCassandra()).andReturn(supportMock).anyTimes();
    expect(supportMock.getPreparedQuery(ScannerImpl.FTI_CHECK_Q)).andReturn(statementMock);
    expect(supportMock.getSession()).andReturn(sessionMock);
    expect(scanner.getName()).andReturn("Dent, Aurthur Dent");
    expect(bsMock.bind("42", "Dent, Aurthur Dent")).andReturn(bsMock);
    expect(sessionMock.execute(bsMock)).andReturn(rsMock);
    expect(rsMock.getAvailableWithoutFetching()).andReturn(1).anyTimes();
    expect(rsMock.isFullyFetched()).andReturn(true);
    List<Row> rows = new ArrayList<>();
    rows.add(rowMock);
    expect(rsMock.all()).andReturn(rows);
    expect(rowMock.getString(0)).andReturn("PROCESSING");
    expect(scanner.heuristicDirty(docMock)).andReturn(true);
    expect(docMock.getIdField()).andReturn("id");
    expect(docMock.removeAll("id")).andReturn(null);
    expect(docMock.put("id", "42")).andReturn(true);
    scanner.sendToNext(docMock);
    replay();
    scanner.docFound(docMock);
  }

  @Test
  public void testDocFoundNoMemory() {
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
    expect(scanner.isRemembering()).andReturn(true).anyTimes();
    expect(scanner.isHashing()).andReturn(true).anyTimes();
    expect(docMock.getId()).andReturn("42").anyTimes();
    expect(scanner.getIdFunction()).andReturn((foo) -> foo);
    expect(scanner.createBoundStatement(statementMock)).andReturn(bsMock);
    expect(scanner.getCassandra()).andReturn(supportMock).anyTimes();
    expect(supportMock.getPreparedQuery(ScannerImpl.FTI_CHECK_Q)).andReturn(statementMock);
    expect(supportMock.getSession()).andReturn(sessionMock);
    expect(scanner.getName()).andReturn("Dent, Aurthur Dent");
    expect(bsMock.bind("42", "Dent, Aurthur Dent")).andReturn(bsMock);
    expect(sessionMock.execute(bsMock)).andReturn(rsMock);
    expect(rsMock.getAvailableWithoutFetching()).andReturn(1).anyTimes();
    expect(rsMock.isFullyFetched()).andReturn(true);
    List<Row> rows = new ArrayList<>();
    rows.add(rowMock);
    expect(rsMock.all()).andReturn(rows);
    expect(rowMock.getString(0)).andReturn("PROCESSING");
    expect(rowMock.getString(1)).andReturn("CAFEBABE");
    expect(scanner.heuristicDirty(docMock)).andReturn(false);
    expect(docMock.getHash()).andReturn("DEADBEEF");
    expect(docMock.removeAll("id")).andReturn(null);
    expect(docMock.getIdField()).andReturn("id");
    expect(docMock.put("id", "42")).andReturn(true);
    scanner.sendToNext(docMock);
    replay();
    scanner.docFound(docMock);
  }

  @Test
  public void testDocFoundProcessingStatusButNoHashChange() {
    expect(scanner.isRemembering()).andReturn(true).anyTimes();
    expect(scanner.isHashing()).andReturn(true).anyTimes();
    expect(docMock.getId()).andReturn("42").anyTimes();
    expect(scanner.getIdFunction()).andReturn((foo) -> foo);
    expect(scanner.createBoundStatement(statementMock)).andReturn(bsMock);
    expect(scanner.getCassandra()).andReturn(supportMock).anyTimes();
    expect(supportMock.getPreparedQuery(ScannerImpl.FTI_CHECK_Q)).andReturn(statementMock);
    expect(supportMock.getSession()).andReturn(sessionMock);
    expect(scanner.getName()).andReturn("Dent, Aurthur Dent");
    expect(bsMock.bind("42", "Dent, Aurthur Dent")).andReturn(bsMock);
    expect(sessionMock.execute(bsMock)).andReturn(rsMock);
    expect(rsMock.getAvailableWithoutFetching()).andReturn(1).anyTimes();
    expect(rsMock.isFullyFetched()).andReturn(true);
    List<Row> rows = new ArrayList<>();
    rows.add(rowMock);
    expect(rsMock.all()).andReturn(rows);
    expect(rowMock.getString(0)).andReturn("PROCESSING");
    expect(rowMock.getString(1)).andReturn("CAFEBABE");
    expect(docMock.getIdField()).andReturn("id");
    expect(docMock.put("id", "42")).andReturn(true);
    expect(docMock.removeAll("id")).andReturn(null);
    expect(scanner.heuristicDirty(docMock)).andReturn(false);
    expect(docMock.getHash()).andReturn("CAFEBABE");
    replay();
    scanner.docFound(docMock);
  }

  @Test
  public void testDocFoundProcessingStatusButNoHash() {
    expect(scanner.isRemembering()).andReturn(true).anyTimes();
    expect(scanner.isHashing()).andReturn(true).anyTimes();
    expect(docMock.getId()).andReturn("42").anyTimes();
    expect(scanner.getIdFunction()).andReturn((foo) -> foo);
    expect(scanner.createBoundStatement(statementMock)).andReturn(bsMock);
    expect(scanner.getCassandra()).andReturn(supportMock).anyTimes();
    expect(supportMock.getPreparedQuery(ScannerImpl.FTI_CHECK_Q)).andReturn(statementMock);
    expect(supportMock.getSession()).andReturn(sessionMock);
    expect(scanner.getName()).andReturn("Dent, Aurthur Dent");
    expect(bsMock.bind("42", "Dent, Aurthur Dent")).andReturn(bsMock);
    expect(sessionMock.execute(bsMock)).andReturn(rsMock);
    expect(rsMock.getAvailableWithoutFetching()).andReturn(1).anyTimes();
    expect(rsMock.isFullyFetched()).andReturn(true);
    List<Row> rows = new ArrayList<>();
    rows.add(rowMock);
    expect(rsMock.all()).andReturn(rows);
    expect(rowMock.getString(0)).andReturn("PROCESSING");
    expect(rowMock.getString(1)).andReturn(null);
    expect(scanner.heuristicDirty(docMock)).andReturn(false);
    expect(docMock.getIdField()).andReturn("id");
    expect(docMock.removeAll("id")).andReturn(null);
    expect(docMock.put("id", "42")).andReturn(true);
    scanner.sendToNext(docMock);
    replay();
    scanner.docFound(docMock);
  }

  @Test(expected = RuntimeException.class)
  public void testMultiplePrimaryKeyError1() {
    expect(docMock.getIdField()).andReturn("id");
    expect(docMock.removeAll("id")).andReturn(null);
    expect(docMock.put("id", "42")).andReturn(true);
    expect(scanner.isRemembering()).andReturn(true).anyTimes();
    expect(docMock.getId()).andReturn("42").anyTimes();
    expect(scanner.getIdFunction()).andReturn((foo) -> foo);
    expect(scanner.createBoundStatement(statementMock)).andReturn(bsMock);
    expect(scanner.getCassandra()).andReturn(supportMock).anyTimes();
    expect(supportMock.getPreparedQuery(ScannerImpl.FTI_CHECK_Q)).andReturn(statementMock);
    expect(supportMock.getSession()).andReturn(sessionMock);
    expect(scanner.getName()).andReturn("Dent, Aurthur Dent");
    expect(bsMock.bind("42", "Dent, Aurthur Dent")).andReturn(bsMock);
    expect(sessionMock.execute(bsMock)).andReturn(rsMock);
    expect(rsMock.getAvailableWithoutFetching()).andReturn(2).anyTimes();
    replay();
    scanner.docFound(docMock);
  }


  @Test(expected = RuntimeException.class)
  public void testMultiplePrimaryKeyError2() {
    expect(docMock.getIdField()).andReturn("id");
    expect(docMock.removeAll("id")).andReturn(null);
    expect(docMock.put("id", "42")).andReturn(true);
    expect(scanner.isRemembering()).andReturn(true).anyTimes();
    expect(docMock.getId()).andReturn("42").anyTimes();
    expect(scanner.getIdFunction()).andReturn((foo) -> foo);
    expect(scanner.createBoundStatement(statementMock)).andReturn(bsMock);
    expect(scanner.getCassandra()).andReturn(supportMock).anyTimes();
    expect(supportMock.getPreparedQuery(ScannerImpl.FTI_CHECK_Q)).andReturn(statementMock);
    expect(supportMock.getSession()).andReturn(sessionMock);
    expect(scanner.getName()).andReturn("Dent, Aurthur Dent");
    expect(bsMock.bind("42", "Dent, Aurthur Dent")).andReturn(bsMock);
    expect(sessionMock.execute(bsMock)).andReturn(rsMock);
    expect(rsMock.getAvailableWithoutFetching()).andReturn(1).anyTimes();
    expect(rsMock.isFullyFetched()).andReturn(false);
    replay();
    scanner.docFound(docMock);
  }

  @Test
  public void testSendToNext() {
    expect(scanner.isRemembering()).andReturn(true);
    expect(scanner.getCassandra()).andReturn(supportMock).anyTimes();
    expect(supportMock.getSession()).andReturn(sessionMock);
    expect(supportMock.getPreparedQuery(UPDATE_HASH_U)).andReturn(statementMock);
    expect(docMock.getHash()).andReturn("DEADBEEF");
    expect(docMock.getId()).andReturn("42").anyTimes();
    expect(docMock.getSourceScannerName()).andReturn("Arthur Dent");
    expect(statementMock.bind("DEADBEEF", "42", "Arthur Dent")).andReturn(bsMock);
    expect(sessionMock.execute(bsMock)).andReturn(null);
    scanner.superSendToNext(docMock);
    replay();
    scanner.sendToNext(docMock);
  }
}
