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

package org.jesterj.ingest.processors;

import com.copyright.easiertest.Mock;
import com.copyright.easiertest.ObjectUnderTest;
import org.apache.cassandra.utils.ConcurrentBiMap;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.easymock.Capture;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.Status;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static com.copyright.easiertest.EasierMocks.replay;
import static com.copyright.easiertest.EasierMocks.reset;
import static com.copyright.easiertest.EasierMocks.verify;
import static com.copyright.easiertest.EasierMocks.*;
import static junit.framework.TestCase.assertTrue;
import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 9/23/16
 */
@SuppressWarnings("DataFlowIssue")
public class SendToSolrCloudProcessorTest {
  @ObjectUnderTest
  SendToSolrCloudProcessor proc;
  @Mock private ConcurrentBiMap<Document, SolrInputDocument> batchMock;
  @Mock private Document docMock;
  @Mock private Logger logMock;
  @Mock private SolrInputDocument inputDocMock;
  @Mock private SolrInputDocument inputDocMock2;
  @Mock private Document docMock2;
  @Mock private SolrInputDocument inputDocMock3;
  @Mock private Document docMock3;
  @Mock private CloudSolrClient solrClientMock;
  @Mock private UpdateResponse updateResponseMock;
  @Mock private UpdateResponse deleteResponseMock;
  @Mock private NamedList<Object> namedListMock;
  @Mock private DocumentLoggingContext docContextMock;

  public SendToSolrCloudProcessorTest() {
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
  public void testPerDocumentFailure() {
    Set<Document> documents = new HashSet<>();
    documents.add(docMock);
    expect(batchMock.keySet()).andReturn(documents);
    RuntimeException e = new RuntimeException("TEST EXCEPTION");
    expect(proc.createDocContext(docMock)).andReturn(docContextMock);
    docContextMock.run(isA(Runnable.class));
    replay();
    proc.perDocumentFailure(batchMock, e);
  }

  @Test
  public void testPerDocumentFailLogging() {
    RuntimeException e = new RuntimeException("TEST EXCEPTION");
    expect(proc.log()).andReturn(logMock).anyTimes();
    expect(docMock.getId()).andReturn("42");
    logMock.info(Status.ERROR.getMarker(), "{} could not be sent to solr because of {}", "42", "TEST EXCEPTION");
    logMock.error("Error communicating with solr!", e);
    replay();
    proc.perDocFailLogging(e, docMock);
  }

  @Test
  public void testPerBatchOperation() throws IOException, SolrServerException {
    ConcurrentBiMap<Document, SolrInputDocument> biMap = expect3Docs();
    expect(proc.getParams()).andReturn(null).anyTimes();
    expect(proc.getSolrClient()).andReturn(solrClientMock).anyTimes();
    Capture<List<SolrInputDocument>> addCap = newCapture();
    Capture<List<String>> delCap = newCapture();
    expect(solrClientMock.add(capture(addCap))).andReturn(updateResponseMock);
    expect(solrClientMock.deleteById(capture(delCap))).andReturn(deleteResponseMock);
    expectRunContexts();
    replay();
    proc.batchOperation(biMap);
    assertEquals("42", delCap.getValue().get(0));
    assertTrue(addCap.getValue().contains(inputDocMock));
    assertFalse(addCap.getValue().contains(inputDocMock2));
    assertTrue(addCap.getValue().contains(inputDocMock3));
  }

  private void expectRunContexts() {
    expect(proc.createDocContext(docMock)).andReturn(docContextMock);
    docContextMock.run(isA(Runnable.class));
    expect(proc.createDocContext(docMock2)).andReturn(docContextMock);
    docContextMock.run(isA(Runnable.class));
    expect(proc.createDocContext(docMock3)).andReturn(docContextMock);
    docContextMock.run(isA(Runnable.class));
  }

  @Test
  public void testPerBatchOperationWithChain() throws IOException, SolrServerException {
    ConcurrentBiMap<Document, SolrInputDocument> biMap = expect3Docs();

    Map<String,String> params = new HashMap<>();
    params.put("update.chain", "myCustomChain");

    expect(proc.getParams()).andReturn(params).anyTimes();
    expect(proc.getSolrClient()).andReturn(solrClientMock).anyTimes();
    Capture<UpdateRequest> addCap = newCapture();
    Capture<List<String>> delCap = newCapture();
    //noinspection ConstantConditions
    expect(solrClientMock.request(capture(addCap), eq(null))).andReturn(namedListMock);
    expect(solrClientMock.deleteById(capture(delCap))).andReturn(deleteResponseMock);

    expectRunContexts();
    replay();
    proc.batchOperation(biMap);

    assertEquals("42", delCap.getValue().get(0));
    UpdateRequest updateRequest = addCap.getValue();
    List<SolrInputDocument> documents = updateRequest.getDocuments();
    assertTrue(documents.contains(inputDocMock));
    assertFalse(documents.contains(inputDocMock2));
    assertTrue(documents.contains(inputDocMock3));
    ModifiableSolrParams reqParams = updateRequest.getParams();
    assertEquals("myCustomChain", reqParams.get("update.chain"));
  }

  private ConcurrentBiMap<Document, SolrInputDocument> expect3Docs() {
    ConcurrentBiMap<Document, SolrInputDocument> biMap = new ConcurrentBiMap<>();
    biMap.put(docMock, inputDocMock);
    biMap.put(docMock2, inputDocMock2);
    biMap.put(docMock3, inputDocMock3);
    expect(docMock.getOperation()).andReturn(Document.Operation.NEW).anyTimes();
    expect(docMock2.getOperation()).andReturn(Document.Operation.DELETE).anyTimes();
    expect(docMock3.getOperation()).andReturn(Document.Operation.UPDATE).anyTimes();
    expect(docMock.getId()).andReturn("41").anyTimes();
    expect(docMock2.getId()).andReturn("42").anyTimes();
    expect(docMock3.getId()).andReturn("43").anyTimes();
    return biMap;
  }
}
