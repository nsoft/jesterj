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

import static com.copyright.easiertest.EasierMocks.prepareMocks;
import static com.copyright.easiertest.EasierMocks.replay;
import static com.copyright.easiertest.EasierMocks.reset;
import static com.copyright.easiertest.EasierMocks.verify;
import static junit.framework.TestCase.assertTrue;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.newCapture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.copyright.easiertest.Mock;
import com.copyright.easiertest.ObjectUnderTest;
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
import org.jesterj.ingest.model.impl.DocumentImpl;
import org.jesterj.ingest.utils.SynchronizedLinkedBimap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 9/23/16
 */
public class SendToSolrCloudHttpUrlProcessorTest {
  @ObjectUnderTest
  SendToSolrCloudHttpUrlProcessor proc;
  @Mock private SynchronizedLinkedBimap<Document, SolrInputDocument> batchMock;
  @Mock private DocumentImpl docMock;
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

  public SendToSolrCloudHttpUrlProcessorTest() {
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
    proc.perDocFailLogging(e, docMock);

    replay();
    proc.entireBatchFailure(batchMock, e);
  }

  @Test
  public void testPerDocumentFailLogging() {
    RuntimeException e = new RuntimeException("TEST EXCEPTION");
    expect(proc.log()).andReturn(logMock).anyTimes();
    expect(docMock.getId()).andReturn("42");
    expect(proc.getName()).andReturn("testProc");
    docMock.setStatus(Status.ERROR,  "{} could not be sent by {} because of {}", "42", "testProc", "TEST EXCEPTION");
    docMock.reportDocStatus();
    replay();
    proc.perDocFailLogging(e, docMock);
  }

  @Test
  public void testIdSubstitution() {
    expect(docMock.getOperation()).andReturn(Document.Operation.NEW);
    expect(docMock.keySet()).andReturn(Set.of("id","myField"));
    expect(docMock.get("id")).andReturn(List.of("forty-two")).anyTimes();
    expect(docMock.get("myField")).andReturn(List.of("this","that"));
    expect(docMock.getRawData()).andReturn(null);
    expect(proc.getFieldsField()).andReturn(null);
    expect(proc.getIdTransformer()).andReturn(v -> 42);
    expect(docMock.getIdField()).andReturn("id");
    expect(docMock.getFirstValue("id")).andReturn("42");

    replay();
    SolrInputDocument solrDoc = proc.convertDoc(docMock);
    assertEquals(42, solrDoc.get("id").getValue());
    List<Object> myField = (List<Object>) solrDoc.get("myField").getValues();
    assertEquals("this", myField.get(0));
    assertEquals("that", myField.get(1));
  }
  @Test
  public void testPerBatchOperation() throws IOException, SolrServerException {
    SynchronizedLinkedBimap<Document, SolrInputDocument> biMap = expect3Docs();
    expect(proc.getParams()).andReturn(null).anyTimes();
    expect(proc.getSolrClient()).andReturn(solrClientMock).anyTimes();
    expect(proc.getName()).andReturn("test_per_batch").anyTimes();
    Capture<List<SolrInputDocument>> addCap = newCapture();
    Capture<List<String>> delCap = newCapture();
    expect(solrClientMock.add(capture(addCap))).andReturn(updateResponseMock);
    expect(solrClientMock.deleteById(capture(delCap))).andReturn(deleteResponseMock);

    for (Document document : biMap.keySet()) {
      document.setStatus(Status.INDEXING, "Indexing started for a batch of 3 documents");
      document.reportDocStatus();
    }

    docMock.setStatus(Status.INDEXED, "{} sent by {} successfully", "41", "test_per_batch");
    docMock.reportDocStatus();
    docMock2.setStatus(Status.INDEXED, "{} deleted by {} successfully", "42", "test_per_batch");
    docMock2.reportDocStatus();
    docMock3.setStatus(Status.INDEXED, "{} sent by {} successfully", "43", "test_per_batch");
    docMock3.reportDocStatus();

    replay();
    proc.batchOperation(biMap);
    assertEquals("42", delCap.getValue().get(0));
    assertTrue(addCap.getValue().contains(inputDocMock));
    assertFalse(addCap.getValue().contains(inputDocMock2));
    assertTrue(addCap.getValue().contains(inputDocMock3));
  }

  @Test
  public void testPerBatchOperationWithChain() throws IOException, SolrServerException {
    SynchronizedLinkedBimap<Document, SolrInputDocument> biMap = expect3Docs();

    Map<String,String> params = new HashMap<>();
    params.put("update.chain", "myCustomChain");

    expect(proc.getParams()).andReturn(params).anyTimes();
    expect(proc.getSolrClient()).andReturn(solrClientMock).anyTimes();
    expect(proc.getName()).andReturn("test_per_batch_chain").anyTimes();
    Capture<UpdateRequest> addCap = newCapture();
    Capture<List<String>> delCap = newCapture();
    //noinspection ConstantConditions
    expect(solrClientMock.request(capture(addCap), eq(null))).andReturn(namedListMock);
    expect(solrClientMock.deleteById(capture(delCap))).andReturn(deleteResponseMock);

    for (Document document : biMap.keySet()) {
      document.setStatus(Status.INDEXING, "Indexing started for a batch of 3 documents");
      document.reportDocStatus();
    }

    docMock.setStatus(Status.INDEXED, "{} sent by {} successfully", "41", "test_per_batch_chain");
    docMock.reportDocStatus();
    docMock2.setStatus(Status.INDEXED, "{} deleted by {} successfully", "42",  "test_per_batch_chain");
    docMock2.reportDocStatus();
    docMock3.setStatus(Status.INDEXED, "{} sent by {} successfully", "43", "test_per_batch_chain");
    docMock3.reportDocStatus();

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

  private SynchronizedLinkedBimap<Document, SolrInputDocument> expect3Docs() {
    SynchronizedLinkedBimap<Document, SolrInputDocument> biMap =
        new SynchronizedLinkedBimap<>();
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
