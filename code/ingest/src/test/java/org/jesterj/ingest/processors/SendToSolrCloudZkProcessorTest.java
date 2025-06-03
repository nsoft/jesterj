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
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.easymock.Capture;
import org.jesterj.ingest.model.DocStatusChange;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.Status;
import org.jesterj.ingest.utils.SynchronizedLinkedBimap;
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
import static org.jesterj.ingest.model.Status.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 9/23/16
 */
@SuppressWarnings("DataFlowIssue")
public class SendToSolrCloudZkProcessorTest {
  public static final int BATCH_TIMEOUT = 1000;
  @ObjectUnderTest
  SendToSolrCloudZkProcessor proc;
  @Mock
  private SynchronizedLinkedBimap<Document, SolrInputDocument> batchMock;
  @Mock
  private Document docMock;
  @Mock
  private Logger logMock;
  @Mock
  private SolrInputDocument inputDocMock;
  @Mock
  private SolrInputDocument inputDocMock2;
  @Mock
  private Document docMock2;
  @Mock
  private SolrInputDocument inputDocMock3;
  @Mock
  private Document docMock3;
  @Mock
  private SolrInputDocument inputDocMock4;
  @Mock
  private Document docMock4;
  @Mock
  private SolrInputDocument inputDocMock5;
  @Mock
  private Document docMock5;
  @Mock
  private SolrInputDocument inputDocMock6;
  @Mock
  private Document docMock6;
  @Mock
  private CloudSolrClient solrClientMock;
  @Mock
  private UpdateResponse updateResponseMock;
  @Mock
  private UpdateResponse deleteResponseMock;
  @Mock
  private NamedList<Object> namedListMock;
  @Mock
  private DocumentLoggingContext docContextMock;
  @Mock
  private DocStatusChange statusChangeMock;

  public SendToSolrCloudZkProcessorTest() {
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
    docMock.setStatus(Status.ERROR, "{} could not be sent to solr because of {}", "42", "TEST EXCEPTION");
    docMock.reportDocStatus();
    replay();
    proc.perDocFailLogging(e, docMock);
  }

  @Test
  public void testIdSubstitution() {
    expect(docMock.getOperation()).andReturn(Document.Operation.NEW);
    expect(docMock.keySet()).andReturn(Set.of("id", "myField"));
    expect(docMock.get("id")).andReturn(List.of("forty-two")).anyTimes();
    expect(docMock.get("myField")).andReturn(List.of("this", "that"));
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
    Capture<List<SolrInputDocument>> addCap = newCapture();
    Capture<List<String>> delCap = newCapture();
    expect(solrClientMock.add(capture(addCap))).andReturn(updateResponseMock);
    expect(solrClientMock.deleteById(capture(delCap))).andReturn(deleteResponseMock);

    for (Document document : biMap.keySet()) {
      document.setStatus(INDEXING, "Indexing started for a batch of 3 documents");
      document.reportDocStatus();
    }

    docMock.setStatus(INDEXED, "{} sent to solr successfully", "41");
    docMock.reportDocStatus();
    docMock2.setStatus(INDEXED, "{} deleted from solr successfully", "42");
    docMock2.reportDocStatus();
    docMock3.setStatus(INDEXED, "{} sent to solr successfully", "43");
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

    Map<String, String> params = new HashMap<>();
    params.put("update.chain", "myCustomChain");

    expect(proc.getParams()).andReturn(params).anyTimes();
    expect(proc.getSolrClient()).andReturn(solrClientMock).anyTimes();
    Capture<UpdateRequest> addCap = newCapture();
    Capture<List<String>> delCap = newCapture();
    //noinspection ConstantConditions
    expect(solrClientMock.request(capture(addCap), eq(null))).andReturn(namedListMock);
    expect(solrClientMock.deleteById(capture(delCap))).andReturn(deleteResponseMock);

    for (Document document : biMap.keySet()) {
      document.setStatus(INDEXING, "Indexing started for a batch of 3 documents");
      document.reportDocStatus();
    }

    docMock.setStatus(INDEXED, "{} sent to solr successfully", "41");
    docMock.reportDocStatus();
    docMock2.setStatus(INDEXED, "{} deleted from solr successfully", "42");
    docMock2.reportDocStatus();
    docMock3.setStatus(INDEXED, "{} sent to solr successfully", "43");
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

  @Test
  public void testBuilder() throws SolrServerException, IOException, InterruptedException {
    boolean[] transfomred = {false};
    SendToSolrCloudZkProcessor.Builder builder = new SendToSolrCloudZkProcessor.Builder()
        .withZookeeper("localhost:2181")
        .placingTextContentIn("content")
        .named("test_zk_builder")
        .usingCollection("fooCollection")
        .withDocFieldsIn(".fields")
        .transformIdsWith((s -> {
          transfomred[0] = true;
          return s;
        }))
        .withRequestParameters(Map.of("foo", "bar", "baz", "bam"))
        .zkChroot("/chroot2here")
        .sendingBatchesOf(4)
        .sendingPartialBatchesAfterMs(BATCH_TIMEOUT);
    SendToSolrCloudZkProcessor proc = builder.build();
    SolrClient solrClient = proc.getSolrClient();
    assertTrue(solrClient instanceof CloudSolrClient);
    CloudSolrClient csc = (CloudSolrClient) solrClient;
    assertEquals("fooCollection", csc.getDefaultCollection());
    assertEquals(SolrRequest.SolrClientContext.CLIENT, csc.getContext());

    // proved that we can create a solr client, but now we want to mock it.
    proc.setSolrClient(solrClientMock);

    // expectations for mock
    setupDocsForBuilder(docMock,  1);
    setupDocsForBuilder(docMock2, 2);
    setupDocsForBuilder(docMock3, 3);
    setupDocsForBuilder(docMock4, 4);
    setupDocsForBuilder(docMock5, 1);

    // initial batch failure
    expect(solrClientMock.request(isA(UpdateRequest.class), isNull())).andThrow(new SolrServerException("fail"));

    // individual fallbacks
    indexMock(docMock, 4, "idTest1", false, true);
    indexMock(docMock2, 4, "idTest2", false, true);
    indexMock(docMock3, 4, "idTest3", false, true);
    indexMock(docMock4, 4, "idTest4", true, true);
    indexMock(docMock5, 1, "idTest1", false, false);

    expect(solrClientMock.request(isA(UpdateRequest.class), isNull())).andReturn(namedListMock);

    replay();
    proc.processDocument(docMock);
    proc.processDocument(docMock2);
    proc.processDocument(docMock3);
    proc.processDocument(docMock4);

    assertEquals(4, proc.getDocsReceived());
    assertEquals(0, proc.getDocsAttempted());
    assertEquals(0, proc.getDocsSucceeded()); // verify batching

    proc.processDocument(docMock5); // simulates re-send of doc 1

    assertEquals(5, proc.getDocsReceived());
    assertEquals(4, proc.getDocsAttempted());
    assertEquals(3, proc.getDocsSucceeded()); // should fail and fallback to individual sends

    assertTrue(transfomred[0]);

    Thread.sleep(2500); // partial batch send

    assertEquals(5, proc.getDocsReceived());
    assertEquals(5, proc.getDocsAttempted());
    assertEquals(4, proc.getDocsSucceeded()); // should fail and fallback to individual sends

  }

  private void indexMock(Document documentMock, int batchSize, String id, boolean fail, boolean fallbackExpected) throws SolrServerException, IOException {
    // when batch is attempted
    documentMock.setStatus(INDEXING, "Indexing started for a batch of " + batchSize + " documents");
    documentMock.reportDocStatus();
    if (fallbackExpected) {
      expect(documentMock.getStatusChange()).andReturn(statusChangeMock).anyTimes();
      expect(statusChangeMock.getStatus()).andReturn(INDEXING).times(2); // once per changing destination
      documentMock.setStatus(Status.INDEXING, "{} is being sent to solr", id);
      documentMock.reportDocStatus();
      if (fail) {
        expect(solrClientMock.add(isA(SolrInputDocument.class))).andThrow(new SolrServerException("doc 4 bad doc (test)"));
        documentMock.setStatus(ERROR, "{} could not be sent to solr because of {}", id, "doc 4 bad doc (test)");
      } else {
        expect(solrClientMock.add(isA(SolrInputDocument.class))).andReturn(updateResponseMock);
        documentMock.setStatus(INDEXED, "{} sent to solr successfully", id);
      }
      documentMock.reportDocStatus();
    }  else { // else it's sent in a successful batch, no call to add
      documentMock.setStatus(INDEXED, "{} sent to solr successfully", id);
      documentMock.reportDocStatus();
    }


  }

  private void setupDocsForBuilder(Document documentMock, int i) {
    expect(documentMock.getOperation()).andReturn(Document.Operation.NEW).anyTimes();
    expect(documentMock.keySet()).andReturn(Set.of("field1", "field2")).anyTimes();
    List<String> value1 = List.of("value1");
    expect(documentMock.get("field1")).andReturn(value1).anyTimes();
    List<String> value2 = List.of("value2", "value2b");
    expect(documentMock.get("field2")).andReturn(value2).anyTimes();

    byte[] t = {};
    expect(documentMock.getRawData()).andReturn(t).anyTimes();

    expect(documentMock.getIdField()).andReturn("id").anyTimes();
    expect(documentMock.getFirstValue("id")).andReturn("idTest" + i).anyTimes();
    expect(documentMock.getId()).andReturn("idTest" + i).anyTimes();

    // this message verifies the setting of .sendingPartialBatchesAfter
    documentMock.setStatus(Status.BATCHED, "{} queued in position {} for sending to solr. Will be sent within " +
        "{} milliseconds.", "idTest" + i, i, BATCH_TIMEOUT);

    documentMock.reportDocStatus();

    // generic logging context stuff
    expect(documentMock.getHash()).andReturn(null).anyTimes();
    expect(documentMock.getSourceScannerName()).andReturn(null).anyTimes();
    expect(documentMock.getParentId()).andReturn(null).anyTimes();
    expect(documentMock.getOrignalParentId()).andReturn(null).anyTimes();
    expect(documentMock.listChangingDestinations()).andReturn(List.of("foo", "bar")).anyTimes();

  }

  private SynchronizedLinkedBimap<Document, SolrInputDocument> expect3Docs() {
    SynchronizedLinkedBimap<Document, SolrInputDocument> biMap = new SynchronizedLinkedBimap<>();
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
