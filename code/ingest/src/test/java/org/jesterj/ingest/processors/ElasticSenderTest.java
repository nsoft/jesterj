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
/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 4/7/16
 */

import com.copyright.easiertest.BeanTester;
import com.copyright.easiertest.Mock;
import com.copyright.easiertest.ObjectUnderTest;
import org.apache.cassandra.utils.ConcurrentBiMap;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionWriteResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.rest.RestStatus;
import org.jesterj.ingest.model.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static com.copyright.easiertest.EasierMocks.prepareMocks;
import static com.copyright.easiertest.EasierMocks.replay;
import static com.copyright.easiertest.EasierMocks.reset;
import static com.copyright.easiertest.EasierMocks.verify;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;

public class ElasticSenderTest {
  @ObjectUnderTest private ElasticSender obj;
  @Mock private Client mockClient;
  @Mock private BulkRequestBuilder mockBulkReq;
  @Mock private ConcurrentBiMap<Document, ActionRequest> mockBatch;
  @Mock private UpdateRequest mockUpdate;
  @Mock private DeleteRequest mockDelete;
  @Mock private IndexRequest mockIndex;
  @Mock private BulkResponse mockBulkResp;
  @Mock private Document docMock;
  @Mock private ActionFuture<UpdateResponse> futureMockUpdate;
  @Mock private ActionFuture<IndexResponse> futureMockIndex;
  @Mock private ActionFuture<DeleteResponse> futureMockDelete;
  @Mock private IndexResponse mockIndexResponse;
  @Mock private ActionWriteResponse.ShardInfo shardInfoMock;


  public ElasticSenderTest() {
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
  public void testBatchOperation() throws Exception {
    expect(obj.getClient()).andReturn(mockClient);
    expect(mockClient.prepareBulk()).andReturn(mockBulkReq);
    ArrayList<ActionRequest> values = new ArrayList<>();
    values.add(mockUpdate);
    values.add(mockDelete);
    values.add(mockIndex);
    expect(mockBatch.values()).andReturn(values);
    expect(mockBulkReq.add(mockUpdate)).andReturn(mockBulkReq);
    expect(mockBulkReq.add(mockDelete)).andReturn(mockBulkReq);
    expect(mockBulkReq.add(mockIndex)).andReturn(mockBulkReq);
    expect(mockBulkReq.get()).andReturn(mockBulkResp);
    expect(mockBulkResp.hasFailures()).andReturn(false);
    HashSet<Document> docs = new HashSet<>();
    docs.add(docMock);
    expect(mockBatch.keySet()).andReturn(docs);
    expect(docMock.getId()).andReturn("foo");
    replay();
    obj.batchOperation(mockBatch);
  }

  @Test(expected = ElasticSender.ESBulkFail.class)
  public void testBatchOperationHasFailures() throws Exception {
    expect(obj.getClient()).andReturn(mockClient);
    expect(mockClient.prepareBulk()).andReturn(mockBulkReq);
    ArrayList<ActionRequest> values = new ArrayList<>();
    values.add(mockUpdate);
    values.add(mockDelete);
    values.add(mockIndex);
    expect(mockBatch.values()).andReturn(values);
    expect(mockBulkReq.add(mockUpdate)).andReturn(mockBulkReq);
    expect(mockBulkReq.add(mockDelete)).andReturn(mockBulkReq);
    expect(mockBulkReq.add(mockIndex)).andReturn(mockBulkReq);
    expect(mockBulkReq.get()).andReturn(mockBulkResp);
    expect(mockBulkResp.hasFailures()).andReturn(true);
    replay();
    obj.batchOperation(mockBatch);
  }

  @Test(expected = IllegalStateException.class)
  public void testBatchOperationWrongObj() throws Exception {
    expect(obj.getClient()).andReturn(mockClient);
    expect(mockClient.prepareBulk()).andReturn(mockBulkReq);
    ArrayList<ActionRequest> values = new ArrayList<>();
    values.add(new ActionRequest() {
      @Override
      public ActionRequestValidationException validate() {
        return null;
      }
    });
    values.add(mockDelete);
    values.add(mockIndex);
    expect(mockBatch.values()).andReturn(values);
    replay();
    obj.batchOperation(mockBatch);
  }


  @Test
  public void testConvertDeleteDocument() {
    expect(docMock.getOperation()).andReturn(Document.Operation.DELETE);
    expect(obj.getIndexName()).andReturn("foo");
    expect(obj.getObjectType()).andReturn("bar");
    expect(docMock.getId()).andReturn("foobar");
    replay();
    obj.convertDoc(docMock);
  }

  @Test
  public void testConvertUpdateDocument() {
    expect(docMock.getOperation()).andReturn(Document.Operation.UPDATE);
    expect(obj.getIndexName()).andReturn("foo");
    expect(obj.getObjectType()).andReturn("bar");
    expect(docMock.getId()).andReturn("foobar");
    expect(docMock.asMap()).andReturn(new HashMap<>());
    replay();
    obj.convertDoc(docMock);
  }

  @Test
  public void testConvertNewDocument() {
    expect(docMock.getOperation()).andReturn(Document.Operation.NEW);
    expect(obj.getIndexName()).andReturn("foo");
    expect(obj.getObjectType()).andReturn("bar");
    expect(docMock.getId()).andReturn("foobar");
    expect(docMock.asMap()).andReturn(new HashMap<>());
    replay();
    obj.convertDoc(docMock);
  }

  @Test
  public void testRetryIndividualDocs() {
    ElasticSender.ESBulkFail e = new ElasticSender.ESBulkFail();
    Collection<ActionRequest> actionRequests = new ArrayList<>();
    actionRequests.add(mockUpdate);
    actionRequests.add(mockIndex);
    actionRequests.add(mockDelete);
    expect(mockBatch.values()).andReturn(actionRequests);
    expect(obj.getClient()).andReturn(mockClient).anyTimes();
    expect(mockClient.index(mockIndex)).andReturn(futureMockIndex);
    expect(mockClient.update(mockUpdate)).andReturn(futureMockUpdate);
    expect(mockClient.delete(mockDelete)).andReturn(futureMockDelete);
    //noinspection unchecked
    obj.handleRetryResult(eq(e), isA(HashMap.class), eq(futureMockDelete), eq(mockBatch));
    //noinspection unchecked
    obj.handleRetryResult(eq(e), isA(HashMap.class), eq(futureMockUpdate), eq(mockBatch));
    //noinspection unchecked
    obj.handleRetryResult(eq(e), isA(HashMap.class), eq(futureMockIndex), eq(mockBatch));
    replay();
    obj.individualFallbackOperation(mockBatch, e);
  }

  @Test
  public void testHandleRetryResult() {
    ElasticSender.ESBulkFail e = new ElasticSender.ESBulkFail();
    Map<ActionFuture, ActionRequest> actionFutureActionRequestMap = new HashMap<>();
    actionFutureActionRequestMap.put(futureMockIndex, mockIndex);
    expect(futureMockIndex.actionGet()).andReturn(mockIndexResponse);
    Map<ActionRequest, Document> actionRequestDocumentMap = new HashMap<>();
    actionRequestDocumentMap.put(mockIndex, docMock);
    expect(mockBatch.inverse()).andReturn(actionRequestDocumentMap);
    expect(docMock.getId()).andReturn("foo");
    obj.checkResponse(docMock, mockIndexResponse);
    replay();
    obj.handleRetryResult(e, actionFutureActionRequestMap, futureMockIndex, mockBatch);
  }

  @Test
  public void testHandleRetryResultException() {
    ElasticSender.ESBulkFail e = new ElasticSender.ESBulkFail();
    Map<ActionFuture, ActionRequest> actionFutureActionRequestMap = new HashMap<>();
    actionFutureActionRequestMap.put(futureMockIndex, mockIndex);
    expect(futureMockIndex.actionGet()).andThrow(new RuntimeException());
    Map<ActionRequest, Document> actionRequestDocumentMap = new HashMap<>();
    actionRequestDocumentMap.put(mockIndex, docMock);
    expect(mockBatch.inverse()).andReturn(actionRequestDocumentMap);
    expect(docMock.getId()).andReturn("foo");
    replay();
    obj.handleRetryResult(e, actionFutureActionRequestMap, futureMockIndex, mockBatch);
  }

  @Test
  public void testCheckResponseOk() {
    expect(docMock.getId()).andReturn("foo").anyTimes();
    expect(mockIndexResponse.getShardInfo()).andReturn(shardInfoMock);
    expect(shardInfoMock.status()).andReturn(RestStatus.OK);
    replay();
    obj.checkResponse(docMock, mockIndexResponse);
  }

  @Test
  public void testCheckResponse500oneOf2() {
    expect(docMock.getId()).andReturn("foo").anyTimes();
    expect(mockIndexResponse.getShardInfo()).andReturn(shardInfoMock);
    expect(shardInfoMock.status()).andReturn(RestStatus.INTERNAL_SERVER_ERROR);
    expect(shardInfoMock.getSuccessful()).andReturn(1);
    expect(shardInfoMock.getFailed()).andReturn(1);
    replay();
    obj.checkResponse(docMock, mockIndexResponse);
  }

  @Test
  public void testCheckResponse400TwoOf2() {
    expect(docMock.getId()).andReturn("foo").anyTimes();
    expect(mockIndexResponse.getShardInfo()).andReturn(shardInfoMock);
    expect(shardInfoMock.status()).andReturn(RestStatus.BAD_REQUEST);
    expect(shardInfoMock.getSuccessful()).andReturn(0);
    replay();
    obj.checkResponse(docMock, mockIndexResponse);
  }


  @Test
  public void testSimpleProperties() {
    replay();
    BeanTester tester = new BeanTester();
    tester.testBean(new ElasticSender() {

      @Override
      protected void perDocumentFailure(ConcurrentBiMap<Document, ActionRequest> oldBatch, Exception e) {
      }

      @Override
      protected boolean exceptionIndicatesDocumentIssue(Exception e) {
        return false;
      }
    });
  }

}
