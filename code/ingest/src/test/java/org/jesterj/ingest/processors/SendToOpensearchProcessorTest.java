package org.jesterj.ingest.processors;

import com.copyright.easiertest.Mock;
import com.copyright.easiertest.ObjectUnderTest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jesterj.ingest.model.DocStatusChange;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.Status;
import org.jesterj.ingest.utils.SynchronizedLinkedBimap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.copyright.easiertest.EasierMocks.*;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.jesterj.ingest.model.Status.*;
import static org.junit.Assert.*;

public class SendToOpensearchProcessorTest {

  public static final int BATCH_TIMEOUT = 1000;

  // mangled from https://docs.opensearch.org/latest/api-reference/document-apis/bulk/
  public static final String RESPONSE = "{\n" +
      "  \"took\": 11,\n" +
      "  \"errors\": true,\n" +
      "  \"items\": [\n" +
      "    {\n" +
      "      \"index\": {\n" +
      "        \"_index\": \"movies\",\n" +
      "        \"_id\": \"idTest1\",\n" +
      "        \"_version\": 1,\n" +
      "        \"result\": \"created\",\n" +
      "        \"_shards\": {\n" +
      "          \"total\": 2,\n" +
      "          \"successful\": 1,\n" +
      "          \"failed\": 0\n" +
      "        },\n" +
      "        \"_seq_no\": 1,\n" +
      "        \"_primary_term\": 1,\n" +
      "        \"status\": 201\n" +
      "      }\n" +
      "    },\n" +
      "    {\n" +
      "      \"index\": {\n" +
      "        \"_index\": \"movies\",\n" +
      "        \"_id\": \"idTest2\",\n" +
      "        \"_version\": 1,\n" +
      "        \"result\": \"created\",\n" +
      "        \"_shards\": {\n" +
      "          \"total\": 2,\n" +
      "          \"successful\": 1,\n" +
      "          \"failed\": 0\n" +
      "        },\n" +
      "        \"_seq_no\": 1,\n" +
      "        \"_primary_term\": 1,\n" +
      "        \"status\": 201\n" +
      "      }\n" +
      "    },\n" +
      "    {\n" +
      "      \"index\": {\n" +
      "        \"_index\": \"movies\",\n" +
      "        \"_id\": \"idTest3\",\n" +
      "        \"_version\": 1,\n" +
      "        \"result\": \"created\",\n" +
      "        \"_shards\": {\n" +
      "          \"total\": 2,\n" +
      "          \"successful\": 1,\n" +
      "          \"failed\": 0\n" +
      "        },\n" +
      "        \"_seq_no\": 1,\n" +
      "        \"_primary_term\": 1,\n" +
      "        \"status\": 201\n" +
      "      }\n" +
      "    },\n" +
      "    {\n" +
      "      \"index\": {\n" +
      "        \"_index\": \"movies\",\n" +
      "        \"_id\": \"idTest4\",\n" +
      "        \"_version\": 1,\n" +
      "        \"result\": \"created\",\n" +
      "        \"status\": 400,\n" +
      "        \"error\": {\n" +
      "          \"type\": \"Bad(ish) Request\",\n" +
      "          \"reason\": \"Invalid Date Format or whatever\",\n" +
      "          \"index\": \"movies\",\n" +
      "          \"shard\": \"0\",\n" +
      "          \"index_uuid\": \"yhizhusbSWmP0G7OJnmcLg\"\n" +
      "        }\n" +
      "      }\n" +
      "    }\n" +
      "  ]\n" +
      "}\n";
  public static final String RESPONSE2 = "{\n" +
      "  \"took\": 11,\n" +
      "  \"errors\": false,\n" +
      "  \"items\": [\n" +
      "    {\n" +
      "      \"index\": {\n" +
      "        \"_index\": \"movies\",\n" +
      "        \"_id\": \"idTest5\",\n" +
      "        \"_version\": 1,\n" +
      "        \"result\": \"created\",\n" +
      "        \"_shards\": {\n" +
      "          \"total\": 2,\n" +
      "          \"successful\": 1,\n" +
      "          \"failed\": 0\n" +
      "        },\n" +
      "        \"_seq_no\": 1,\n" +
      "        \"_primary_term\": 1,\n" +
      "        \"status\": 201\n" +
      "      }\n" +
      "    }"+
      "  ]\n" +
      "}\n";

  @ObjectUnderTest
  SendToOpenSearchProcessor processor;

  @Mock
  HttpClient mockHttpClient;
  @Mock
  Document docMock1;
  @Mock
  Document docMock2;
  @Mock
  Document docMock3;
  @Mock
  Document docMock4;
  @Mock
  Document docMock5;
  @Mock
  DocStatusChange statusChangeMock;
  @Mock
  private CompletableFuture<HttpResponse<String>> futureMock;
  @Mock
  private HttpResponse<String> httpResponseMock;


  public SendToOpensearchProcessorTest() {
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
  public void testResponseAsMapValidJson() {
    expect(processor.getMapper()).andReturn(new ObjectMapper());
    expect(httpResponseMock.body()).andReturn("{\"foo\":\"bar\"}");
    replay();
    Map<String, Object> stringObjectMap = processor.responseAsMap(httpResponseMock);
    assertEquals("bar", stringObjectMap.get("foo"));
  }

  @Test
  public void testResponseAsMapGotArray() {
    expect(processor.getMapper()).andReturn(new ObjectMapper());
    expect(httpResponseMock.body()).andReturn("[\"foo\",\"bar\"]");
    replay();
    Map<String, Object> stringObjectMap = processor.responseAsMap(httpResponseMock);
    assertNull(stringObjectMap);
  }

  @Test
  public void testResponseAsMapGotGarbage() {
    expect(processor.getMapper()).andReturn(new ObjectMapper());
    expect(httpResponseMock.body()).andReturn("[abcdefg;':");
    replay();
    Map<String, Object> stringObjectMap = processor.responseAsMap(httpResponseMock);
    assertNull(stringObjectMap);
  }

  @Test
  public void testFallbackHttpNotOk() {
    OpenSearchBatchFailureException testFallbackHttpNotOkException =
        new OpenSearchBatchFailureException("testFallbackHttpNotOk Exception", httpResponseMock);
    SynchronizedLinkedBimap<Document,String> batch = new SynchronizedLinkedBimap<>();
    batch.put(docMock1, "{\"foo\":\"bar\"");
    processor.perDocFailLogging(testFallbackHttpNotOkException,docMock1);
    replay();
    processor.fallbackHttpNotOk(batch,400, testFallbackHttpNotOkException);
  }

  @Test
  public void testHandleMissingJsonBody() {
    SynchronizedLinkedBimap<Document,String> batch = new SynchronizedLinkedBimap<>();
    batch.put(docMock1, "{\"foo\":\"bar\"");

    docMock1.setStatus(DEAD,"Unexpected empty response body from Opensearch!");
    docMock1.reportDocStatus();
    replay();
    processor.handleMissingResponseBody(batch);
  }

  @Test
  public void testExtractCreates() throws JsonProcessingException {
    String createResponses = "{\n" +
        "  \"took\": 11,\n" +
        "  \"errors\": true,\n" +
        "  \"items\": [\n" +
        "    {\n" +
        "      \"create\": {\n" +
        "        \"_index\": \"movies\",\n" +
        "        \"_id\": \"tt1979320\",\n" +
        "        \"_version\": 1,\n" +
        // <snip: irrelevant stuff>
        "        \"status\": 201\n" +
        "      }\n" +
        "    },\n" +
        "    {\n" +
        "      \"create\": {\n" +
        "        \"_index\": \"movies\",\n" +
        "        \"_id\": \"tt1979321\",\n" +
        // <snip: irrelevant stuff>
        "        \"status\": 201\n" +
        "      }\n" +
        "    }\n" +
        "  ]\n" +
        "}";
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> stringObjectMap = mapper.readValue(createResponses, new TypeReference<>() {
    });
    replay();
    List<Map<String, Object>> list = processor.extractItemList(stringObjectMap);
    assertNotNull(list);
    assertEquals(2,list.size());
    assertEquals("tt1979320",list.get(0).get("_id"));
    assertEquals("tt1979321",list.get(1).get("_id"));
  }

  @Test
  public void testExtractUpdates() throws JsonProcessingException {
    String createResponses = "{\n" +
        "  \"took\": 11,\n" +
        "  \"errors\": true,\n" +
        "  \"items\": [\n" +
        "    {\n" +
        "      \"index\": {\n" +
        "        \"_index\": \"movies\",\n" +
        "        \"_id\": \"tt1979320\",\n" +
        "        \"_version\": 1,\n" +
        // <snip: irrelevant stuff>
        "        \"status\": 201\n" +
        "      }\n" +
        "    },\n" +
        "    {\n" +
        "      \"index\": {\n" +
        "        \"_index\": \"movies\",\n" +
        "        \"_id\": \"tt1979321\",\n" +
        // <snip: irrelevant stuff>
        "        \"status\": 201\n" +
        "      }\n" +
        "    }\n" +
        "  ]\n" +
        "}";
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> stringObjectMap = mapper.readValue(createResponses, new TypeReference<>() {
    });
    replay();
    List<Map<String, Object>> list = processor.extractItemList(stringObjectMap);
    assertNotNull(list);
    assertEquals(2,list.size());
    assertEquals("tt1979320",list.get(0).get("_id"));
    assertEquals("tt1979321",list.get(1).get("_id"));
  }

  @Test (expected = IllegalArgumentException.class)
  public void testUnknownsIgnored() throws JsonProcessingException {
    String createResponses = "{\n" +
        "  \"took\": 11,\n" +
        "  \"errors\": true,\n" +
        "  \"items\": [\n" +
        "    {\n" +
        "      \"fibbledyfoo\": {\n" +
        "        \"_index\": \"movies\",\n" +
        "        \"_id\": \"tt1979320\",\n" +
        "        \"_version\": 1,\n" +
        // <snip: irrelevant stuff>
        "        \"status\": 201\n" +
        "      }\n" +
        "    },\n" +
        "    {\n" +
        "      \"flopperdydoodle\": {\n" +
        "        \"_index\": \"movies\",\n" +
        "        \"_id\": \"tt1979321\",\n" +
        // <snip: irrelevant stuff>
        "        \"status\": 201\n" +
        "      }\n" +
        "    }\n" +
        "  ]\n" +
        "}";
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> stringObjectMap = mapper.readValue(createResponses, new TypeReference<>() {
    });
    replay();
    processor.extractItemList(stringObjectMap);
  }

  @Test
  public void testIndividualFallbackOperation() throws JsonProcessingException {
    OpenSearchBatchFailureException testEx =
        new OpenSearchBatchFailureException("testIndividualFallbackOperation Exception", httpResponseMock);
    SynchronizedLinkedBimap<Document,String> batch = new SynchronizedLinkedBimap<>();
    batch.put(docMock1,"tt1979320");
    batch.put(docMock2,"tt1979321");
    String responses = "{\n" +
        "  \"took\": 11,\n" +
        "  \"errors\": true,\n" +
        "  \"items\": [\n" +
        "    {\n" +
        "      \"delete\": {\n" +
        "        \"_index\": \"movies\",\n" +
        "        \"_id\": \"tt1979320\",\n" +
        "        \"_version\": 1,\n" +
        // <snip: irrelevant stuff>
        "        \"status\": 200\n" +
        "      }\n" +
        "    },\n" +
        "    {\n" +
        "      \"delete\": {\n" +
        "        \"_index\": \"movies\",\n" +
        "        \"_id\": \"tt1979321\",\n" +
        // <snip: irrelevant stuff>
        "        \"status\": 404\n" +
        "      }\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    String response1 =
        "      {\n" +
            "        \"_index\": \"movies\",\n" +
            "        \"_id\": \"tt1979320\",\n" +
            "        \"_version\": 1,\n" +
            // <snip: irrelevant stuff>
            "        \"status\": 200\n" +
            "      }";

    String response2 =
        "      {\n" +
            "        \"_index\": \"movies\",\n" +
            "        \"_id\": \"tt1979321\",\n" +
            // <snip: irrelevant stuff>
            "        \"status\": 404\n" +
            "      }";
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> responsesMap = mapper.readValue(responses, new TypeReference<>() {
    });
    Map<String, Object> response1Map = mapper.readValue(response1, new TypeReference<>() {
    });
    Map<String, Object> response2Map = mapper.readValue(response2, new TypeReference<>() {
    });
    ArrayList<Map<String,Object>> responselist = new ArrayList<>();
    responselist.add(response1Map);
    responselist.add(response2Map);


    expect(httpResponseMock.statusCode()).andReturn(200);
    expect(processor.responseAsMap(httpResponseMock)).andReturn(responsesMap);
    expect(docMock1.getId()).andReturn("tt1979320");
    expect(docMock2.getId()).andReturn("tt1979321");
    expect(processor.extractItemList(responsesMap)).andReturn(responselist);
    docMock1.setStatus(INDEXED,"{_index=movies, _id=tt1979320, _version=1, status=200}");
    docMock1.reportDocStatus();
    docMock2.setStatus(ERROR,"{_index=movies, _id=tt1979321, status=404}");
    docMock2.reportDocStatus();

    replay();
    processor.individualFallbackOperation(batch,testEx);
  }

  @Test
  public void testIndividualFallbackOperationNoResponseJson() {
    OpenSearchBatchFailureException testEx =
        new OpenSearchBatchFailureException("testIndividualFallbackOperation Exception", httpResponseMock);
    SynchronizedLinkedBimap<Document,String> batch = new SynchronizedLinkedBimap<>();
    batch.put(docMock1,"tt1979320");
    batch.put(docMock2,"tt1979321");

    expect(httpResponseMock.statusCode()).andReturn(200);
    expect(processor.responseAsMap(httpResponseMock)).andReturn(null);
    processor.handleMissingResponseBody(batch);

    replay();
    processor.individualFallbackOperation(batch,testEx);
  }

  @Test
  public void testIndividualFallbackOperationBadStatus() {
    OpenSearchBatchFailureException testEx =
        new OpenSearchBatchFailureException("testIndividualFallbackOperation Exception", httpResponseMock);
    SynchronizedLinkedBimap<Document,String> batch = new SynchronizedLinkedBimap<>();
    batch.put(docMock1,"tt1979320");
    batch.put(docMock2,"tt1979321");


    expect(httpResponseMock.statusCode()).andReturn(429);
    processor.fallbackHttpNotOk(batch,429, testEx);

    replay();
    processor.individualFallbackOperation(batch,testEx);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIndividualFallbackOperationBadExceptionType() {
    RuntimeException testEx =
        new RuntimeException();
    SynchronizedLinkedBimap<Document,String> batch = new SynchronizedLinkedBimap<>();
    batch.put(docMock1,"tt1979320");
    batch.put(docMock2,"tt1979321");

    replay();
    processor.individualFallbackOperation(batch,testEx);
  }

  @Test
  public void testExtractDeletes() throws JsonProcessingException {
    String createResponses = "{\n" +
        "  \"took\": 11,\n" +
        "  \"errors\": true,\n" +
        "  \"items\": [\n" +
        "    {\n" +
        "      \"delete\": {\n" +
        "        \"_index\": \"movies\",\n" +
        "        \"_id\": \"tt1979320\",\n" +
        "        \"_version\": 1,\n" +
        // <snip: irrelevant stuff>
        "        \"status\": 201\n" +
        "      }\n" +
        "    },\n" +
        "    {\n" +
        "      \"delete\": {\n" +
        "        \"_index\": \"movies\",\n" +
        "        \"_id\": \"tt1979321\",\n" +
        // <snip: irrelevant stuff>
        "        \"status\": 201\n" +
        "      }\n" +
        "    }\n" +
        "  ]\n" +
        "}";
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> stringObjectMap = mapper.readValue(createResponses, new TypeReference<>() {
    });
    replay();
    List<Map<String, Object>> list = processor.extractItemList(stringObjectMap);
    assertNotNull(list);
    assertEquals(2,list.size());
    assertEquals("tt1979320",list.get(0).get("_id"));
    assertEquals("tt1979321",list.get(1).get("_id"));
  }
  @Test
  public void testBuilderHappyPath() throws Exception {
    SendToOpenSearchProcessor.Builder builder = new SendToOpenSearchProcessor.Builder();
    CountDownLatch sendCounter = new CountDownLatch(2);
    builder
        .indexNamed("foo")
        .named("testProc")
        .openSearchAt("https://example.com:9600/")
        .asUser("Dent")
        .authenticatedBy("Arthur Dent")
        .sendingBatchesOf(4)
        .sendingPartialBatchesAfterMs(BATCH_TIMEOUT)
        .withSendListener(sentDocs -> {
          System.out.println("Sent " + sentDocs.size());
          sendCounter.countDown();
        });
    // can't test  builder.insecureTrustAllHttps() here because that causes an http request on build.
    SendToOpenSearchProcessor proc = builder.build();

    proc.setClient(mockHttpClient);

    // expectations for interactions with the document mocks
    setupDocsForBuilder(docMock1,  1);
    setupDocsForBuilder(docMock2, 2);
    setupDocsForBuilder(docMock3, 3);
    setupDocsForBuilder(docMock4, 4);
    setupDocsForBuilder(docMock5, 5);

    indexMock(docMock1, 1, "idTest1", false);
    indexMock(docMock2, 2, "idTest2", false);
    indexMock(docMock3, 3, "idTest3", false);
    indexMock(docMock4, 4, "idTest4", true);
    indexMock(docMock5, 5, "idTest5", false); // causes previous 4 to send, starts new batch

    // expectations for http activity
    //noinspection unchecked
    expect(mockHttpClient.sendAsync(isA(HttpRequest.class), isA(HttpResponse.BodyHandler.class))).andReturn(futureMock);
    expect(futureMock.get()).andReturn(httpResponseMock);

    // expectations for handling of first batch
    expect(httpResponseMock.statusCode()).andReturn(200);
    expect(httpResponseMock.body()).andReturn(RESPONSE); // in batchOperation
    expect(httpResponseMock.statusCode()).andReturn(200);
    expect(httpResponseMock.body()).andReturn(RESPONSE); // in individualFallback due to error in 4

    // after 2nd batch times out:
    //noinspection unchecked
    expect(mockHttpClient.sendAsync(isA(HttpRequest.class), isA(HttpResponse.BodyHandler.class))).andReturn(futureMock);
    expect(futureMock.get()).andReturn(httpResponseMock);
    expect(httpResponseMock.statusCode()).andReturn(200);
    expect(httpResponseMock.body()).andReturn(RESPONSE2); // in batchOperation (should not fall back)

    replay();
    // note, proc must not be an @ObjectUnderTest mock here!
    proc.processDocument(docMock1);
    proc.processDocument(docMock2);
    proc.processDocument(docMock3);
    proc.processDocument(docMock4);
    proc.processDocument(docMock5);

    assertTrue("Not enough batches sent, there are " + sendCounter.getCount() + " batches remaining",
        sendCounter.await(20, TimeUnit.SECONDS));

  }

  private void setupDocsForBuilder(Document documentMock, int i) {
    String id = "idTest" + i;
    expect(documentMock.getOperation()).andReturn(Document.Operation.NEW).anyTimes();
    expect(documentMock.keySet()).andReturn(Set.of("field1", "field2", "jjNonce")).anyTimes();
    List<String> value1 = List.of("value1");
    expect(documentMock.get("field1")).andReturn(value1).anyTimes();
    List<String> value2 = List.of("value2", "value2b");
    expect(documentMock.get("field2")).andReturn(value2).anyTimes();
    List<String> value3 = List.of(id);
    expect(documentMock.get("jjNonce")).andReturn(value3).anyTimes();

    byte[] t = {};
    expect(documentMock.getRawData()).andReturn(t).anyTimes();

    expect(documentMock.getIdField()).andReturn("id").anyTimes();

    expect(documentMock.getFirstValue("id")).andReturn(id).anyTimes();
    expect(documentMock.getId()).andReturn(id).anyTimes();
    // this message verifies the setting of .sendingPartialBatchesAfter

    // generic logging context stuff
    expect(documentMock.getStatusChange()).andReturn(statusChangeMock).anyTimes();
//    expect(statusChangeMock.getStatus()).andReturn(INDEXING).times(2); // once per changing destination
    expect(documentMock.getHash()).andReturn(null).anyTimes();
    expect(documentMock.getSourceScannerName()).andReturn(null).anyTimes();
    expect(documentMock.getParentId()).andReturn(null).anyTimes();
    expect(documentMock.getOrignalParentId()).andReturn(null).anyTimes();
    expect(documentMock.listChangingDestinations()).andReturn(List.of("foo", "bar")).anyTimes();

  }


  private void indexMock(Document documentMock, int i, String id, boolean fail) {
    // when batch is attempted
    expect(documentMock.addNonce("jjNonce")).andReturn(id).anyTimes();
    batchDoc(documentMock, i, id);
    indexDoc(documentMock, i, id);
    finishDoc(documentMock, id, fail);
  }

  private static void finishDoc(Document documentMock, String id, boolean fail) {
    if (fail) {
      documentMock.setStatus(ERROR, "{_index=movies, _id=idTest4, _version=1, result=created, status=400, " +
          "error={type=Bad(ish) Request, reason=Invalid Date Format or whatever, index=movies, shard=0, " +
          "index_uuid=yhizhusbSWmP0G7OJnmcLg}}");
    } else {
      if ("idTest5".equals(id)) {
        documentMock.setStatus(INDEXED, "{} Successfully indexed.", "idTest5");
      } else {
        documentMock.setStatus(INDEXED, "{_index=movies, _id="+ id +", _version=1, result=created, " +
            "_shards={total=2, successful=1, failed=0}, _seq_no=1, _primary_term=1, status=201}");
      }
    }
    documentMock.reportDocStatus();
  }

  private static void indexDoc(Document documentMock, int i, String id) {
    documentMock.setStatus(Status.INDEXING, "{} is being created in opensearch in a batch of {} documents",
        id,
        i <5 ? 4 : 1 // assumes docs numbered 1 2 3 4 5
    );
    documentMock.reportDocStatus();
  }

  private static void batchDoc(Document documentMock, int i, String id) {
    documentMock.setStatus(Status.BATCHED, "{} queued in position {} by {}. Will send within {} milliseconds.",
        id, ((i -1) % 4) , "testProc", BATCH_TIMEOUT);
    documentMock.reportDocStatus();
  }


}

