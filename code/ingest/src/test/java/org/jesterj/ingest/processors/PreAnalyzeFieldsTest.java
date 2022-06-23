package org.jesterj.ingest.processors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.TermsResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Slice;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.Scanner;
import org.jesterj.ingest.model.impl.DocumentImpl;
import org.jesterj.ingest.model.impl.PlanImpl;
import org.jesterj.ingest.model.impl.ScannerImpl;
import org.junit.*;
import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;
import org.objenesis.instantiator.ObjectInstantiator;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
// need to ignore org.apache.zookeeper.server.SessionTrackerImpl thread

public class PreAnalyzeFieldsTest extends SolrCloudTestCase {
  private static final Logger log = LogManager.getLogger();
  public static final String CONFIG = "preanalyze";
  public static final String REF_GUIDE_EXAMPLE = "{\n" +
      "  \"v\":\"1\",\n" +
      "  \"str\":\"test ąćęłńóśźż\",\n" +
      "  \"tokens\": [\n" +
      "    {\"t\":\"two\",\"s\":5,\"e\":8,\"i\":1,\"y\":\"word\"},\n" +
      "    {\"t\":\"three\",\"s\":20,\"e\":22,\"i\":1,\"y\":\"foobar\"},\n" +
      "    {\"t\":\"one\",\"s\":123,\"e\":128,\"i\":22,\"p\":\"DQ4KDQsODg8=\",\"y\":\"word\"}\n" +
      "  ]\n" +
      "}";

  public static final String ANALYZED = "{\"str\":\"Quick red fox or 2 + 2 = 4\",\"v\":\"1\",\"tokens\":[" +
      "{\"s\":0,\"t\":\"quick\",\"e\":5,\"i\":1,\"y\":\"<ALPHANUM>\"}," +
      "{\"s\":6,\"t\":\"red\",\"e\":9,\"i\":1,\"y\":\"<ALPHANUM>\"}," +
      "{\"s\":10,\"t\":\"fox\",\"e\":13,\"i\":1,\"y\":\"<ALPHANUM>\"}," +
      "{\"s\":17,\"t\":\"2\",\"e\":18,\"i\":2,\"y\":\"<NUM>\"}," +
      "{\"s\":21,\"t\":\"2\",\"e\":22,\"i\":1,\"y\":\"<NUM>\"}," +
      "{\"s\":25,\"t\":\"4\",\"e\":26,\"i\":1,\"y\":\"<NUM>\"}]}";


  private static CloudSolrClient solrClient;


  @Before
  public void doBefore() throws Exception {
    log.info("STARTUP BEGINS");
    configureCluster(2).addConfig(CONFIG, configset(CONFIG)).configure();
    solrClient = getCloudSolrClient(cluster);
    //log this to help debug potential causes of problems
    log.info("SolrClient: {}", solrClient);
    log.info("ClusterStateProvider {}", solrClient.getClusterStateProvider());
  }

  @After
  public void doAfter() throws Exception {
    log.info("SHUTDOWN BEGINS");
    solrClient.close();
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @AfterClass
  public static void finish() throws Exception {
    IOUtils.close(solrClient);
  }

  @Test
  public void testPreanalyzedJsonAccepted() throws Exception {
    log.info("BEGIN TEST");
    CollectionAdminRequest.createCollection("testCol", CONFIG, 2, 2)
        .setMaxShardsPerNode(2)
        .process(solrClient);
    waitCol(2, "testCol");
    SolrInputDocument sdoc = sdoc("id", "1", "preanalyzed", REF_GUIDE_EXAMPLE);
    assertUpdateResponse(solrClient.add("testCol", Collections.singletonList(sdoc), 100));
  }

  @Test
  public void testPreanalyzeSimpleTextEn() throws Exception {
    PlanImpl plan = new PlanImpl() {
      @Override
      public String getDocIdField() {
        return "id";
      }
    };

    String schemaFile = "solr/configsets/preanalyze/conf/schema.xml";
    PreAnalyzeFields paf = new PreAnalyzeFields.Builder()
        .named("foo")
        .forTypeNamed("text_en")
        .preAnalyzingField("preanalyzed")
        .fromFile(schemaFile)
        .loadingResourcesVia(() -> plan.getClass().getClassLoader())
        .build();

    Scanner s = new ScannerImpl() {
      @Override
      public ScanOp getScanOperation() {
        return null;
      }

      @Override
      public boolean isReady() {
        return false;
      }

      @Override
      public Optional<Document> fetchById(String id) {
        return Optional.empty();
      }

      @Override
      public String getName() {
        return "foo";
      }
    };
    Document document = new DocumentImpl(new byte[0], "1", plan, Document.Operation.NEW, s);
    document.put("preanalyzed", "Quick red fox or 2 + 2 = 4");
    paf.processDocument(document);
    assertEquals(ANALYZED, document.get("preanalyzed").get(0));

    String testCol = "testCol";
    CollectionAdminRequest.createCollection(testCol, CONFIG, 2, 2)
        .setMaxShardsPerNode(2)
        .process(solrClient);
    waitCol(2, testCol);

    // rather than build up a whole plan and try to launch ourselves, mimic SentToSolrCloudProcessor's core
    // sending functionality... Basically all we do here is transfer the analyzed value to a SolrInputDocument.
    SolrInputDocument sdoc = new SolrInputDocument();
    for (String field : document.keySet()) {
      List<String> values = document.get(field);
      if (values.size() > 1) {
        sdoc.addField(field, values);
      } else {
        sdoc.addField(field, document.getFirstValue(field));
      }
    }

    // send the document to our test cluster (constructed by SolrCloudTestCase)
    assertUpdateResponse(solrClient.add(testCol, Collections.singletonList(sdoc), 100));
    assertUpdateResponse(solrClient.commit(testCol));

    // test that something got in
    QueryResponse resp = solrClient.query(testCol, params(
        "q", "*:*",
        "rows", "10"));
    SolrDocumentList results = resp.getResults();
    assertEquals(1, results.getNumFound());

    // test that we can query by id
    resp = solrClient.query(testCol, params(
        "q", "id:1",
        "rows", "10"));
    results = resp.getResults();
    assertEquals(1, results.getNumFound());

    // test that there is something in the field
    resp = solrClient.query(testCol, params(
        "q", "preanalyzed:*",
        "rows", "10"));
    results = resp.getResults();
    assertEquals(1, results.getNumFound());

    // test tokens indexed and searchable
    resp = solrClient.query(testCol, params(
        "q", "preanalyzed:quick",
        "rows", "10"));
    results = resp.getResults();
    assertEquals(1, results.getNumFound());

    // test stopwords not matched as per text_en
    resp = solrClient.query(testCol, params(
        "q", "preanalyzed:or",
        "rows", "10"));
    results = resp.getResults();
    assertEquals(0, results.getNumFound());

    // TA-DA! a fully functional field indexed outside of solr! <bow-left/> <bow-right/>

  }

  private void waitCol(int slices, String collection) {
    waitForState("waiting for collections to be created", collection,
        (liveNodes, collectionState) -> {
          if (collectionState == null) {
            // per predicate javadoc, this is what we get if the collection doesn't exist at all.
            return false;
          }
          Collection<Slice> activeSlices = collectionState.getActiveSlices();
          int size = activeSlices.size();
          return size == slices;
        });
  }

  @SuppressWarnings("rawtypes")
  private void assertUpdateResponse(UpdateResponse rsp) {
    // use of TolerantUpdateProcessor can cause non-thrown "errors" that we need to check for
    List errors = (List) rsp.getResponseHeader().get("errors");
    assertTrue("Expected no errors: " + errors, errors == null || errors.isEmpty());
  }

}
