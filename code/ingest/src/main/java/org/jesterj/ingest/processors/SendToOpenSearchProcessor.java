package org.jesterj.ingest.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.mizosoft.methanol.WritableBodyPublisher;
import com.google.common.annotations.VisibleForTesting;
import nl.altindag.ssl.SSLFactory;
import nl.altindag.ssl.util.CertificateUtils;
import nl.altindag.ssl.util.TrustManagerUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.Status;
import org.jesterj.ingest.utils.SynchronizedLinkedBimap;
import org.jetbrains.annotations.NotNull;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.stream.Collectors;

public class SendToOpenSearchProcessor extends BatchProcessor<String> {
  private static final Logger log = LogManager.getLogger();
  private URL opensearchUrl;
  private String indexName;
  private HttpClient client;
  private String name;
  private final ObjectMapper mapper = new ObjectMapper();
  private String username;
  private String password;

  @VisibleForTesting
  HttpClient getClient() {
    return client;
  }

  @VisibleForTesting
  void setClient(HttpClient client) {
    this.client = client;
  }

  @Override
  protected void perDocFailLogging(Exception e, Document doc) {
    doc.setStatus(Status.ERROR, "Error response from Opensearch! Status=" +
        getResponse((OpenSearchBatchFailureException) e).statusCode());
    doc.reportDocStatus();
  }

  @Override
  protected boolean exceptionIndicatesDocumentIssue(Exception e) {
    // With opensearch we always want to inspect the response
    return true;
  }

  @Override
  protected String convertDoc(Document document) {
    log.debug("Converting {}", document.getId());
    ObjectNode root = getMapper().createObjectNode();
    for (String key : document.keySet()) {
      List<String> value = document.get(key);
      root.put(key, Arrays.toString(value.toArray(new String[]{})));
    }
    return root.toString();
  }

  @Override
  protected void batchOperation(SynchronizedLinkedBimap<Document, String> batch) throws Exception {
    var publisher = WritableBodyPublisher.create();

    var request = HttpRequest.newBuilder()
        .uri(URI.create(opensearchUrl.toString() + "_bulk"))
        .header("Content-Type", "application/json")
        .header("Authorization", "Basic " +
            Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.US_ASCII)))
        .POST(publisher)
        .build();

    log.trace(request);
    log.trace(request.headers());

    // If I've done this right, the batch will be streamed, and we won't be building a huge string
    // of json to send all at once.
    var responseAsync = getClient().sendAsync(request, HttpResponse.BodyHandlers.ofString());

    try (var writer = new PrintWriter(publisher.outputStream())) {
      for (Map.Entry<Document, String> entry : batch.entrySet()) {
        ObjectNode crazyBulk2LineFormat = getMapper().createObjectNode();
        Document doc = entry.getKey();
        crazyBulk2LineFormat.put("_id", doc.getId()); // id for document, not in document json?
        crazyBulk2LineFormat.put("_index", indexName); // redundant specification of index??
        ObjectNode crazyBulkEnvelope = getMapper().createObjectNode();

        switch (doc.getOperation()) {
          case UPDATE:
            doc.setStatus(Status.INDEXING, "{} is being updated in opensearch in a batch of {} documents",
                doc.getId(), batch.size());
            crazyBulkEnvelope.set("index", crazyBulk2LineFormat);
            writer.println(crazyBulkEnvelope);
            writer.println(entry.getValue()); // data next to envelope, rather than within it?
            log.trace(crazyBulkEnvelope.toString());
            log.trace(entry.getValue());
            break;
          case DELETE:
            doc.setStatus(Status.INDEXING, "{} is being deleted from opensearch in a batch of {} documents",
                doc.getId(), batch.size());
            crazyBulkEnvelope.set("delete", crazyBulk2LineFormat);
            writer.println(crazyBulkEnvelope);
            log.trace(crazyBulkEnvelope.toString());
            // no doc to send
            break;
          case NEW:
            doc.setStatus(Status.INDEXING, "{} is being created in opensearch in a batch of {} documents",
                doc.getId(), batch.size());
            crazyBulkEnvelope.set("create", crazyBulk2LineFormat);
            writer.println(crazyBulkEnvelope);
            writer.println(entry.getValue()); // data next to envelope, rather than within it?
            log.trace(crazyBulkEnvelope.toString());
            log.trace(entry.getValue());
        }
        doc.reportDocStatus();
      }
    }

    // should block till batch processed on server side and response received
    HttpResponse<String> resp = responseAsync.get();
    Map<String, Object> respJson = null;
    int statusCode = resp.statusCode();
    if (statusCode != 200 || (respJson = responseAsMap(resp)) == null || respContainsErrors(respJson)) {
      log.trace("response Uri:{}", resp::uri);
      log.debug("response Status:{}", statusCode);
      log.trace("response headers:{}", resp::headers);
      log.trace("response body:\n{}", resp::body);
      log.trace(respJson);
      throw new OpenSearchBatchFailureException(
          "Opensearch batch contains failures. Status=" + statusCode, resp);
    } else {
      // with opensearch single document failures do not fail the whole batch
      for (Document document : batch.keySet()) {
        document.setStatus(Status.INDEXED, "{} Successfully indexed.", document.getId());
        document.reportDocStatus();
      }
    }
  }

  private static boolean respContainsErrors(Map<String, Object> respJson) {
    Object errors = respJson.get("errors");
    return "true".equals(String.valueOf(errors));
  }

  @Override
  public boolean isSafe() {
    return false;
  }

  @Override
  public boolean isPotent() {
    return true;
  }

  @Override
  protected int individualFallbackOperation(SynchronizedLinkedBimap<Document, String> batch, Exception e) {
    int successCount = 0;
    if (!(e instanceof OpenSearchBatchFailureException)) {
      throw new IllegalArgumentException();
    }
    OpenSearchBatchFailureException ex = (OpenSearchBatchFailureException) e;

    int statusCode = ex.response.statusCode();
    if (statusCode == 200) {
      Map<String, Object> respMap = responseAsMap(ex.response);
      if (respMap != null) {
        Map<String, Map.Entry<Document, String>> lookupDoc = new HashMap<>();
        for (Map.Entry<Document, String> item : batch.entrySet()) {
          lookupDoc.put(item.getKey().getId(), item);
        }
        List<Map<String, Object>> items = extractItemList(respMap);
        for (Map<String, Object> item : items) {
          // With opensearch they don't have batch all or nothing functionality. We just have to hope that
          // none of the operations in the batch were on the same document (i.e. no delete, create sequence
          // on the same id where an error could cause re-ordering) The inability to guard against
          // this possibility is a design flaw in OpenSearch, and we can't solve it here.
          Document doc = lookupDoc.get((String)item.get("_id")).getKey();
          if (Integer.parseInt(String.valueOf(item.get("status"))) >= 400) {
            doc.setStatus(Status.ERROR, item.toString());
            doc.reportDocStatus();
          } else {
            doc.setStatus(Status.INDEXED, item.toString());
            doc.reportDocStatus();
            successCount++;
          }
        }
      } else {
        handleMissingResponseBody(batch);
      }
    } else {
      fallbackHttpNotOk(batch, statusCode, ex);
    }
    return successCount;
  }

  /**
   * Extract a list of items from the relatively inconvenient response format. This is isolated to it's own method
   * in hopes of insulating any changes, especially in the event that this format diverges in Opensearch vs Elastic.
   *
   * @param respMap The {@code Map<String,Object>} form of Elastic's json response.
   * @return a list of map objects
   */
  @SuppressWarnings("unchecked")
  @VisibleForTesting
  List<Map<String, Object>> extractItemList(Map<String, Object> respMap) {
    List<Map<String, Object>> items = (List<Map<String, Object>>) respMap.get("items");
    items = items.stream()
        .map(i -> {
          // try the several possibilities
          Map<String, Object> create = (Map<String, Object>) i.get("create");
          Map<String, Object> index = (Map<String, Object>) i.get("index");
          Map<String, Object> delete = (Map<String, Object>) i.get("delete");
          // return whichever hits as an item in the list. We won't care what
          // type of operation it was anyway.
          Map<String, Object> listItem = create != null ? create : delete != null ? delete : index;
          if (listItem == null) {
            throw new IllegalArgumentException("Unknown element type, expected one of 'create','index'," +
                "'delete'! available keys were " + i.keySet());
          }
          return listItem;
        })
        .collect(Collectors.toList());
    return items;
  }

  @VisibleForTesting
  void handleMissingResponseBody(SynchronizedLinkedBimap<Document, String> batch) {
    log.error("Success response from opensearch (status 200), but no json body!! Document state unknown so we " +
        "will not retry to ensure at-most-once delivery. This is unexpected behavior from OpenSearch!!");
    for (Document document : batch.keySet()) {
      document.setStatus(Status.DEAD, "Unexpected empty response body from Opensearch!");
      document.reportDocStatus();
    }
  }

  @VisibleForTesting
  void fallbackHttpNotOk(SynchronizedLinkedBimap<Document, String> batch, int statusCode, OpenSearchBatchFailureException ex) {
    log.debug("Error response from opensearch (status {})", statusCode);
    log.trace("response headers:{}", getResponse(ex)::headers);
    log.trace("response body:\n{}", getResponse(ex)::body);
    for (Document document : batch.keySet()) {
      perDocFailLogging(ex, document);
    }
  }

  @VisibleForTesting
  Map<String, Object> responseAsMap(HttpResponse<String> response) {
    try {
      String body = response.body();
      TypeReference<Map<String, Object>> valueTypeRef = new TypeReference<>() {
      };
      return getMapper().readValue(body, valueTypeRef);
    } catch (JsonProcessingException e) {
      return null;
    }
  }

  @VisibleForTesting
  ObjectMapper getMapper() {
    return mapper;
  }

  private static HttpResponse<String> getResponse(OpenSearchBatchFailureException ex) {
    return ex.response;
  }

  @Override
  public String getName() {
    return name;
  }

  @SuppressWarnings("unused")
  public static class Builder extends BatchProcessor.Builder<String> {

    private SendToOpenSearchProcessor obj = new SendToOpenSearchProcessor();
    private boolean insecureSslConnect = false;

    @Override
    public Builder named(String name) {
      getObj().name = name;
      return this;
    }

    @Override
    public boolean isValid() {
      return super.isValid();
    }

    public Builder sendingBatchesOf(int batchSize) {
      super.sendingBatchesOf(batchSize);
      return this;
    }

    public Builder sendingPartialBatchesAfterMs(int ms) {
      super.sendingPartialBatchesAfterMs(ms);
      return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    public Builder openSearchAt(String url) throws MalformedURLException {
      getObj().opensearchUrl = new URL(url);
      return this;
    }

    public Builder indexNamed(String name) {
      getObj().indexName = name;
      return this;
    }

    public Builder asUser(String username) {
      getObj().username = username;
      return this;
    }

    public Builder authenticatedBy(String password) {
      getObj().password = password;
      return this;
    }


    /**
     * This enables a trust on first connect (in)security model. Https connections will be trusted
     * on first access (which happens during build()). This is more secure than simply disabling
     * https, but it is INAPPROPRIATE for production usage, where proper trust should be installed
     * in the JVM truststore. Setting this value will cause JesterJ to complain loudly, but
     * otherwise continue. Use of this option is only expected for testing and developer local
     * operations.
     *
     * @return this builder for ongoing configuration
     * @see #insecureClient(String)
     */
    public Builder insecureTrustAllHttps() {
      insecureSslConnect = true;
      return this;
    }

    @Override
    protected SendToOpenSearchProcessor getObj() {
      return obj;
    }

    @Override
    public SendToOpenSearchProcessor build() {
      if (this.insecureSslConnect) {
        getObj().client = insecureClient(getObj().opensearchUrl.toString());
      } else {
        getObj().client = secureClient();
      }
      SendToOpenSearchProcessor built = getObj();
      obj = new SendToOpenSearchProcessor();
      return built;
    }

    protected HttpClient secureClient() {
      SSLFactory sslFactory = getSslFactory();
      return HttpClient.newBuilder()
          .sslParameters(sslFactory.getSslParameters())
          .sslContext(sslFactory.getSslContext())
          .build();
    }

    private SSLFactory getSslFactory() {
      return SSLFactory.builder()
          .withDefaultTrustMaterial()
          .withSystemTrustMaterial()
          .withInflatableTrustMaterial()
          .build();
    }

    /**
     * Forge an insecure connection to opensearch. If the url starts with https: the resulting
     * http client will download and trust the server's certificate automatically. THIS IS
     * INAPPROPRIATE for production systems. You have been warned, run with scissors at your
     * own risk!
     *
     * <p>That said, if the insecure url starts with https, the communications will be encrypted, and
     * the server's certificate cannot be changed without restarting JesterJ. Using this method
     * to generate a client leaves a vulnerability open every time an instance of
     * SendToOpenSearchProcessor is created.
     *
     * @param trustedServerUrl a url for an opensearch server that will be implicitly trusted.
     * @return an http client willing to connect to that server only
     */
    protected HttpClient insecureClient(String trustedServerUrl) {
      String message = "******* W A R N I N G ******* - connection to " + trustedServerUrl + " is implicitly trusted." +
          " This message should never be seen in a production system!";
      log.error(message);
      System.out.println(message);
      System.err.println(message);

      if (!trustedServerUrl.startsWith("https:")) {
        // no special configuration for http, still https capable and requiring trusted certs in case
        // https is deployed on port 80 for some reason.
        return secureClient();
      }

      SSLFactory sslFactory = getSslFactory();

      // Grab the cert from the server
      List<X509Certificate> certificates = CertificateUtils.getCertificatesFromExternalSource(trustedServerUrl);

      // install and trust the server's cert
      //noinspection OptionalGetWithoutIsPresent
      TrustManagerUtils.addCertificate(sslFactory.getTrustManager().get(), certificates);

      return HttpClient.newBuilder()
          .sslParameters(sslFactory.getSslParameters())
          .sslContext(sslFactory.getSslContext())
          .build();
    }

    private @NotNull List<String> getServerCerts() throws NoSuchAlgorithmException, KeyManagementException, IOException, CertificateEncodingException {
      SSLContext sslCtx = SSLContext.getInstance("TLS");
      sslCtx.init(null, new TrustManager[]{new X509TrustManager() {

        private X509Certificate[] accepted;

        @Override
        public void checkClientTrusted(X509Certificate[] xcs, String string) {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] xcs, String string) {
          accepted = xcs;
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
          return accepted;
        }
      }}, null);

      HttpsURLConnection connection = (HttpsURLConnection) getObj().opensearchUrl.openConnection();
      connection.setHostnameVerifier((string, ssls) -> true);
      connection.setSSLSocketFactory(sslCtx.getSocketFactory());

      List<String> certs = new ArrayList<>();
      if (connection.getResponseCode() == 200) {
        Certificate[] certificates = connection.getServerCertificates();
        for (Certificate certificate : certificates) {
          certs.add(new String(certificate.getEncoded()));
        }
      }
      return certs;
    }
  }
}
