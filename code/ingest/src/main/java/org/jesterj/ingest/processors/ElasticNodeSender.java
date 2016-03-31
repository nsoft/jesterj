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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionRequest;
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.jesterj.ingest.Main;
import org.jesterj.ingest.logging.JesterJAppender;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.Status;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 3/30/16
 */
public class ElasticNodeSender extends BatchProcessor<ActionRequest> {
  private static final Logger log = LogManager.getLogger();

  private String name;

  private String nodeName = "My beautiful node";
  private String clusterName = "my-cluster";
  private String indexName;
  private String objectType;

  private Settings settings;
  private Node node;

  private Client client;


  @Override
  protected void perDocumentFailure(Exception e) {
    // something's wrong with the network etc all documents must be errored out:
    for (Document doc : getBatch().keySet()) {
      ThreadContext.put(JesterJAppender.JJ_INGEST_DOCID, doc.getId());
      log.info(Status.ERROR.getMarker(), "{} could not be sent to elastic because of {}", doc.getId(), e.getMessage());
      log.error("Error communicating with elastic!", e);
    }
  }

  @Override
  protected void individualFallbackOperation(Exception e) {
    Map<ActionFuture, ActionRequest> futures = new HashMap<>();
    for (ActionRequest request : getBatch().values()) {
      if (request instanceof UpdateRequest) {
        futures.put(client.update((UpdateRequest) request), request);
      } else if (request instanceof DeleteRequest) {
        futures.put(client.delete((DeleteRequest) request), request);
      } else if (request instanceof IndexRequest) {
        futures.put(client.index((IndexRequest) request), request);
      } else {
        throw new IllegalStateException("Should only have generated index, update and delete " +
            "actions, but found" + request.getClass());
      }
    }

    for (ActionFuture individualRetry : futures.keySet()) {
      ActionRequest request = futures.get(individualRetry);
      Document document = getBatch().inverse().get(request);
      ThreadContext.put(JesterJAppender.JJ_INGEST_DOCID, document.getId());
      try {
        ActionWriteResponse resp = (ActionWriteResponse) individualRetry.actionGet();
        if (resp instanceof UpdateResponse) {
          UpdateResponse ursp = (UpdateResponse) resp;
          if (!ursp.isCreated()) {
            log.info(Status.ERROR.getMarker(), "{} could not be updated in elastic because of some reason that" +
                "the elastic api won't tell us about!", document.getId());
          }
        } else if (resp instanceof DeleteResponse) {
          DeleteResponse drsp = (DeleteResponse) resp;
          if (!drsp.isFound()) {
            log.info(Status.SEARCHABLE.getMarker(), "{} already deleted or never indexed... " +
                "Our work is done here!", document.getId());
          }
        } else if (resp instanceof IndexResponse) {
          IndexResponse irsp = (IndexResponse) resp;
          if (!irsp.isCreated()) {
            log.info(Status.ERROR.getMarker(), "{} could not be created in elastic because of some reason that" +
                "the elastic api won't tell us about!", document.getId());
          }
        } else {
          throw new IllegalStateException("Should only have generated index, update and delete " +
              "actions, but found" + resp.getClass());
        }
      } catch (Exception ex) {
        log.info(Status.ERROR.getMarker(), "{} could not be sent to elastic because of {}",
            document.getId(), ex.getMessage());
        log.error("Error sending to elastic!", e);
      }
    }
  }

  @Override
  protected void batchOperation() throws Exception {
    BulkRequestBuilder builder = client.prepareBulk();
    for (ActionRequest request : getBatch().values()) {
      if (request instanceof UpdateRequest) {
        builder.add((UpdateRequest) request);
      } else if (request instanceof DeleteRequest) {
        builder.add((DeleteRequest) request);
      } else if (request instanceof IndexRequest) {
        builder.add((IndexRequest) request);
      } else {
        throw new IllegalStateException("Should only have generated index, update and delete " +
            "actions, but found" + request.getClass());
      }
    }
    BulkResponse bulkResponse = builder.get();
    if (bulkResponse.hasFailures()) {
      throw new ESBulkFail();
    }
  }

  @Override
  protected boolean exceptionIndicatesDocumentIssue(Exception e) {
    // TODO figure out what causes might be due to a single document vs not available etc
    return e instanceof ESBulkFail;
  }

  @Override
  protected ActionRequest convertDoc(Document document) {
    if (Document.Operation.NEW == document.getOperation()) {
      IndexRequest indexRequest = new IndexRequest(indexName, objectType, document.getId());
      indexRequest.source(document.asMap());
      getBatch().put(document, indexRequest);
      return indexRequest;
    }
    if (Document.Operation.UPDATE == document.getOperation()) {
      UpdateRequest updateRequest = new UpdateRequest(indexName, objectType, document.getId());
      updateRequest.doc(document.asMap());
      getBatch().put(document, updateRequest);
      return updateRequest;
    }
    if (Document.Operation.DELETE == document.getOperation()) {
      DeleteRequest deleteRequest = new DeleteRequest(indexName, objectType, document.getId());
      getBatch().put(document, deleteRequest);
      return deleteRequest;
    }
    throw new UnsupportedOperationException("Operation was:" + document.getOperation());
  }

  @Override
  public String getName() {
    return name;
  }

  public static class Builder extends BatchProcessor.Builder {

    private ElasticNodeSender obj = new ElasticNodeSender();

    @Override
    protected ElasticNodeSender getObj() {
      return obj;
    }

    @Override
    public ElasticNodeSender build() {
      ElasticNodeSender obj = getObj();
      String home = Main.JJ_DIR + System.getProperty("file.separator") + obj.nodeName;
      //noinspection ResultOfMethodCallIgnored
      new File(home).mkdirs();
      obj.settings = Settings.settingsBuilder()
          .put("node.name", obj.nodeName)
          // TODO this should be cusomizable too
          .put("path.home", home)
          .build();
      obj.node = new NodeBuilder()
          .data(false)
          .local(false)
          .client(true)
          .settings(obj.settings)
          .clusterName(obj.clusterName)
          .build().start();
      obj.client = obj.node.client();
      return obj;
    }

    @Override
    public Builder named(String name) {
      getObj().name = name;
      return this;
    }

    public Builder usingCluster(String clusterName) {
      getObj().clusterName = clusterName;
      return this;
    }

    public Builder nodeName(String nodeName) {
      getObj().nodeName = nodeName;
      return this;
    }

    public Builder forIndex(String indexName) {
      getObj().indexName = indexName;
      return this;
    }

    public Builder forObjectType(String objectType) {
      getObj().objectType = objectType;
      return this;
    }
  }

  private static class ESBulkFail extends RuntimeException {
  }
}
