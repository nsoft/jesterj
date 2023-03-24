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

import org.apache.cassandra.utils.ConcurrentBiMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.Status;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class SendToSolrCloudProcessor extends BatchProcessor<SolrInputDocument> implements DocumentProcessor {
  private static final Logger log = LogManager.getLogger();

  private String collection;
  private String textContentField = "content";
  private String fieldsField;
  private Map<String, String> params;

  private CloudSolrClient solrClient;
  private String name;

  protected SendToSolrCloudProcessor() {
  }

  // in this class logging is important, so encapsulate it for tests.
  public Logger log() {
    return log;
  }

  @Override
  protected void perDocFailLogging(Exception e, Document doc) {
    doc.setStatus(Status.ERROR, "{} could not be sent to solr because of {}", doc.getId(), e.getMessage());
    doc.reportDocStatus();
    log().error("Error communicating with solr!", e);
  }

  @Override
  public boolean isPotent() {
    return true;
  }

  @Override
  protected void individualFallbackOperation(ConcurrentBiMap<Document, SolrInputDocument> oldBatch, Exception e) {
    // TODO: send in bisected batches to avoid massive traffic due to one doc when batches are large
    for (Document document : oldBatch.keySet()) {
      createDocContext(document).run(() -> {
        try {
          SolrInputDocument doc = oldBatch.get(document);
          if (doc instanceof Delete) {
            document.setStatus(Status.INDEXING,"{} is being deleted from solr", document.getId());
            document.reportDocStatus();
            getSolrClient().deleteById(oldBatch.inverse().get(doc).getId());
            document.setStatus(Status.INDEXED,"{} deleted from solr successfully", document.getId());
          } else {
            document.setStatus(Status.INDEXING,"{} is being sent to solr", document.getId());
            document.reportDocStatus();
            getSolrClient().add(doc);
            document.setStatus(Status.INDEXED,"{} sent to solr successfully", document.getId());
          }
        } catch (IOException | SolrServerException e1) {
          document.setStatus(Status.ERROR,"{} could not be sent to solr because of {}", document.getId(), e1.getMessage());
          log().error("Error sending to solr!", e1);
        }
        document.reportDocStatus();
      });
    }
  }

  @Override
  protected void batchOperation(ConcurrentBiMap<Document, SolrInputDocument> oldBatch) throws SolrServerException, IOException {
    List<Document> documentsToAdd = oldBatch.keySet().stream()
        .filter(doc -> doc.getOperation() != Document.Operation.DELETE).collect(Collectors.toList());
    List<SolrInputDocument> adds = documentsToAdd.stream()
        .map(oldBatch::get)
        .collect(Collectors.toList());
    if (adds.size() > 0) {
      Map<String, String> params = getParams();
      if (params == null) {
        markIndexing(documentsToAdd, oldBatch.size()); // not factoring out to minimize delay before request to solr
        getSolrClient().add(adds);
      } else {
        UpdateRequest req = new UpdateRequest();
        req.add(adds);
        // always true right now, but pattern for additional global params...
        for (String s : params.keySet()) {
          req.setParam(s, params.get(s));
        }
        markIndexing(documentsToAdd, oldBatch.size());
        getSolrClient().request(req);
      }
    }
    List<Document> documentsToDelete = oldBatch.keySet().stream()
        .filter(doc -> doc.getOperation() == Document.Operation.DELETE)
        .collect(Collectors.toList());
    if (documentsToDelete.size() > 0) {
      markIndexing(documentsToDelete, oldBatch.size());
      getSolrClient().deleteById(documentsToDelete.stream().map(Document::getId)
          .collect(Collectors.toList()));
    }
    for (Document document : oldBatch.keySet()) {
      // Note this runnable is being executed immediately in THIS thread.
      if (document.getOperation() == Document.Operation.DELETE) {
        document.setStatus(Status.INDEXED, "{} deleted from solr successfully", document.getId());
      } else {
        document.setStatus(Status.INDEXED, "{} sent to solr successfully", document.getId());
      }
      document.reportDocStatus();
    }
  }

  private static void markIndexing(Collection<Document> documents, int size) {
    for (Document document : documents) {
      document.setStatus(Status.INDEXING, "Indexing started for a batch of " + size + " documents");
      document.reportDocStatus();
    }
  }

  @Override
  @SuppressWarnings("UnusedParameters")
  protected boolean exceptionIndicatesDocumentIssue(Exception e) {
    // TODO make this better
    return e instanceof SolrServerException;
  }

  @Override
  protected SolrInputDocument convertDoc(Document document) {
    SolrInputDocument doc;
    if (document.getOperation() == Document.Operation.DELETE) {
      doc = new Delete();
    } else {
      doc = new SolrInputDocument();
    }
    for (String field : document.keySet()) {
      List<String> values = document.get(field);
      if (values.size() > 1) {
        doc.addField(field, values);
      } else {
        doc.addField(field, document.getFirstValue(field));
      }
      // Note that raw data should be empty or have been converted to the bytes of a utf-8 string.
      if (document.getRawData() != null && document.getRawData().length > 0) {
        String value = new String(document.getRawData(), StandardCharsets.UTF_8);
        doc.addField(textContentField, value);
      }
      if (fieldsField != null) {
        doc.addField(fieldsField, field);
      }
    }
    return doc;
  }

  Map<String, String> getParams() {
    return params;
  }

  @SuppressWarnings("unused")
  void setParams(Map<String, String> updateChain) {
    this.params = updateChain;
  }

  CloudSolrClient getSolrClient() {
    return solrClient;
  }

  void setSolrClient(CloudSolrClient solrClient) {
    this.solrClient = solrClient;
  }

  private static class Delete extends SolrInputDocument {
  }

  @Override
  public String getName() {
    return name;
  }

  public static class Builder extends BatchProcessor.Builder<SolrInputDocument> {

    SendToSolrCloudProcessor obj = new SendToSolrCloudProcessor();
    List<String> zkList = new ArrayList<>();
    String chroot;

    public Builder placingTextContentIn(String field) {
      getObj().textContentField = field;
      return this;
    }

    public Builder usingCollection(String collection) {
      getObj().collection = collection;
      return this;
    }

    @SuppressWarnings("unused")
    public Builder withRequestParameters(Map<String,String> params) {
      getObj().params = params;
      return this;
    }

    /**
     * Add a zookeeper host:port. If :port is omitted :2181 will be assumed
     *
     * @param zk a host name, and port specification
     * @return This builder for further configuration;
     */
    @SuppressWarnings("ConstantConditions")
    public Builder withZookeeper(String zk) {
      if (zk.indexOf(":") < -1) {
        zk += ":2181";
      }
      zkList.add(zk);
      return this;
    }

    @SuppressWarnings("unused")
    public Builder zkChroot(String chroot) {
      this.chroot = chroot;
      return this;
    }

    public Builder withDocFieldsIn(String fieldsField) {
      getObj().fieldsField = fieldsField;
      return this;
    }

    public Builder named(String name) {
      getObj().name = name;
      return this;
    }

    protected SendToSolrCloudProcessor getObj() {
      return obj;
    }

    private void setObj(SendToSolrCloudProcessor obj) {
      this.obj = obj;
    }

    public SendToSolrCloudProcessor build() {
      SendToSolrCloudProcessor tmp = getObj();
      setObj(new SendToSolrCloudProcessor());
      tmp.setSolrClient(new CloudSolrClient.Builder(this.zkList,Optional.ofNullable(chroot)).build());
      tmp.getSolrClient().setDefaultCollection(tmp.collection);
      return tmp;
    }
  }
}
