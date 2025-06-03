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
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.Status;
import org.jesterj.ingest.utils.SynchronizedLinkedBimap;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class SendToSolrProcessor extends BatchProcessor<SolrInputDocument> implements DocumentProcessor {
  private static final Logger log = LogManager.getLogger();
  protected String collection;
  protected String textContentField = "content";
  protected String fieldsField;
  protected Map<String, String> params;
  protected String name;
  protected Function<String, Object> idTransformer;

  private static void markIndexing(Collection<Document> documents, int size) {
    for (Document document : documents) {
      document.setStatus(Status.INDEXING, "Indexing started for a batch of " + size + " documents");
      document.reportDocStatus();
    }
  }

  // in this class logging is important, so encapsulate it for tests.
  public Logger log() {
    return log;
  }

  @Override
  protected void perDocFailLogging(Exception e, Document doc) {
    doc.setStatus(Status.ERROR, "{} could not be sent to solr because of {}", doc.getId(), e.getMessage());
    doc.reportDocStatus();
  }

  @Override
  public boolean isPotent() {
    return true;
  }

  @Override
  protected int individualFallbackOperation(SynchronizedLinkedBimap<Document, SolrInputDocument> oldBatch, Exception e) {
    AtomicInteger succeeded = new AtomicInteger();
    // TODO: send in bisected batches to avoid massive traffic due to one doc when batches are large
    for (Document document : oldBatch.keySet()) {
      createDocContext(document).run(() -> {
        try {
          SolrInputDocument doc = oldBatch.get(document);
          if (doc instanceof Delete) {
            document.setStatus(Status.INDEXING, "{} is being deleted from solr", document.getId());
            document.reportDocStatus();
            getSolrClient().deleteById(oldBatch.inverse().get(doc).getId());
            document.setStatus(Status.INDEXED, "{} deleted from solr successfully", document.getId());
          } else {
            document.setStatus(Status.INDEXING, "{} is being sent to solr", document.getId());
            document.reportDocStatus();
            getSolrClient().add(doc);
            // relying on add to throw if not successful
            document.setStatus(Status.INDEXED, "{} sent to solr successfully", document.getId());
          }
          docsSucceeded.incrementAndGet();
          document.reportDocStatus();
        } catch (IOException | SolrServerException e1) {
          perDocFailLogging(e1,document); // contains reportstatus call
        }
      });
    }
    return succeeded.get();
  }

  @Override
  protected void batchOperation(SynchronizedLinkedBimap<Document, SolrInputDocument> oldBatch) throws SolrServerException, IOException {
    List<Document> documentsToAdd = oldBatch.keySet().stream()
        .filter(doc -> doc.getOperation() != Document.Operation.DELETE).collect(Collectors.toList());
    List<SolrInputDocument> adds = documentsToAdd.stream()
        .map(oldBatch::get)
        .collect(Collectors.toList());
    if (!adds.isEmpty()) {
      Map<String, String> params = getParams();
      if (params == null) {
        SendToSolrProcessor.markIndexing(documentsToAdd, oldBatch.size()); // not factoring out to minimize delay before request to solr
        getSolrClient().add(adds);
      } else {
        // todo: adjust getParams to return Map<String,String[]> to facilitate simpler req.setParams()
        UpdateRequest req = new UpdateRequest();
        req.add(adds);
        // always true right now, but pattern for additional global params...
        for (String s : params.keySet()) {
          req.setParam(s, params.get(s));
        }
        SendToSolrProcessor.markIndexing(documentsToAdd, oldBatch.size());
        getSolrClient().request(req);
      }
    }
    List<Document> documentsToDelete = oldBatch.keySet().stream()
        .filter(doc -> doc.getOperation() == Document.Operation.DELETE)
        .collect(Collectors.toList());
    if (!documentsToDelete.isEmpty()) {
      SendToSolrProcessor.markIndexing(documentsToDelete, oldBatch.size());
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
    String fieldsField = getFieldsField();
    for (String field : document.keySet()) {
      List<String> values = document.get(field);
      doc.addField(field, values);
      // Note that raw data should be empty or have been converted to the bytes of a utf-8 string.
      if (fieldsField != null) {
        doc.addField(fieldsField, field);
      }
    }
    byte[] rawData = document.getRawData();
    if (rawData != null && rawData.length > 0) {
      String value = new String(rawData, StandardCharsets.UTF_8);
      doc.addField(textContentField, value);
    }

    Function<String, Object> idTransformer = getIdTransformer();
    if (idTransformer != null) {
      String idField = document.getIdField();
      doc.remove(idField);
      doc.addField(idField, idTransformer.apply(document.getFirstValue(idField)));
    }
    return doc;
  }

  // visible for testing
  String getFieldsField() {
    return fieldsField;
  }

  Map<String, String> getParams() {
    return params;
  }

  @SuppressWarnings("unused")
  void setParams(Map<String, String> updateChain) {
    this.params = updateChain;
  }

  abstract SolrClient getSolrClient();

  abstract void setSolrClient(SolrClient solrClient);

  // for testing
  Function<String, Object> getIdTransformer() {
    return this.idTransformer;
  }

  private static class Delete extends SolrInputDocument {
  }

  @Override
  public String getName() {
    return name;
  }

  public abstract static class Builder extends BatchProcessor.Builder<SolrInputDocument> {


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

    @SuppressWarnings({"unused", "UnusedReturnValue"})
    public Builder withRequestParameters(Map<String, String> params) {
      getObj().params = params;
      return this;
    }


    public Builder withDocFieldsIn(String fieldsField) {
      getObj().fieldsField = fieldsField;
      return this;
    }

    /**
     * The document identifier is a field that JesterJ relies upon and so it must not be meddled with until
     * the final creation of the solr input document. (i.e. never setId() on the Document).
     *
     * @param transformer a function to replace the id
     * @return this builder for additional configuration
     */
    @SuppressWarnings({"unused", "UnusedReturnValue"})
    public Builder transformIdsWith(Function<String, Object> transformer) {
      getObj().idTransformer = transformer;
      return this;
    }

    public Builder named(String name) {
      getObj().name = name;
      return this;
    }

    protected abstract SendToSolrProcessor getObj() ;

    public abstract SendToSolrProcessor build() ;
  }
}
