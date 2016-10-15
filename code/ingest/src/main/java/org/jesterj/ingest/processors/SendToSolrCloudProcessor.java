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
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.Status;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 3/19/16
 */
public class SendToSolrCloudProcessor extends BatchProcessor<SolrInputDocument> implements DocumentProcessor {
  private static final Logger log = LogManager.getLogger();

  private String collection;
  private String textContentField = "content";
  private String fieldsField;

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
    log().info(Status.ERROR.getMarker(), "{} could not be sent to solr because of {}", doc.getId(), e.getMessage());
    log().error("Error communicating with solr!", e);
  }

  @Override
  protected void individualFallbackOperation(ConcurrentBiMap<Document, SolrInputDocument> oldBatch, Exception e) {
    // TODO: send in bisected batches to avoid massive traffic down due to one doc when batches are large
    for (Document document : oldBatch.keySet()) {
      putIdInThreadContext(document);
      try {
        SolrInputDocument doc = oldBatch.get(document);
        if (doc instanceof Delete) {
          solrClient.deleteById(oldBatch.inverse().get(doc).getId());
          log().info(Status.INDEXED.getMarker(), "{} deleted from solr successfully", document.getId());
        } else {
          solrClient.add(doc);
          log().info(Status.INDEXED.getMarker(), "{} sent to solr successfully", document.getId());
        }
      } catch (IOException | SolrServerException e1) {
        log().info(Status.ERROR.getMarker(), "{} could not be sent to solr because of {}", document.getId(), e1.getMessage());
        log().error("Error sending to with solr!", e1);
      }
    }
  }

  @Override
  protected void batchOperation(ConcurrentBiMap<Document, SolrInputDocument> oldBatch) throws SolrServerException, IOException {
    List<String> deletes = oldBatch.keySet().stream()
        .filter(doc -> doc.getOperation() == Document.Operation.DELETE)
        .map(Document::getId)
        .collect(Collectors.toList());
    if (deletes.size() > 0) {
      solrClient.deleteById(deletes);
    }
    List<SolrInputDocument> adds = oldBatch.keySet().stream()
        .filter(doc -> doc.getOperation() != Document.Operation.DELETE)
        .map(oldBatch::get)
        .collect(Collectors.toList());
    if (adds.size() > 0) {
      solrClient.add(adds);
    }
    for (Document document : oldBatch.keySet()) {
      putIdInThreadContext(document);
      if (document.getOperation() == Document.Operation.DELETE) {
        log().info(Status.INDEXED.getMarker(), "{} deleted from solr successfully", document.getId());
      } else {
        log().info(Status.INDEXED.getMarker(), "{} sent to solr successfully", document.getId());
      }
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
        String value = new String(document.getRawData(), Charset.forName("UTF-8"));
        doc.addField(textContentField, value);
      }
      if (fieldsField != null) {
        doc.addField(fieldsField, field);
      }
    }
    return doc;
  }

  private static class Delete extends SolrInputDocument {
  }
  
  @Override
  public String getName() {
    return name;
  }


  public static class Builder extends BatchProcessor.Builder {

    SendToSolrCloudProcessor obj = new SendToSolrCloudProcessor();
    List<String> zkList = new ArrayList<>();
    String chroot;

    public Builder sendingBatchesOf(int batchSize) {
      super.sendingBatchesOf(batchSize);
      return this;
    }

    public Builder sendingPartialBatchesAfterMs(int ms) {
      super.sendingPartialBatchesAfterMs(ms);
      return this;
    }

    public Builder placingTextContentIn(String field) {
      getObj().textContentField = field;
      return this;
    }

    public Builder usingCollection(String collection) {
      getObj().collection = collection;
      return this;
    }

    /**
     * Add a zookeeper host:port. If :port is omitted :2181 will be assumed
     *
     * @param zk a host name, and port specification
     * @return This builder for further configuration;
     */
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
      String zkConnection = StringUtils.join(zkList, ',');
      zkConnection = chroot == null ? zkConnection : zkConnection + chroot;
      tmp.solrClient = new CloudSolrClient(zkConnection);
      tmp.solrClient.setDefaultCollection(tmp.collection);
      return tmp;
    }
  }
}
