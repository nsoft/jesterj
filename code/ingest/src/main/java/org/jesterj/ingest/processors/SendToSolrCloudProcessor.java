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
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.jesterj.ingest.logging.JesterJAppender;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.Status;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 3/19/16
 */
public class SendToSolrCloudProcessor implements DocumentProcessor {
  private static final Logger log = LogManager.getLogger();

  private String zkHost;
  private int zkPort = 2181;
  private String collection;
  private int batchSize = 100;
  private int sendPartialBatchAfterMs = 5000;
  private final ScheduledExecutorService sender = Executors.newScheduledThreadPool(1);
  private ScheduledFuture scheduledSend;
  private final ConcurrentHashMap<Document, SolrInputDocument> batch = new ConcurrentHashMap<>(batchSize);
  private String textContentField = "content";

  private CloudSolrClient solrClient;

  protected SendToSolrCloudProcessor() {
  }

  @Override
  public Document[] processDocument(Document document) {
    SolrInputDocument doc = converToSolrDoc(document);
    synchronized (batch) {
      if (batch.size() == batchSize) {
        sendBatch();
      } else {
        batch.put(document, doc);
        if (scheduledSend != null) {
          scheduledSend.cancel(false);
        }
        scheduledSend = sender.schedule((Runnable) this::sendBatch, sendPartialBatchAfterMs, TimeUnit.MILLISECONDS);
      }
      log.info(Status.READY.getMarker(), "{} queued in postition {} for sending to solr. " +
          "Will be sent within {} milliseconds.", document.getId(), batch.size(), sendPartialBatchAfterMs);
    }
    return new Document[0];
  }

  private void sendBatch() {
    // This really shouldn't be called outside of the synchronized block in process document, but just in case...
    synchronized (batch) {
      try {
        solrClient.add(batch.values());
        for (Document document : batch.keySet()) {
          ThreadContext.put(JesterJAppender.JJ_INGEST_DOCID, document.getId());
          log.info(Status.INDEXED.getMarker(), "{} sent to solr successfully", document.getId());
        }
      } catch (SolrServerException e) {
        // we may have a single bad document... 
        //noinspection ConstantConditions
        if (exceptionIndicatesDocumentIssue(e)) {
          // TODO: send in bisected batches to avoid massive traffic down due to one doc when batches are large
          for (Document document : batch.keySet()) {
            ThreadContext.put(JesterJAppender.JJ_INGEST_DOCID, document.getId());
            try {
              solrClient.add(batch.get(document));
              log.info(Status.INDEXED.getMarker(), "{} sent to solr successfully", document.getId());
            } catch (IOException | SolrServerException e1) {
              log.info(Status.ERROR.getMarker(), "{} could not be sent to solr because of {}", document.getId(), e1.getMessage());
              log.error("Error sending to with solr!", e1);
            }
          }
        }
      } catch (IOException e) {
        // something's wrong with the network all documents must be errored out:
        for (Document doc : batch.keySet()) {
          ThreadContext.put(JesterJAppender.JJ_INGEST_DOCID, doc.getId());
          log.info(Status.ERROR.getMarker(), "{} could not be sent to solr because of {}", doc.getId(), e.getMessage());
          log.error("Error communicating with solr!", e);
        }
      } finally {
        batch.clear();
        ThreadContext.remove(JesterJAppender.JJ_INGEST_DOCID);
      }
    }
  }

  @SuppressWarnings("UnusedParameters")
  private boolean exceptionIndicatesDocumentIssue(SolrServerException e) {
    // TODO make this better
    return true;
  }

  private SolrInputDocument converToSolrDoc(Document document) {
    SolrInputDocument doc = new SolrInputDocument();
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
    }
    return doc;
  }


  public static class Builder {

    SendToSolrCloudProcessor obj = new SendToSolrCloudProcessor();

    public Builder sendingBatchesOf(int batchSize) {
      getObj().batchSize = batchSize;
      return this;
    }

    public Builder sendingPartialBatchesAfterMs(int ms) {
      getObj().sendPartialBatchAfterMs = ms;
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

    public Builder withZookeperHost(String hostname) {
      getObj().zkHost = hostname;
      return this;
    }

    public Builder atZookeeperPort(int port) {
      getObj().zkPort = port;
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
      tmp.solrClient = new CloudSolrClient(String.format("%s:%s", tmp.zkHost, tmp.zkPort));
      tmp.solrClient.setDefaultCollection(tmp.collection);
      return tmp;
    }
  }
}
