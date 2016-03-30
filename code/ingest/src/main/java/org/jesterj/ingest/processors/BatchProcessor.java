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
import org.jesterj.ingest.logging.JesterJAppender;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.Status;
import org.jesterj.ingest.model.impl.NamedBuilder;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 3/30/16
 */
public abstract class BatchProcessor<T> implements DocumentProcessor {
  private static final Logger log = LogManager.getLogger();

  private final ScheduledExecutorService sender = Executors.newScheduledThreadPool(1);
  private int batchSize = 100;
  private int sendPartialBatchAfterMs = 5000;
  private ScheduledFuture scheduledSend;
  private ConcurrentHashMap<Document, T> batch;

  public Document[] processDocument(Document document) {
    T doc = convertDoc(document);
    synchronized (getBatch()) {
      if (getBatch().size() == batchSize) {
        sendBatch();
      } else {
        getBatch().put(document, doc);
        if (scheduledSend != null) {
          scheduledSend.cancel(false);
        }
        scheduledSend = sender.schedule((Runnable) this::sendBatch, sendPartialBatchAfterMs, TimeUnit.MILLISECONDS);
      }
      log.info(Status.BATCHED.getMarker(), "{} queued in postition {} for sending to solr. " +
          "Will be sent within {} milliseconds.", document.getId(), getBatch().size(), sendPartialBatchAfterMs);
    }
    return new Document[0];
  }

  private void sendBatch() {
    // This really shouldn't be called outside of the synchronized block in process document, but just in case...
    synchronized (getBatch()) {
      try {
        batchOperation();
      } catch (Exception e) {
        // we may have a single bad document... 
        //noinspection ConstantConditions
        if (exceptionIndicatesDocumentIssue(e)) {
          individualFallbackOperation();
        } else {
          perDocumentFailure(e);
        }
      } finally {
        getBatch().clear();
        ThreadContext.remove(JesterJAppender.JJ_INGEST_DOCID);
      }
    }
  }

  protected abstract void perDocumentFailure(Exception e);

  protected abstract void individualFallbackOperation();

  protected abstract void batchOperation() throws Exception;

  @SuppressWarnings("UnusedParameters")
  protected abstract boolean exceptionIndicatesDocumentIssue(Exception e);

  protected abstract T convertDoc(Document document);

  protected ConcurrentHashMap<Document, T> getBatch() {
    return batch;
  }

  protected int getBatchSize() {
    return batchSize;
  }

  protected void setBatch(ConcurrentHashMap<Document, T> batch) {
    this.batch = batch;
  }

  public static abstract class Builder extends NamedBuilder<BatchProcessor> {

    public Builder sendingBatchesOf(int batchSize) {
      getObj().batchSize = batchSize;
      return this;
    }

    public Builder sendingPartialBatchesAfterMs(int ms) {
      getObj().sendPartialBatchAfterMs = ms;
      return this;
    }

  }
}
