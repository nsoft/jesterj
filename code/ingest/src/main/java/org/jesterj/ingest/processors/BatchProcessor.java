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
import org.apache.logging.log4j.ThreadContext;
import org.jesterj.ingest.logging.JesterJAppender;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.Status;
import org.jesterj.ingest.model.impl.NamedBuilder;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 3/30/16
 */
abstract class BatchProcessor<T> implements DocumentProcessor {
  private static final Logger log = LogManager.getLogger();

  private final ScheduledExecutorService sender = Executors.newScheduledThreadPool(1);
  private int batchSize = 100;
  private int sendPartialBatchAfterMs = 5000;
  private ScheduledFuture scheduledSend;

  private final Object batchLock = new Object();

  private ConcurrentBiMap<Document, T> batch;

  {
    // lock on monitor to ensure initialization "happens before" any access.
    synchronized (batchLock) {
      batch = new ConcurrentBiMap<>();
    }
  }

  public Document[] processDocument(Document document) {
    T doc = convertDoc(document);
    ConcurrentBiMap<Document, T> oldBatch = null;
    int size;
    synchronized (batchLock) {
      if (this.batch.size() >= batchSize) {
        oldBatch = takeBatch();
      }
      size = this.batch.size();
      this.batch.put(document, doc);
    }
    if (oldBatch != null) {
      sendBatch(oldBatch);
    }
    if (scheduledSend != null) {
      scheduledSend.cancel(false);
    }
    scheduledSend = sender.schedule(() -> sendBatch(takeBatch()), sendPartialBatchAfterMs, TimeUnit.MILLISECONDS);

    log.info(Status.BATCHED.getMarker(), "{} queued in postition {} for sending to solr. " +
        "Will be sent within {} milliseconds.", document.getId(), size, sendPartialBatchAfterMs);

    return new Document[0];
  }

  private ConcurrentBiMap<Document, T> takeBatch() {
    synchronized (batchLock) {
      ConcurrentBiMap<Document, T> oldBatch = this.batch;
      this.batch = new ConcurrentBiMap<>();
      return oldBatch;
    }
  }

  private void sendBatch(ConcurrentBiMap<Document, T> oldBatch) {
    // This really shouldn't be called outside of the synchronized block in process document, but just in case...
    try {
      batchOperation(oldBatch);
    } catch (Exception e) {
      // we may have a single bad document... 
      //noinspection ConstantConditions
      if (exceptionIndicatesDocumentIssue(e)) {
        individualFallbackOperation(oldBatch, e);
      } else {
        perDocumentFailure(oldBatch, e);
      }
    } finally {
      ThreadContext.remove(JesterJAppender.JJ_INGEST_DOCID);
    }
  }

  protected abstract void perDocumentFailure(ConcurrentBiMap<Document, T> oldBatch, Exception e);

  /**
   * If the bulk request fails it might be just one document that's causing a problem, try each document individually
   *
   * @param oldBatch The batch for which to handle failures. This will have been detached from this object and will
   *                 become eligible for garbage collection after this method returns, so do not add objects to it.
   * @param e        the exception reported with the failure
   */
  protected abstract void individualFallbackOperation(ConcurrentBiMap<Document, T> oldBatch, Exception e);

  protected abstract void batchOperation(ConcurrentBiMap<Document, T> documentTConcurrentBiMap) throws Exception;

  @SuppressWarnings("UnusedParameters")
  protected abstract boolean exceptionIndicatesDocumentIssue(Exception e);

  protected abstract T convertDoc(Document document);

  protected int getBatchSize() {
    return batchSize;
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
