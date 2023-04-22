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
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.Status;
import org.jesterj.ingest.model.impl.NamedBuilder;

import java.util.Map;
import java.util.concurrent.*;

abstract class BatchProcessor<T> implements DocumentProcessor {
  private static final Logger log = LogManager.getLogger();

  private volatile ScheduledExecutorService sender;
  private int batchSize = 100;
  private int sendPartialBatchAfterMs = 5000;
  private ScheduledFuture<?> scheduledSend;

  private final Object batchLock = new Object();
  private final Object sendLock = new Object();

  private ConcurrentBiMap<Document, T> batch;

  {
    // lock on monitor to ensure initialization "happens before" any access.
    synchronized (batchLock) {
      batch = new ConcurrentBiMap<>();
    }
  }

  public Document[] processDocument(Document document) {
    if (this.sender == null) {
      synchronized (this) {
        if (this.sender == null) {
          sender = Executors.newScheduledThreadPool(1, new ThreadFactory() {
            @Override
            public Thread newThread( Runnable r) {
              return new Thread(r) {
                final private Map<String, String> context = ThreadContext.getContext();

                @Override
                public void run() {
                  ThreadContext.putAll(context);
                  super.run();
                }
              };
            }
          });
          schedulePartialBatch();
        }
      }
    }
    T doc = convertDoc(document);
    ConcurrentBiMap<Document, T> oldBatch = null;
    synchronized (batchLock) {
      if (this.batch.size() >= batchSize) {
        oldBatch = takeBatch();
      }
      this.batch.put(document, doc);
      document.setStatus(Status.BATCHED, "{} queued in position {} for sending to solr. " +
          "Will be sent within {} milliseconds.", document.getId(), this.batch.size(), sendPartialBatchAfterMs);
      document.reportDocStatus();    }
    if (oldBatch != null) {
      sendBatch(oldBatch);
    }
    log.trace("Batch Processor ({}) processed {}", getName(), document.getId());

    return new Document[0];
  }

  private ConcurrentBiMap<Document, T> takeBatch() {
    synchronized (batchLock) {
      ConcurrentBiMap<Document, T> oldBatch = this.batch;
      this.batch = new ConcurrentBiMap<>();
      log.trace("took batch {} with size {}", oldBatch.toString(), oldBatch.size());
      return oldBatch;
    }
  }

  private void sendBatch(ConcurrentBiMap<Document, T> oldBatch) {
    // there's a small window where the same BiMap could be grabbed by a timer and a full batch causing a double
    // send. Thus, we have a lock to ensure that the oldBatch.clear() in the finally is called
    // before the second thread tries to send the same batch. We tolerate this because it means batches can fill up
    // while sending is in progress.
    synchronized (sendLock) {
      try {
        if (oldBatch.size() == 0) {
          return;
        }
        batchOperation(oldBatch);
      } catch (InterruptedException e) {
        // no fall back if shutting down, and cassandra won't be avail so no failure marking either
        log.info("Send aborted due to system shutdown");
      } catch (Exception e) {
        log.info("Batch Send failed", e);
        // we may have a single bad document...
        //noinspection ConstantConditions
        if (exceptionIndicatesDocumentIssue(e)) {
          individualFallbackOperation(oldBatch, e);
        } else {
          perDocumentFailure(oldBatch, e);
        }
      } finally {
        schedulePartialBatch();
        oldBatch.clear();
      }
    }
  }

  private void schedulePartialBatch() {
    log.trace("Scheduling partial batch");
    if (scheduledSend != null) {
      scheduledSend.cancel(false);
    }
    scheduledSend = sender.schedule(() -> {
      log.trace("Scheduled Send Activated");
      sendBatch(takeBatch());
    }, sendPartialBatchAfterMs, TimeUnit.MILLISECONDS);
  }

  protected void perDocumentFailure(ConcurrentBiMap<Document, ?> oldBatch, Exception e) {
    // something's wrong with the network etc. all documents must be errored out:
    for (Document doc : oldBatch.keySet()) {
      perDocFailLogging(e, doc);
    }
  }

  DocumentLoggingContext createDocContext(Document doc) {
    return new DocumentLoggingContext(doc);
  }

  protected abstract void perDocFailLogging(Exception e, Document doc);

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


  @SuppressWarnings("unused")
  public static abstract class Builder<T> extends NamedBuilder<BatchProcessor<T>> {

    public Builder<T> sendingBatchesOf(int batchSize) {
      getObj().batchSize = batchSize;
      return this;
    }

    public Builder<T> sendingPartialBatchesAfterMs(int ms) {
      getObj().sendPartialBatchAfterMs = ms;
      return this;
    }

  }
}
