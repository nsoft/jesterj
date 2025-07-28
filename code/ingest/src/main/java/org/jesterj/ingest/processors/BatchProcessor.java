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
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.Status;
import org.jesterj.ingest.model.impl.NamedBuilder;
import org.jesterj.ingest.utils.SynchronizedLinkedBimap;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

abstract class BatchProcessor<T> implements DocumentProcessor {
  private static final Logger log = LogManager.getLogger();
  protected AtomicLong docsReceived = new AtomicLong(0);
  protected AtomicLong docsSucceeded = new AtomicLong(0);
  protected AtomicLong docsAttempted = new AtomicLong(0);

  private volatile ScheduledExecutorService sender;
  private int batchSize = 100;
  private int sendPartialBatchAfterMs = 5000;
  private ScheduledFuture<?> scheduledSend;

  private final Object batchLock = new Object();
  private final Object sendLock = new Object();

  private final List<BatchSendListener> sendListeners = Collections.synchronizedList(new ArrayList<>());

  // While order is not critical for proper functionality,
  // it is useful for writing unit tests, if this proves to
  // be a bottleneck later we can optimize it.
  private SynchronizedLinkedBimap<Document, T> batch;
  private String nonceField = "jjNonce";

  {
    // lock on monitor to ensure initialization "happens before" any access.
    synchronized (batchLock) {
      batch = new SynchronizedLinkedBimap<>();
    }
  }

  // these 3 are primarily for testing purposes
  public long getDocsSucceeded() {
    return docsSucceeded.get();
  }

  public long getDocsReceived() {
    return docsReceived.get();
  }
  public long getDocsAttempted() {
    return docsAttempted.get();
  }

  public Document[] processDocument(Document document) {
    log.debug("Document {} received by batch processor {}", document.getId(), getName());
    document.addNonce(nonceField);
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
          log.debug("Batch send thread started for {}", getName());
        }
      }
    }
    T searchEngineDocument = convertDoc(document);
    SynchronizedLinkedBimap<Document, T> oldBatch = null;
    synchronized (batchLock) {
      if (this.batch.size() >= batchSize) {
        oldBatch = takeBatch();
      }
      docsReceived.incrementAndGet();
      log.trace("adding {}", document.getId() );
      this.batch.put(document, searchEngineDocument);
      document.setStatus(Status.BATCHED, "{} queued in position {} by {}. " +
          "Will send within {} milliseconds.", document.getId(), this.batch.size() - 1,getName(), sendPartialBatchAfterMs);
      document.reportDocStatus();
    }
    if (oldBatch != null) {
      sendBatch(oldBatch);
    }
    log.trace("Batch Processor ({}) processed {}", getName(), document.getId());

    return new Document[0];
  }

  private SynchronizedLinkedBimap<Document, T> takeBatch() {
    synchronized (batchLock) {
      SynchronizedLinkedBimap<Document, T> oldBatch = this.batch;
      this.batch = new SynchronizedLinkedBimap<>();
      log.trace("took batch {} with size {}", oldBatch.toString(), oldBatch.size());
      return oldBatch;
    }
  }

  private void sendBatch(SynchronizedLinkedBimap<Document, T> oldBatch) {
    docsAttempted.addAndGet(oldBatch.size());
    // there's a small window where the same BiMap could be grabbed by a timer and a full batch causing a double
    // send. Thus, we have a lock to ensure that the oldBatch.clear() in the finally is called
    // before the second thread tries to send the same batch. We tolerate this because it means batches can fill up
    // while sending is in progress.
    synchronized (sendLock) {
      try {
        if (oldBatch.isEmpty()) {
          return;
        }
        batchOperation(oldBatch);
        docsSucceeded.addAndGet(oldBatch.size());
      } catch (InterruptedException e) {
        // no fall back if shutting down, and cassandra won't be avail so no failure marking either
        log.info("Send aborted due to system shutdown");
      } catch (Throwable e) {
        Exception ex = handleAssertionErrorsForTests(e); // other Errors rethrown
        log.info("Batch Send failed", ex);
        // we may have a single bad document...
        //noinspection ConstantConditions
        if (exceptionIndicatesDocumentIssue(ex)) {
          docsSucceeded.addAndGet(
              individualFallbackOperation(oldBatch, ex)
          );
        } else {
          // in this case the entire batch failed (i/o error etc)
          entireBatchFailure(oldBatch, ex);
        }
      } finally {
        schedulePartialBatch();
        for (BatchSendListener sendListener : sendListeners) {
          // inspect doc statuses to determine disposition
          sendListener.batchSent(new ArrayList<>(oldBatch.keySet()));
        }
        oldBatch.clear();
      }
    }
  }

  private static @NotNull Exception handleAssertionErrorsForTests(Throwable e) {
    Exception ex;
    if (e instanceof AssertionError) {
      // unit test... if we don't do this assertion failures are hidden
      ex = new Exception(e);
    } else if (e instanceof Exception) {
      ex = (Exception) e;
    } else {
      throw (Error) e;
    }
    return ex;
  }

  private void schedulePartialBatch() {
    log.trace("Scheduling partial batch");
    if (scheduledSend != null) {
      if (!scheduledSend.cancel(false)){
        log.warn("Double cancel in {} (this is only a problem if it starts happening more frequently then the batch timeout)",getName());
      }
    }
    // small but unimportant race condition here. If another thread passes us at this point
    // we will cancel their task, but at worst this is a <2x inflation of send time, and
    // that case is only likely with exceptionally broken thread scheduling, or very short
    // settings for sendPartialBatchAfterMs. It is only possible to send later, not to
    // fail to send (except system shut down which is what fault tolerance handles)
    // in theory its possible to imagine repeated stall resume at exactly this point creating
    // infinite delay, but I can think of no logic by which such a thing would happen consistently.
    // The above double cancel log message showing up frequently would indicate this has become
    // a possibility.
    scheduledSend = sender.schedule(() -> {
      log.trace("Scheduled Send Activated");
      sendBatch(takeBatch());
    }, sendPartialBatchAfterMs, TimeUnit.MILLISECONDS);
  }

  protected void entireBatchFailure(SynchronizedLinkedBimap<Document, ?> oldBatch, Exception e) {
    // something's wrong with the network etc. all documents must be errored out:
    for (Document doc : oldBatch.keySet()) {
      perDocFailLogging(e, doc);
    }
    log.error("Error communicating with destination!", e);
  }

  DocumentLoggingContext createDocContext(Document doc) {
    return new DocumentLoggingContext(doc);
  }

  protected abstract void perDocFailLogging(Exception e, Document doc);

  /**
   * If the bulk request fails it might be just one document that's causing a problem, try each document individually
   * or handle information returned about which documents succeeded if available
   *
   * @param oldBatch The batch for which to handle failures. This will have been detached from this object and will
   *                 become eligible for garbage collection after this method returns, so do not add objects to it.
   * @param e        the exception reported with the failure
   * @return the number of individual documents that succeeded
   */
  protected abstract int individualFallbackOperation(SynchronizedLinkedBimap<Document, T> oldBatch, Exception e);

  protected abstract void batchOperation(SynchronizedLinkedBimap<Document, T> documentTConcurrentLinkedHashMap) throws Exception;

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

    public Builder<T> storingNonceIn(String field) {
      getObj().nonceField = field;
      return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    public Builder<T> withSendListener(BatchSendListener listener) {
      getObj().sendListeners.add(listener);
      return this;
    }

  }
}
