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

package org.jesterj.ingest.model.impl;

import net.jini.space.JavaSpace;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.Router;
import org.jesterj.ingest.model.Scanner;
import org.jesterj.ingest.model.Step;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 11/29/14
 */

/**
 * A base implementation of a scanner that doesn't do anything. {@link #getScanOperation()} and
 * {@link #getDocumentTracker()} should be overridden for most implementations.
 */
public abstract class ScannerImpl extends StepImpl implements Scanner {

  private static final Logger log = LogManager.getLogger();

  private long interval;

  // can be used to avoid starting a scan while one is still running. This is not required however
  // and can be ignored if desired.
  protected final AtomicBoolean activeScan = new AtomicBoolean(false);
  
  private final ScheduledExecutorService scheduler =
      Executors.newScheduledThreadPool(1);

  // don't want multiple scans initiated simultaneously. If the scanning mechanism
  // wants to distribute work among threads it may, but that should be done in the scan operation.
  private final ExecutorService scanExec = Executors.newSingleThreadExecutor();

  protected ScannerImpl() {
  }


  public void run() {
    ScheduledFuture<?> scanner = scheduler.schedule(getScanOperation(), interval, TimeUnit.MILLISECONDS);
    while (this.isActive()) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    scanner.cancel(true);
  }

  /**
   * What to do when a document has been recognized as required for indexing.
   *
   * @param doc The document to be processed
   */
  public void docFound(Document doc) {
    doc.put(doc.getIdField(), getIdFunction().apply(doc.getId()));
    getDocumentTracker().accept(doc);
    sendToNext(doc);
  }
  
  @Override
  public long getInterval() {
    return this.interval;
  }

  @Override
  public Step[] getSubsequentSteps() {
    return new Step[0];
  }

  @Override
  public boolean isFinalHelper() {
    return false;
  }

  @Override
  public synchronized void activate() {
  }

  @Override
  public synchronized void deactivate() {
  }

  @Override
  public boolean isActive() {
    return false;
  }

  @Override
  public boolean add(Document document) {
    throw new UnsupportedOperationException("Scanners are a push only source of documents. Queue methods are not supported for this type of step.");
  }

  @Override
  public boolean offer(Document document) {
    throw new UnsupportedOperationException("Scanners are a push only source of documents. Queue methods are not supported for this type of step.");
  }

  @Override
  public Document remove() {
    throw new UnsupportedOperationException("Scanners are a push only source of documents. Queue methods are not supported for this type of step.");
  }

  @Override
  public Document poll() {
    throw new UnsupportedOperationException("Scanners are a push only source of documents. Queue methods are not supported for this type of step.");
  }

  @Override
  public Document element() {
    throw new UnsupportedOperationException("Scanners are a push only source of documents. Queue methods are not supported for this type of step.");
  }

  @Override
  public Document peek() {
    throw new UnsupportedOperationException("Scanners are a push only source of documents. Queue methods are not supported for this type of step.");
  }

  @Override
  public void put(Document document) throws InterruptedException {
    throw new UnsupportedOperationException("Scanners are a push only source of documents. Queue methods are not supported for this type of step.");
  }

  @Override
  public boolean offer(Document document, long timeout, TimeUnit unit) throws InterruptedException {
    throw new UnsupportedOperationException("Scanners are a push only source of documents. Queue methods are not supported for this type of step.");
  }

  @Override
  public Document take() throws InterruptedException {
    throw new UnsupportedOperationException("Scanners are a push only source of documents. Queue methods are not supported for this type of step.");
  }

  @Override
  public Document poll(long timeout, TimeUnit unit) throws InterruptedException {
    throw new UnsupportedOperationException("Scanners are a push only source of documents. Queue methods are not supported for this type of step.");
  }

  @Override
  public int remainingCapacity() {
    throw new UnsupportedOperationException("Scanners are a push only source of documents. Queue methods are not supported for this type of step.");
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException("Scanners are a push only source of documents. Queue methods are not supported for this type of step.");
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    throw new UnsupportedOperationException("Scanners are a push only source of documents. Queue methods are not supported for this type of step.");
  }

  @Override
  public boolean addAll(Collection<? extends Document> c) {
    throw new UnsupportedOperationException("Scanners are a push only source of documents. Queue methods are not supported for this type of step.");
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException("Scanners are a push only source of documents. Queue methods are not supported for this type of step.");
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException("Scanners are a push only source of documents. Queue methods are not supported for this type of step.");
  }

  @Override
  public void clear() {

  }

  @Override
  public int size() {
    throw new UnsupportedOperationException("Scanners are a push only source of documents. Queue methods are not supported for this type of step.");
  }

  @Override
  public boolean isEmpty() {
    throw new UnsupportedOperationException("Scanners are a push only source of documents. Queue methods are not supported for this type of step.");
  }

  @Override
  public boolean contains(Object o) {
    throw new UnsupportedOperationException("Scanners are a push only source of documents. Queue methods are not supported for this type of step.");
  }

  @Override
  public Iterator<Document> iterator() {
    throw new UnsupportedOperationException("Scanners are a push only source of documents. Queue methods are not supported for this type of step.");
  }

  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException("Scanners are a push only source of documents. Queue methods are not supported for this type of step.");
  }

  @Override
  public <T> T[] toArray(T[] a) {
    throw new UnsupportedOperationException("Scanners are a push only source of documents. Queue methods are not supported for this type of step.");
  }

  @Override
  public int drainTo(Collection<? super Document> c) {
    throw new UnsupportedOperationException("Scanners are a push only source of documents. Queue methods are not supported for this type of step.");
  }

  @Override
  public int drainTo(Collection<? super Document> c, int maxElements) {
    throw new UnsupportedOperationException("Scanners are a push only source of documents. Queue methods are not supported for this type of step.");
  }

  @Override
  public void advertise() {
    // ignore for now
  }

  @Override
  public void stopAdvertising() {
    // ignore for now
  }

  @Override
  public void acceptJiniRequests() {
    // ignore for now
  }

  @Override
  public void denyJiniRequests() {
    // ignore for now
  }

  @Override
  public boolean readyForJiniRequests() {
    return false;
  }


  @Override
  protected Logger getLogger() {
    return log;
  }

  public boolean isActiveScan() {
    return activeScan.get();
  }

  public void scanStarted() {
    activeScan.set(true);
  }

  public void scanFinished() {
    activeScan.set(false);
  }

  public static abstract class Builder extends StepImpl.Builder {



    public Builder() {
      // abstract class don't need instance
    }

    @Override
    public StepImpl.Builder batchSize(int size) {
      super.batchSize(size);
      return this;
    }

    @Override
    public StepImpl.Builder outputSpace(JavaSpace outputSpace) {
      super.outputSpace(outputSpace);
      return this;
    }

    @Override
    public StepImpl.Builder inputSpace(JavaSpace inputSpace) {
      super.inputSpace(inputSpace);
      return this;
    }

    @Override
    public StepImpl.Builder stepName(String stepName) {
      super.stepName(stepName);
      return this;
    }

    @Override
    public StepImpl.Builder routingBy(Router router) {
      super.routingBy(router);
      return this;
    }

    @Override
    protected abstract ScannerImpl getObject();
    
    public Builder interval(long interval) {
      getObject().interval = interval;
      return this;
    }

    public abstract ScannerImpl build();


  }


}
