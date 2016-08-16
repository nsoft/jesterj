/*
 * Copyright 2014-2016 Needham Software LLC
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
import org.jesterj.ingest.model.ConfiguredBuildable;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.Router;
import org.jesterj.ingest.model.Scanner;
import org.jesterj.ingest.model.Step;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;


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
  protected final AtomicInteger activeScans = new AtomicInteger(0);

  private final ExecutorService exec =
      Executors.newCachedThreadPool();

  // don't want multiple scans initiated simultaneously. If the scanning mechanism
  // wants to distribute work among threads it may, but that should be done in the scan operation.
  private final ExecutorService scanExec = Executors.newSingleThreadExecutor();
  private long nanoInterval;

  protected ScannerImpl() {
  }


  public void run() {
    nanoInterval = interval * 1000000;
    Future<?> scanner = null;
    long last = 0;
    if (isActive()) {
      scanner = exec.submit(getScanOperation());
      last = System.nanoTime();
    }
    while (this.isActive()) {
      try {
        Thread.sleep(25);
        if (isReady() && longerAgoThanInterval(last)) {
          scanner = exec.submit(getScanOperation());
          last = System.nanoTime();
        }
      } catch (InterruptedException e) {
        if (scanner != null) {
          scanner.cancel(true);
        }
        e.printStackTrace();
      }
    }
    if (scanner != null) {
      scanner.cancel(true);
    }
  }

  boolean longerAgoThanInterval(long last) {
    return last + nanoInterval < System.nanoTime();
  }

  /**
   * What to do when a document has been recognized as required for indexing.
   *
   * @param doc The document to be processed
   */
  public void docFound(Document doc) {
    String id = doc.getId();
    Function<String, String> idFunction = getIdFunction();
    String result = idFunction.apply(id);
    String idField = doc.getIdField();
    doc.put(idField, result);
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
  public boolean isEmpty() {
    return true; // always empty as it has no queue. Throwing exception messes up debuggers and Yaml Serialization
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

  public boolean getActiveScans() {
    return activeScans.get() > 0;
  }

  public void scanStarted() {
    activeScans.incrementAndGet();
  }

  public void scanFinished() {
    activeScans.decrementAndGet();
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
    public StepImpl.Builder named(String stepName) {
      super.named(stepName);
      return this;
    }

    @Override
    public StepImpl.Builder routingBy(ConfiguredBuildable<? extends Router> router) {
      super.routingBy(router);
      return this;
    }

    @Override
    protected abstract ScannerImpl getObject();

    /**
     * The scanning frequency. 25ms is the minimum. Smaller intervals will be treated as 25ms
     *
     * @param interval a number of milliseconds >= 25
     * @return This builder object for further configuration.
     */
    public Builder scanFreqMS(long interval) {
      getObject().interval = interval;
      return this;
    }

    public abstract ScannerImpl build();


  }


}
