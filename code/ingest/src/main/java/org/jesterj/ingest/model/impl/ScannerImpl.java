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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import net.jini.space.JavaSpace;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.ConfiguredBuildable;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.Router;
import org.jesterj.ingest.model.Scanner;
import org.jesterj.ingest.model.Status;
import org.jesterj.ingest.model.Step;
import org.jesterj.ingest.persistence.Cassandra;
import org.jesterj.ingest.persistence.CassandraSupport;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A base implementation of a scanner that doesn't do anything. {@link #getScanOperation()} and
 * {@link #getDocumentTracker()} should be overridden for most implementations.
 */
public abstract class ScannerImpl extends StepImpl implements Scanner {

  private static final Logger log = LogManager.getLogger();
  public static final int DEF_MAX_ERROR_RETRY = Integer.getInteger("org.jesterj.scanner.max_error_retry", 3);

  private boolean hashing;
  private long interval;
  boolean remembering;
  private int retryErrors = DEF_MAX_ERROR_RETRY;

  // can be used to avoid starting a scan while one is still running. This is not required however
  // and can be ignored if desired.
  @SuppressWarnings("WeakerAccess")
  protected final AtomicInteger activeScans = new AtomicInteger(0);

  private final ExecutorService exec =
      new ThreadPoolExecutor(0, 1,
          60L, TimeUnit.SECONDS,
          new SynchronousQueue<>(), r -> {
        Thread scanner = new Thread(r);
        scanner.setName("jj-scan-" + ScannerImpl.this.getName());
        scanner.setDaemon(true);
        return scanner;
      });

  private long nanoInterval;

  private CassandraSupport cassandra = new CassandraSupport();

  static final String FIND_DIRTY_FOR_SCANNER_Q = "dirty_for_scanner";
  static final String FIND_PROCESSING_FOR_SCANNER_Q = "proc_for_scanner";
  static final String FIND_ERROR_FOR_SCANNER_Q = "error_for_scanner";
  static final String FIND_BATCHED_FOR_SCANNER_Q = "batched_for_scanner";
  static final String FIND_RESTART_FOR_SCANNER_Q = "restart_for_scanner";

  public static final String FOR_SCANNER = "and scanner = ? ALLOW FILTERING";
  static final String FIND_DIRTY =
      "SELECT docid, scanner FROM jj_logging.fault_tolerant WHERE status = 'DIRTY' ";
  static final String FIND_PROCESSING =
      "SELECT docid, scanner FROM jj_logging.fault_tolerant WHERE status = 'PROCESSING' ";
  static final String FIND_ERROR =
      "SELECT docid, scanner FROM jj_logging.fault_tolerant WHERE status = 'ERROR' ";
  static final String FIND_BATCHED =
      "SELECT docid, scanner FROM jj_logging.fault_tolerant WHERE status = 'BATCHED' ";
  static final String FIND_RESTART =
      "SELECT docid, scanner FROM jj_logging.fault_tolerant WHERE status = 'RESTART' ";

  public static final String RESET_DOCS_U = "RESET_DOCS_U";
  static final String RESET_DOCS = "UPDATE jj_logging.fault_tolerant SET status = 'RESTART' " +
      " where docid = ? and scanner = ? ";

  public static final String UPDATE_HASH_U = "UPDATE_HASH_U";
  static final String UPDATE_HASH = "UPDATE jj_logging.fault_tolerant set md5hash = ? " +
      " where docid = ? and scanner = ? ";

  static String FTI_CHECK_Q = "FTI_CHECK_Q";
  static String FTI_CHECK_DOC = "SELECT status, md5hash, error_count from jj_logging.fault_tolerant where docid = ? " + FOR_SCANNER;

  protected ScannerImpl() {
    getCassandra().addStatement(FTI_CHECK_Q, FTI_CHECK_DOC);
    getCassandra().addStatement(FIND_PROCESSING_FOR_SCANNER_Q, FIND_PROCESSING + FOR_SCANNER);
    getCassandra().addStatement(FIND_ERROR_FOR_SCANNER_Q, FIND_ERROR + FOR_SCANNER);
    getCassandra().addStatement(FIND_BATCHED_FOR_SCANNER_Q, FIND_BATCHED + FOR_SCANNER);
    getCassandra().addStatement(FIND_DIRTY_FOR_SCANNER_Q, FIND_DIRTY + FOR_SCANNER);
    getCassandra().addStatement(FIND_RESTART_FOR_SCANNER_Q, FIND_RESTART + FOR_SCANNER);
    getCassandra().addStatement(RESET_DOCS_U, RESET_DOCS);
    getCassandra().addStatement(UPDATE_HASH_U, UPDATE_HASH);
  }

  @Override
  public void activate() {
    if (isRemembering()) {
      // if we're hashing this means we try again even if the hash is the same
      // if we're not hashing this causes these documents to be attempted first
      // and even if heuristicDirty() is false
      CqlSession session = getCassandra().getSession();
      List<DocKey> strandedDocs = createList();
      addToDirtyList(session, strandedDocs, FIND_PROCESSING_FOR_SCANNER_Q);
      addToDirtyList(session, strandedDocs, FIND_ERROR_FOR_SCANNER_Q);
      addToDirtyList(session, strandedDocs, FIND_BATCHED_FOR_SCANNER_Q);

      PreparedStatement preparedQuery = getCassandra().getPreparedQuery(RESET_DOCS_U);

      BatchStatement bs = createCassandraBatch();
      List<BoundStatement> boundStatements = createListBS();
      for (DocKey docKey : strandedDocs) {
        BoundStatement statement = preparedQuery.bind(docKey.getDocid(), docKey.getScanner());
        boundStatements.add(statement);
      }
      while (boundStatements.size() > 0) {
        int maxPerBatch = Math.min(boundStatements.size(), 100);
        List<BoundStatement> batch = boundStatements.subList(0, maxPerBatch);
        log.debug("Processing batch of {}", batch.size());
        session.execute(bs.addAll(batch));
        boundStatements.removeAll(batch);
      }
    }
    // call AFTER we set up the tables :)
    superActivate();
  }

  @NotNull
  List<DocKey> createList() {
    return new ArrayList<>();
  }

  @NotNull
  List<BoundStatement> createListBS() {
    return new ArrayList<>();
  }

  @NotNull
  BatchStatement createCassandraBatch() {
    return BatchStatement.newInstance(BatchType.LOGGED);
  }

  void superActivate() {
    super.activate();
  }

  void addToDirtyList(CqlSession session, List<DocKey> strandedDocs, String query) {
    PreparedStatement preparedQuery;
    BoundStatement statement;
    preparedQuery = getCassandra().getPreparedQuery(query);
    statement = preparedQuery.bind(getName());
    ResultSet batchedRs = session.execute(statement);
    List<DocKey> batchedKeys = batchedRs.all().stream()
        .map((row) -> createKey(row.getString(0), row.getString(1))).collect(Collectors.toList());
    if (log.isInfoEnabled()) {
      log.info("Found {} {} documents to mark RESTART: {}", batchedKeys.size(), query, batchedKeys);
    }
    strandedDocs.addAll(batchedKeys);
  }

  @NotNull
  DocKey createKey(String string, String string1) {
    return new DocKey(string, string1);
  }


  @Override
  public void deactivate() {
    super.deactivate();
  }

  static class DocKey {
    private String docid;
    private String scanner;

    public DocKey(String docid, String scanner) {
      this.setDocid(docid);
      this.setScanner(scanner);
    }

    @Override
    public String toString() {
      return "DocKey{" +
          "docid='" + getDocid() + '\'' +
          ", scanner='" + getScanner() + '\'' +
          '}';
    }

    public String getDocid() {
      return docid;
    }

    public void setDocid(String docid) {
      this.docid = docid;
    }

    public String getScanner() {
      return scanner;
    }

    public void setScanner(String scanner) {
      this.scanner = scanner;
    }
  }

  public void run() {
    nanoInterval = interval * 1000000;
    Future<?> scanner = null;
    long last = System.nanoTime() - 1; // minus 1 in case we get to the next call really really fast.
    if (isActive()) {
      scanner = safeSubmit();
      last = System.nanoTime();
    }
    try {
      while (this.isActive()) {
        try {
          Thread.sleep(25);
          if (isReady() && longerAgoThanInterval(last)) {
            scanner = safeSubmit();
            last = System.nanoTime();
          }
        } catch (InterruptedException e) {
          if (scanner != null) {
            scanner.cancel(true);
          }
          log.error(e);
        }
      }
    } catch (Throwable t) {
      log.error("Exited scanner due to throwable!",t);
      throw t;
    } finally {
      log.info("Exited {}", getName());
    }
    if (scanner != null) {
      scanner.cancel(true);
    }
  }

  Future<?> safeSubmit() {
    Future<?> scanner = null;
    try {
      scanner = exec.submit(getScanOperation());
    } catch (Exception e) {
      log.error("Scan operation for {} failed.", getName());
      log.error(e);
    }
    return scanner;
  }

  boolean longerAgoThanInterval(long last) {
    return last + nanoInterval < System.nanoTime();
  }

  @Override
  public void sendToNext(Document doc) {
    if (isRemembering()) {
      CqlSession session = getCassandra().getSession();
      PreparedStatement preparedQuery = getCassandra().getPreparedQuery(UPDATE_HASH_U);
      BoundStatement bind = preparedQuery.bind(doc.getHash(), doc.getId(), doc.getSourceScannerName());
      session.execute(bind);
    }
    superSendToNext(doc);
  }

  // mockable method for unit tests
  void superSendToNext(Document doc) {
    super.sendToNext(doc);
  }

  /**
   * What to do when a document has been recognized as required for indexing.
   *
   * @param doc The document to be processed
   */
  public void docFound(Document doc) {
    log.trace("{} found doc: {}", getName(), doc.getId());
    String id = doc.getId();
    Function<String, String> idFunction = getIdFunction();
    String result = idFunction.apply(id);
    String idField = doc.getIdField();
    doc.removeAll(idField);
    doc.put(idField, result);

    id = doc.getId();
    Status status = null;
    int errorCount = 0;
    String md5 = null;
    if (isRemembering()) {
      PreparedStatement preparedQuery = getCassandra().getPreparedQuery(FTI_CHECK_Q);
      CqlSession session = getCassandra().getSession();
      ResultSet statusRs = session.execute(preparedQuery.bind(id, getName()));
      if (statusRs.getAvailableWithoutFetching() > 0) {
        if (statusRs.getAvailableWithoutFetching() > 1 || !statusRs.isFullyFetched()) {
          log.error("FATAL: duplicate primary keys in cassandra table??");
          throw new RuntimeException("VERY BAD: duplicate primary keys in FTI table?");
        } else {
          Row next = statusRs.all().iterator().next();
          String statusStr = next.getString(0);
          status = statusStr == null ? null : Status.valueOf(statusStr);
          log.trace("Found '{}' with status {}", id, status);
          if (isHashing()) {
            md5 = next.getString(1);
          }
          errorCount = next.getInt(2);
        }
      }
    }
    if (errorCount >= retryErrors && status == Status.RESTART) {
      // we've tried several times, and the document is unchanged
      // just skip it until the document changes or someone resets the status
      // in cassandra manually.
      return;
    }
    // written with negated and's so I can defer doc.getHash() until we are sure we
    // need to check the hash.
    if (isRemembering() &&                                         // easier to read, let jvm optimize this check out
        status != null &&                                          // A status was found so we have seen this before
        (status != Status.DIRTY && status != Status.RESTART) &&    // not marked dirty or restart
        !heuristicDirty(doc)                                       // not dirty by subclass logic
    ) {
      if (!isHashing()) {
        log.trace("{} ignoring previously seen document {}", getName(), id);
        return;
      }
      if (md5 != null) {
        String hash = doc.getHash();
        if (md5.equals(hash)) {
          log.trace("{} ignoring document with previously seen content {}", getName(), id);
          return;
        }
      }
    }
    sendToNext(doc);
  }

  /**
   * Scanners that have a way of detecting dirty data that needs re-indexed can overide this method to trigger
   * re-indexing.
   *
   * @param doc the document to check
   * @return true if indexing is required, false otherwise.
   */
  @SuppressWarnings("unused")
  protected boolean heuristicDirty(Document doc) {
    return false;
  }

  /**
   * The default scan operation is to check the cassandra database for records marked dirty or reestart and
   * process those records using the scanner's document fetching logic (empty by default)
   */
  @Override
  public Runnable getScanOperation() {
    return new DefaultOp();
  }

  protected void processDocsByStatus(CassandraSupport cassandra, String findForScannerQ, Object helper) {
    PreparedStatement pq = cassandra.getPreparedQuery(findForScannerQ);
    BoundStatement bs = pq.bind(getName());
    ResultSet rs = cassandra.getSession().execute(bs);
    log.info("found {} using {}", rs, findForScannerQ);
    int i=0;
    for (Row r : rs) {
      i++;
      fetchById(r.getString(0), helper).ifPresent(this::docFound);
    }
    log.info("updated {} FTI records", i);
  }

  @Override
  public long getInterval() {
    return this.interval;
  }

  @Override
  public Step[] getSubsequentSteps() {
    return new Step[0];
  }

//  @Override
//  public boolean isFinalHelper() {
//    return false;
//  }


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
  public void put(Document document) {
    throw new UnsupportedOperationException("Scanners are a push only source of documents. Queue methods are not supported for this type of step.");
  }

  @Override
  public boolean offer(Document document, long timeout, TimeUnit unit) {
    throw new UnsupportedOperationException("Scanners are a push only source of documents. Queue methods are not supported for this type of step.");
  }

  @Override
  public Document take() {
    throw new UnsupportedOperationException("Scanners are a push only source of documents. Queue methods are not supported for this type of step.");
  }

  @Override
  public Document poll(long timeout, TimeUnit unit) {
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

  public boolean isScanActive() {
    return activeScans.get() > 0;
  }

  public void scanStarted() {
    activeScans.incrementAndGet();
  }

  public void scanFinished() {
    activeScans.decrementAndGet();
  }

  public boolean isRemembering() {
    return remembering;
  }

  public boolean isHashing() {
    return hashing;
  }

  public CassandraSupport getCassandra() {
    return cassandra;
  }

  public void setCassandra(CassandraSupport cassandra) {
    this.cassandra = cassandra;
  }


  public static abstract class Builder extends StepImpl.Builder {

    public Builder() {
      // abstract class don't need instance
    }

    @Override
    public ScannerImpl.Builder batchSize(int size) {
      super.batchSize(size);
      return this;
    }

    @Override
    public ScannerImpl.Builder outputSpace(JavaSpace outputSpace) {
      super.outputSpace(outputSpace);
      return this;
    }

    @Override
    public ScannerImpl.Builder inputSpace(JavaSpace inputSpace) {
      super.inputSpace(inputSpace);
      return this;
    }

    @Override
    public ScannerImpl.Builder named(String stepName) {
      super.named(stepName);
      return this;
    }

    @Override
    public ScannerImpl.Builder routingBy(ConfiguredBuildable<? extends Router> router) {
      super.routingBy(router);
      return this;
    }

    @Override
    protected abstract ScannerImpl getObj();

    /**
     * The scanning frequency. 25ms is the minimum. Smaller intervals will be treated as 25ms
     *
     * @param interval a number of milliseconds &gt;= 25
     * @return This builder object for further configuration.
     */
    @SuppressWarnings("UnusedReturnValue")
    public ScannerImpl.Builder scanFreqMS(long interval) {
      getObj().interval = interval;
      return this;
    }

    /**
     * The number of times to retry a document that has errored out previously before ignoring it.
     *
     * @param retries the number of time to retry an erroring document before giving up
     * @return this builder for further configuration.
     */
    public ScannerImpl.Builder retryErroredDocsUpTo(int retries) {
      getObj().retryErrors = retries;
      return this;
    }

    /**
     * Turn on document Id based memory. When enabled this option will cause the scanner not to submit documents that
     * have already been indexed a second time unless they have been marked dirty, the scanner's heuristics determine
     * that the document is dirty, or hash based change detection has indicated that the document has changed.
     *
     * @param remember whether or not to track which documents have already been submitted
     * @return This builder object for further configuration
     */
    public ScannerImpl.Builder rememberScannedIds(boolean remember) {
      getObj().remembering = remember;
      return this;
    }

    /**
     * Turn on change detection via hashing. When this feature is enabled, the raw bytes of the document and the
     * backing multi-map's contents (e.g. field data from scans of data stores such as databases) will be used to
     * create an md5 hash which is stored and compared on subsequent scans. If the previous hash differs from
     * the current hash, the document will be considered dirty, and eligible for reprocessing. Note that this
     * has no effect unless {@link #rememberScannedIds(boolean)} is turned on, because without that option, all
     * documents are considered dirty every time.
     *
     * @param hash whether or not to perform hashing to track document changes
     * @return This builder object for further configuration
     */
    public ScannerImpl.Builder detectChangesViaHashing(boolean hash) {
      getObj().hashing = hash;
      return this;
    }

  }

  private class DefaultOp implements Runnable {
    @Override
    public void run() {
      CassandraSupport cassandra = ScannerImpl.this.getCassandra();
      if (cassandra == null || Cassandra.isBooting()) {
        // this is ok during unit tests, not ok otherwise
        log.error("Cassandra null or still starting for scan operation, Docs Dirty in C* skipped");
        return;
      }
      processDirtyAndRestartStatuses(cassandra, null);
    }
  }

  public abstract static class IdMangler {
    private Object helper;

    public IdMangler(Object helper) {
      this.helper = helper;
    }

    public IdMangler() {}

    abstract Object fromString(String id);
    public String fromObject(Object obj) {
      return String.valueOf(obj);
    }
  }

  /**
   * Hook for enabling calculation of an ObjectId
   *
   * @param helper an object
   * @return An object to translate Ids
   */
  protected IdMangler getIdMangler(Object helper) {
    return new IdMangler(helper) {
      @Override
      Object fromString(String id) {
        return id;
      }
    };
  }

  protected void processDirtyAndRestartStatuses(CassandraSupport cassandra, Object helper) {
    if (this.isRemembering()) {
      ScannerImpl.this.processDocsByStatus(cassandra, FIND_RESTART_FOR_SCANNER_Q, getIdMangler(helper));
      ScannerImpl.this.processDocsByStatus(cassandra, FIND_DIRTY_FOR_SCANNER_Q, getIdMangler(helper));
    }
  }
}
