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
import com.datastax.oss.driver.api.core.cql.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.jesterj.ingest.model.*;
import org.jesterj.ingest.model.Scanner;
import org.jesterj.ingest.persistence.Cassandra;
import org.jesterj.ingest.persistence.CassandraSupport;
import org.jesterj.ingest.routers.RouterBase;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;


import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.jesterj.ingest.model.Status.*;
import static org.jesterj.ingest.persistence.Cassandra.printErrors;

/**
 * A base implementation of a scanner that doesn't do anything. {@link #getScanOperation()} and
 * {@link #getDocumentTracker()} should be overridden for most implementations.
 */
public abstract class ScannerImpl extends StepImpl implements Scanner {

  private static final Logger log = LogManager.getLogger();
  public static final int DEF_MAX_ERROR_RETRY = Integer.getInteger("org.jesterj.scanner.max_error_retry", 3);
  public static final int TIMEOUT = 600;
  static final String FIND_STRANDED_DOCS = "find_stranded_docs";
  static final String FIND_ERROR_DOCS = "find_error_docs";
  static final String FIND_ERROR_HISTORY = "find_error_history";

  private boolean hashing;
  private long interval;
  boolean remembering;
  private int retryErrors = DEF_MAX_ERROR_RETRY;

  // can be used to avoid starting a scan while one is still running. This is not required however
  // and can be ignored if desired.
  // todo: consider if this is even something we want to support. Having trouble thinking of a good
  //  use case for concurrent scans that can't be serviced by creating a plan with more than one scanner
  //  could simplify checks on this and isReady() which are redundant if we don't have concurrent scans.
  @SuppressWarnings("WeakerAccess")
  protected final AtomicInteger activeScans = new AtomicInteger(0);

  private final ExecutorService exec =
      new ThreadPoolExecutor(0, 1,
          60L, TimeUnit.SECONDS,
          new SynchronousQueue<>(), r -> {
        Thread scanner = new Thread(r);
        scanner.setName("jj-scan-" + ScannerImpl.this.getName() + "-" + System.nanoTime());
        scanner.setDaemon(true);
        return scanner;
      }) {
        @NotNull
        @Override
        public Future<?> submit(@NotNull Runnable task) {
          final Runnable originalTask = task;
          return super.submit(() -> {
            try {
              ThreadContext.put(JJ_PLAN_NAME, getPlan().getName());
              ThreadContext.put(JJ_PLAN_VERSION, String.valueOf(getPlan().getVersion()));
              // leave local var for debugging
              String dsPotentSteps = Arrays.stream(getDownstreamPotentSteps())
                  .map(Configurable::getName)
                  .collect(Collectors.joining(","));
              ThreadContext.put(Step.JJ_DOWNSTREAM_POTENT_STEPS, dsPotentSteps);

              originalTask.run();

            } finally {
              ThreadContext.remove(JJ_PLAN_NAME);
              ThreadContext.remove(JJ_PLAN_VERSION);
              ThreadContext.remove(Step.JJ_DOWNSTREAM_POTENT_STEPS);
            }
          });
        }
      };

  private long nanoInterval;

  private CassandraSupport cassandra = new CassandraSupport();

  public static final String CREATE_FT_KEYSPACE =
      "CREATE KEYSPACE IF NOT EXISTS %s " +
          "WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";

  public static final String CREATE_FT_TABLE =
      "CREATE TABLE IF NOT EXISTS %s.jj_potent_step_status (" +
          "docId varchar, " + // k1
          "docHash varchar, " +
          "parentId varchar, " +
          "origParentId varchar, " +
          "potentStepName varchar, " +
          "status varchar, " +
          "message varchar, " +
          "antiCollision int, " + // C3 avoid collisions on systems with poor time resolution
          "created timestamp, " + // C1
          "createdNanos int, " + // C2 best effort for ordering ties in timestamp, just the nanos
          "PRIMARY KEY (docId, created,createdNanos,potentStepName,antiCollision)) " +
          "WITH CLUSTERING ORDER BY (created DESC, createdNanos DESC);";

  public static final String CREATE_INDEX_STATUS = "CREATE INDEX IF NOT EXISTS jj_ft_idx_step_status ON %s.jj_potent_step_status (status);";
  public static final String CREATE_INDEX_CREATED = "CREATE INDEX IF NOT EXISTS jj_ft_idx_created ON %s.jj_potent_step_status (created);";


  static final String FIND_STRANDED_STATUS =
      "SELECT docid FROM %s.jj_potent_step_status " +
          "WHERE status = ?" +
          " PER PARTITION LIMIT 1";

  static final String FIND_ERRORS =
      "SELECT docid, created FROM %s.jj_potent_step_status " +
          "WHERE status = 'ERROR' " +
          " PER PARTITION LIMIT 1";

  static final String FIND_ERROR_HIST =
      "SELECT docid, status FROM %s.jj_potent_step_status " +
          "WHERE docid = ? " +
          " PER PARTITION LIMIT ?";
  private static final String FIND_LATEST_STATUS_Q = "find_latest_status_for_doc";
  static final String FIND_LATEST_STATUS =
      "SELECT docid, created, status FROM %s.jj_potent_step_status " +
          "WHERE docId = ? " +
          "PER PARTITION LIMIT 1";

  static String FTI_CHECK_DOC_HASH_Q = "FTI_CHECK_Q";
  static String FTI_CHECK_DOC_HASH = "SELECT docHash from %s.jj_scanner_doc_hash " +
      "WHERE docid = ? " +
      "AND hashAlg=? " +
      "LIMIT 1";

  static String FTI_DOC_HASH_U = "FTI_DOC_HASH_Q";
  static String FTI_DOC_HASH = "INSERT docHash from %s.jj_scanner_doc_hash " +
      "(docId, docIdOrder, hashAlg, " +
      "created, createdNanos, antiCollision, " +
      "docHash)"+
      "VALUES(" +
          "?,?,?," +
          "?,?,?,?,?,?," +
          "?) USING TTL ?";
  private volatile boolean shutdownHasStarted;
  private boolean persistenceCreated;

  protected ScannerImpl() {}

  @Override
  public void activate() {
    try {
      addStepContext();
      shutdownHasStarted = false;
      List<String> sentAlready = new ArrayList<>();
      FTIQueryContext ctx = new FTIQueryContext(sentAlready);
      processPendingDocs(ctx, List.of(PROCESSING, BATCHED, RESTART, DIRTY));
      processErrors(ctx);
      superActivate();
    } finally {
      removeStepContext();
    }
  }

  List<DocKey> createList() {
    return new ArrayList<>();
  }


  List<BoundStatement> createListBS() {
    return new ArrayList<>();
  }


  BatchStatement createCassandraBatch() {
    return BatchStatement.newInstance(BatchType.LOGGED);
  }

  void superActivate() {
    super.activate();
  }




  @Override
  public void deactivate() {
    shutdownHasStarted = true;
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
    long last = System.nanoTime() - 1; // minus 1 in case we get to the next call really fast.
    if (isActive()) {
      scanner =   safeSubmit();
      last = System.nanoTime();
    }
    try {
      while (this.isActive()) {
        try {
          boolean timeForNextScan = longerAgoThanInterval(last);
          if (!isScanning() && timeForNextScan) {
            scanner = safeSubmit();
            last = System.nanoTime();
          }
          //noinspection BusyWait
          Thread.sleep(25);
        } catch (InterruptedException e) {
          if (scanner != null) {
            scanner.cancel(true);
          }
          log.error(e);
        }
      }
    } catch (Throwable t) {
      log.error("Exited scanner due to throwable!", t);
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
    Plan plan = getPlan();
    String scannerName = getName();
    log.trace("{} found doc: {}", scannerName, doc.getId());
    String id = doc.getId();
    Function<String, String> idFunction = getIdFunction();
    String result = idFunction.apply(id);
    String idField = doc.getIdField();
    doc.removeAll(idField);
    doc.put(idField, result);

    CqlSession session = getCassandra().getSession();
    int planVersion = plan.getVersion();
    String planName = plan.getName();
    id = doc.getId();

    boolean shouldIndex = true;

    if (isRemembering()) {
      log.trace("We are remembering");
      // if we are hashing and the content has changed we always reindex.
      shouldIndex = isHashing() && checkDocContent(doc, scannerName, id, session, planVersion, planName);
      // if we are not hashing then we only index if we have never seen this doc
      shouldIndex |= !isHashing() && !seenPreviously(scannerName, id, session);

      shouldIndex |= doc.isForceReprocess();

    } else {
      log.trace("Not Remembering");
    }
    if (shouldIndex) {
      log.trace("Memory complete");
      sendToNext(doc);
    }
  }

  private boolean seenPreviously(String scannerName, String id, CqlSession session) {
    String actualQuery = String.format(FIND_LATEST_STATUS, keySpace());
    PreparedStatement seenDocQuery = getCassandra().getPreparedQuery(FIND_LATEST_STATUS_Q, actualQuery);
    BoundStatement bs = seenDocQuery.bind( id);
    ResultSet lastStatus = session.execute(bs);
    if (lastStatus.getAvailableWithoutFetching() > 0) {
      log.trace("{} ignoring document previously seen {}", scannerName, id);
      return true;
    }
    return false;
  }

  private boolean checkDocContent(Document doc, String scannerName, String id, CqlSession session, int planVersion, String planName) {
    String prevHash = findPreviousHash(doc, scannerName, id, session, planVersion, planName);
    if (doc.getHash().equals(prevHash)) {
      log.trace("{} ignoring document with previously seen content {}", scannerName, id);
      return false;
    } else {
      updateHash(doc, scannerName, session);
      return true;
    }
  }

  private void updateHash(Document doc, String scannerName, CqlSession session) {
    String actualQuery = String.format(FTI_DOC_HASH, keySpace());
    PreparedStatement updateHash = getCassandra().getPreparedQuery(FTI_DOC_HASH_U,actualQuery);
    BoundStatement bs = updateHash.bind(doc.getId(), doc.getHashAlg(), scannerName,
        getPlan().getName(), getPlan().getVersion(),
        Instant.now(), (int) (System.nanoTime() % 1_000_000),
        CassandraSupport.antiCollision.get().nextInt());
    session.execute(bs);
  }

  @Nullable
  private String findPreviousHash(Document doc, String scannerName, String id, CqlSession session, int planVersion, String planName) {
    log.trace("We are using hashing to detect new versions");
    String actualQuery = String.format(FTI_CHECK_DOC_HASH,keySpace());
    PreparedStatement preparedQuery = getCassandra().getPreparedQuery(FTI_CHECK_DOC_HASH_Q,actualQuery);

    BoundStatement bind = preparedQuery.bind(id, doc.getHashAlg(), scannerName, planName, planVersion);
    ResultSet statusRs = session.execute(bind);
    printErrors(statusRs);
    String previousHash = null;
    if (statusRs.getAvailableWithoutFetching() > 0) {
      Row next = statusRs.all().iterator().next();
      previousHash = next.getString(1);
      log.trace("Found '{}' with hash {}", id, previousHash);
    }
    return previousHash;
  }

  /**
   * Scanners that have a way of detecting dirty data that needs re-indexed can override this method to trigger
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
   * The default scan operation is to check the cassandra database for records marked dirty or restart and
   * process those records using the scanner's document fetching logic (empty by default)
   */
  @Override
  public abstract ScanOp getScanOperation();


  protected void processPendingDocs(FTIQueryContext ftiQueryContext, List<Status> statusesToProcess) {
    boolean activeAtStart = isActive();
    if (this.isShutdown()) {
      return;
    }
    ensurePersistence();
    List<String> sentAlready = ftiQueryContext.getSentAlready();
    BoundStatement bs;
    PreparedStatement pq;
    ResultSet rs;
    int i = 0;

    // Sadly to avoid allow filtering we have to iterate here instead of just using a single IN()
    for (Status status : statusesToProcess) {
      String actualQuery = String.format(FIND_STRANDED_STATUS, keySpace());
      pq = cassandra.getPreparedQuery(FIND_STRANDED_DOCS, actualQuery);
      bs = pq.bind( String.valueOf(status));
      bs = bs.setTimeout(Duration.ofSeconds(TIMEOUT));
      rs = cassandra.getSession().execute(bs);
      log.info("found {} using {}", rs, actualQuery);

      for (Row r : rs) {
        if (isShutdown() || !isActive() && activeAtStart) {
          // shutdown was initiated during processing.
          break;
        }
        i++;
        String id = r.getString(0);
        sentAlready.add(id);
        // here we handle everything that did not error out; anything that is
        fetchById(id).ifPresentOrElse(((d) ->{d.setForceReprocess(status != DIRTY);docFound(d);}),
            () -> log.error("Unable to load previously scanned (stranded) document {}",id));
      }
    }
    log.info("Found and restarted processing for {} FTI records", i);
  }

  private void ensurePersistence() {
    if (!this.persistenceCreated) {
      // no need for synchronization should ony be one thread, and if exists is safe anyway.
      cassandra.getSession().execute(String.format(CREATE_FT_KEYSPACE, keySpace()));
      cassandra.getSession().execute(String.format(CREATE_FT_TABLE, keySpace()));
      cassandra.getSession().execute(String.format(CREATE_INDEX_STATUS, keySpace()));
      cassandra.getSession().execute(String.format(CREATE_INDEX_CREATED, keySpace()));
      this.persistenceCreated = true;
    }
  }

  private void processErrors(FTIQueryContext scanContext) {
    ResultSet rs;
    PreparedStatement pq;
    BoundStatement bs;
    String actualQuery = String.format(FIND_ERRORS, keySpace());
    pq = cassandra.getPreparedQuery(FIND_ERROR_DOCS, actualQuery);
    bs = pq.bind();
    bs = bs.setTimeout(Duration.ofSeconds(TIMEOUT));
    rs = cassandra.getSession().execute(bs);
    for (Row r : rs) {
      if (!isActive()) {
        break;
      }
      String id = r.getString(0);
      if (scanContext.getSentAlready().contains(id)) {
        log.trace("Skipping error for document already submitted during this FTI processing round");
        continue;
      }
      log.trace("Found Errored document:{}",id);
      String findErrorHistory = String.format(FIND_ERROR_HIST,keySpace());
      PreparedStatement pq2 = cassandra.getPreparedQuery(FIND_ERROR_HISTORY, findErrorHistory);
      ResultSet hist = cassandra.getSession().execute(pq2.bind(id, retryErrors));

      // In most cases the first row is an error because that's how we got a row in the previous
      // query that has partition limit = 1, but concurrency could bite us, so we double-check...
      int errorCount = 0;
      boolean firstRow = true;
      boolean errorMostRecent = true;
      boolean alreadyDropped = false;
      for (Row row : hist) {
        String status = row.getString(1);
        log.trace("Observing status of {} for {}", status, id);
        switch(Status.valueOf(status)){
          case ERROR:
            errorCount++;
            break;
          case DROPPED:
            if (firstRow) {
              alreadyDropped = true;
            }
            //// fall through intentional ////
          default:
            if (firstRow) {
              errorMostRecent = false;
            }
        }
        firstRow = false;
        if (!errorMostRecent) {
          break;
        }
      }
      if (errorMostRecent && errorCount < retryErrors) {
        log.trace("Re-feeding errored document {}", id);
        scanContext.getSentAlready().add(id);
        fetchById(id).ifPresentOrElse((d) -> {
          d.setForceReprocess(true); // ignore the fact that the content hasn't changed if we are hashing.
          docFound(d);
        }, () -> log.error("{} could not be fetched by id for error retry!", id));
      } else {
        if (!alreadyDropped) {
          log.warn("Dropping document {} due to too many error retries", id);
          fetchById(id).ifPresentOrElse(d-> d.reportDocStatus(DEAD, "Retry limit of {} exceeded",retryErrors),
              () -> log.error("{} could not be fetched by id when attempting to mark it dead for repeated retries!",id));
        } else {
          //noinspection ConstantValue
          log.trace("Ignoring {} because errorMostRecent = {} and errorCount = {}", id, errorMostRecent,errorCount);
        }
      }
    }
  }

  private boolean isShutdown() {
    return shutdownHasStarted;
  }

  @Override
  public long getInterval() {
    return this.interval;
  }

  @Override
  public boolean isActivePriorSteps() {
    return false;
  }

  @Override
  public void addPredecessor(StepImpl obj) {
    throw new UnsupportedOperationException("Scanners cannot have predecessors");
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
  protected Logger getLogger() {
    return log;
  }

  public boolean isScanActive() {
    return activeScans.get() > 0;
  }

  public void scanStarted() {
    activeScans.incrementAndGet();
  }

  /**
   * Decrement the active Scans. While it's possible to do more in an overridden version this method be very
   * careful since it runs in a finally block after the step has been deactivated.
   */
  public void scanFinished() {
    activeScans.decrementAndGet();
  }

  @Override
  public boolean isRemembering() {
    return remembering;
  }

  @Override
  public boolean isHashing() {
    return hashing;
  }

  public CassandraSupport getCassandra() {
    return cassandra;
  }

  public void setCassandra(CassandraSupport cassandra) {
    this.cassandra = cassandra;
  }

  @Override
  public String keySpace() {
    // note plans and scanners already enforce sane names, no injection worries. (unless that changes)
    return "jj_" + getName() + "_" + getPlan().getName() + "_" + getPlan().getVersion();
  }

  public static abstract class Builder extends StepImpl.Builder {

    public Builder() {
      // abstract classes don't need an instance
    }

    @Override
    public ScannerImpl.Builder batchSize(int size) {
      super.batchSize(size);
      return this;
    }


    @Override
    public ScannerImpl.Builder named(String stepName) {
      super.named(stepName);
      return this;
    }

    @Override
    public ScannerImpl.Builder routingBy(RouterBase.Builder<? extends Router> router) {
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
    @SuppressWarnings("unused")
    public ScannerImpl.Builder retryErroredDocsUpTo(int retries) {
      getObj().retryErrors = retries;
      return this;
    }

    /**
     * Turn on document id based memory. When enabled this option will cause the scanner not to submit documents that
     * have already been indexed a second time unless they have been marked dirty, the scanner's heuristics determine
     * that the document is dirty, or hash based change detection has indicated that the document has changed.
     *
     * @param remember whether to track which documents have already been submitted
     * @return This builder object for further configuration
     */
    public ScannerImpl.Builder rememberScannedIds(boolean remember) {
      getObj().remembering = remember;
      return this;
    }

    /**
     * Turn on change detection via hashing. When this feature is enabled, the raw bytes of the document and the
     * backing multimap's contents (e.g. field data from scans of data stores such as databases) will be used to
     * create an md5 hash which is stored and compared on subsequent scans. If the previous hash differs from
     * the current hash, the document will be considered dirty, and eligible for reprocessing. Note that this
     * has no effect unless {@link #rememberScannedIds(boolean)} is turned on, because without that option, all
     * documents are considered dirty every time.
     *
     * @param hash whether or not to perform hashing to track document changes
     * @return This builder object for further configuration
     */
    @SuppressWarnings({"unused", "GrazieInspection"})
    public ScannerImpl.Builder detectChangesViaHashing(boolean hash) {
      getObj().hashing = hash;
      return this;
    }

  }

  /**
   * The base, default scan operation. Scanners may wish to provide their own implementation.
   */
  public class ScanOp implements Runnable {
    private final Runnable custom;
    private final Scanner scanner;

    public ScanOp(Runnable custom, Scanner scanner) {
      this.custom = custom;
      this.scanner = scanner;
    }

    @Override
    public void run() {
      CassandraSupport cassandra = ScannerImpl.this.getCassandra();
      if ( scanner.isRemembering() && (cassandra == null || Cassandra.isBooting()) ) {
        log.error("Cassandra null or still starting for scan operation, Invocation skipped");
        return;
      }
      try {
        if (isScanActive()) {
          log.info("Skipping scan, there is already an active scan");
          return;
        } else  {
          log.info("{} of plan {} Starting scan of at {} on {}", scanner.getName(),getPlan().getName(), new Date(), Thread.currentThread().getName());
        }
        // set up our watcher if needed
        scanStarted();
        processDirty();
        custom.run();
      } catch (Exception e) {
        if (Thread.interrupted()) {
          scanner.deactivate();
        }
        log.error("Exception while processing files!", e);
      } finally {
        scanFinished();
      }
    }
  }


  protected void processDirty() {
    if (this.isRemembering()) {
      List<String> sentAlready = new ArrayList<>();
      FTIQueryContext ftiQueryContext = new FTIQueryContext(sentAlready);
      processPendingDocs(ftiQueryContext, List.of(DIRTY));
      processErrors(ftiQueryContext);
    }
  }
}
