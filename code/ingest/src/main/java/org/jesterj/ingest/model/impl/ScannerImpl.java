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
import org.apache.commons.codec.binary.Hex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.jesterj.ingest.model.Scanner;
import org.jesterj.ingest.model.*;
import org.jesterj.ingest.persistence.Cassandra;
import org.jesterj.ingest.persistence.CassandraSupport;
import org.jesterj.ingest.routers.RouterBase;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.jesterj.ingest.logging.JesterJAppender.FTI_TTL;
import static org.jesterj.ingest.model.Status.*;
import static org.jesterj.ingest.persistence.Cassandra.printErrors;

/**
 * A base implementation of a scanner that doesn't do anything. {@link #getScanOperation()} and
 * {@link #getDocumentTracker()} should be overridden for most implementations.
 */
public abstract class ScannerImpl extends StepImpl implements Scanner {

  public static final String SCAN_ORIGIN = "SCAN";
  public static final String FTI_ORIGIN = "FTI";

  private static final Logger log = LogManager.getLogger();
  public static final int DEF_MAX_ERROR_RETRY = Integer.getInteger("org.jesterj.scanner.max_error_retry", 3);
  public static final int TIMEOUT = 600;
  static final String FIND_STRANDED_DOCS = "find_stranded_docs";
  static final String FIND_ERROR_DOCS = "find_error_docs";
  static final String FIND_HISTORY = "find_error_history";
  public static final String NEW_CONTENT_FOUND_MSG = "New content found by {}.";
  public static final int DDL_TIMEOUT = 30;

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

              originalTask.run();

            } catch (Throwable t) {
              t.printStackTrace();
            } finally {
              ThreadContext.remove(JJ_PLAN_NAME);
              ThreadContext.remove(JJ_PLAN_VERSION);
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
      "CREATE TABLE IF NOT EXISTS %s.jj_output_step_status (" +
          "docId varchar, " + // k1
          "docHash varchar, " +
          "parentId varchar, " +
          "origParentId varchar, " +
          "outputStepName varchar, " +
          "status varchar, " +
          "message varchar, " +
          "antiCollision int, " + // C3 avoid collisions on systems with poor time resolution
          "created timestamp, " + // C1
          "createdNanos int, " + // C2 best effort for ordering ties in timestamp, just the nanos
          "PRIMARY KEY (docId, created,createdNanos,outputStepName,antiCollision)) " +
          "WITH CLUSTERING ORDER BY (created DESC, createdNanos DESC);";

  public static final String CREATE_INDEX_STATUS =
      "CREATE INDEX IF NOT EXISTS jj_ft_idx_step_status ON %s.jj_output_step_status (status);";

  public static final String CREATE_DOC_HASH =
      "CREATE TABLE IF NOT EXISTS %s.jj_scanner_doc_hash (" +
          "docId varchar, " +       // k1
          "created timestamp, " +   // C1
          "createdNanos int, " +    // C2 best effort for ordering ties in timestamp, just the nanos
          "antiCollision int, " +   // C3 avoid collisions on systems with poor time resolution
          "hashAlg varchar, " +     //
          "docHash varchar, " +
          "PRIMARY KEY ((docId),created,createdNanos,antiCollision)) " +
          "WITH CLUSTERING ORDER BY (created DESC, createdNanos DESC);";

  static final String FIND_STRANDED_STATUS =
      "SELECT docid FROM %s.jj_output_step_status " +
          "WHERE status = ?" +
          " PER PARTITION LIMIT 1";

  static final String FIND_ERRORS =
      "SELECT docid, created FROM %s.jj_output_step_status " +
          "WHERE status = 'ERROR' " +
          " PER PARTITION LIMIT 1";

  static final String FIND_HIST =
      "SELECT docid, status, created FROM %s.jj_output_step_status " +
          "WHERE docid = ? " +
          " PER PARTITION LIMIT ?";
  private static final String FIND_LATEST_STATUS_Q = "find_latest_status_for_doc";
  static final String FIND_LATEST_STATUS =
      "SELECT docid, created, status FROM %s.jj_output_step_status " +
          "WHERE docId = ? " +
          "PER PARTITION LIMIT 1";

  static String FTI_CHECK_DOC_HASH_Q = "FTI_CHECK_Q";
  static String FTI_CHECK_DOC_HASH = "SELECT docHash from %s.jj_scanner_doc_hash " +
      "WHERE docid = ? " +
      "LIMIT 1";

  static String FTI_DOC_HASH_U = "FTI_DOC_HASH_Q";
  static String FTI_DOC_HASH = "INSERT into %s.jj_scanner_doc_hash " +
      "(docId, created, createdNanos, antiCollision, " +
      "hashAlg, docHash)" +
      "VALUES(" +
      "?,?,?,?," +
      "?,?) USING TTL ?";
  private volatile boolean shutdownHasStarted;
  private boolean persistenceCreated;
  private final Map<String, String> keySpaces = new ConcurrentHashMap<>();

  protected ScannerImpl() {
  }

  @Override
  public void activate() {
    try {
      addStepContext();
      shutdownHasStarted = false;
      Set<String> sentAlready = new HashSet<>();
      FTIQueryContext ctx = new FTIQueryContext(sentAlready);

      // on restart these statuses indicate items that failed in flight. NOTE this is the only
      // time we pick up "processing" and there will be some work to do here on this logic
      // when we get to supporting multiple cooperating nodes. (particularly we may need to mark
      // events with the node name as well (or an additional table to look things up by node name)
      processPendingDocs(ctx, List.of(FORCE, RESTART, PROCESSING, BATCHED), true);
      processErrors(ctx);

      // Dirty items were ready to be processed but had not been started yet so they should not
      // be forced
      processPendingDocs(ctx, List.of(DIRTY), false);
      superActivate();
    } finally {
      removeStepContext();
    }
  }

  void superActivate() {
    super.activate();
  }

  @Override
  public void deactivate() {
    shutdownHasStarted = true;
    super.deactivate();
  }

  public void run() {
    nanoInterval = interval * 1_000_000;
    Future<?> scanner = null;
    long last = System.nanoTime() - 1; // minus 1 in case we get to the next call really fast.
    if (isActive()) {
      scanner = safeSubmit();
      last = System.nanoTime();
    }
    try {
      while (this.isActive()) {
        try {
          boolean timeForNextScan = longerAgoThanInterval(last);
          boolean scanning = isScanning();
          long now = System.nanoTime();
          log.trace("scanning:{} timeForNext:{} now:{}  - (last:{} + nanoInt:{}) = {}",scanning,timeForNextScan, now, last, nanoInterval, now - (last + nanoInterval));
          if (!scanning && timeForNextScan) {
            scanner = safeSubmit();
            last = System.nanoTime();
          } else {
            log.trace("{}:Scan skipped, still scanning:{}; msSinceLast:{}", getName(), scanning, msSinceNanoTime(last));
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
    Instant now = Instant.now();
    long start = System.nanoTime();
    try {
      log.trace("Submitting scan for {} (Scan interval = {} ms)", getName(), getInterval());
      scanner = exec.submit(getScanOperation());
    } catch (Exception e) {
      log.error("Scan operation for {} failed.", getName());
      log.error(e);
      e.printStackTrace();
    } finally {
      log.trace("Scan Submitted for {} (Scan interval = {} ms), started at {}, elapsed:{}", getName(), getInterval(), now, msSinceNanoTime(start));
    }
    return scanner;
  }

  private static long msSinceNanoTime(long start) {
    return System.nanoTime() - start / 1_000_000;
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
   * @return true if the found document should be indexed, false if it should be ignored
   */
  public boolean docFound(Document doc) {
    ((DocumentImpl) doc).stepStarted(this);
    String scannerName = getName();
    log.trace("{} found doc: {}", scannerName, doc.getId());
    doc.setStatus(PROCESSING, "{} found doc:{}", scannerName, doc.getId());
    String id = doc.getId();
    Function<String, String> idFunction = getIdFunction();
    String result = idFunction.apply(id);
    String idField = doc.getIdField();
    doc.removeAll(idField);
    doc.put(idField, result);

    boolean shouldIndex = doc.isForceReprocess();

    if (isRemembering() & !shouldIndex) {
      id = doc.getId();
      CqlSession session = getCassandra().getSession();
      Set<String> outputDestinationNames = getOutputDestinationNames();
      List<String> downstreamOutputSteps = new ArrayList<>(outputDestinationNames);
      for (int i = 0; i < downstreamOutputSteps.size() && !shouldIndex; i++) {
        Status status = doc.getStatus(downstreamOutputSteps.get(i));
        // Typically these statuses already have forceReprocess set, but just in case.
        if (status == FORCE || status == RESTART) {
          shouldIndex = true;
          break;
        }
        // if we are hashing and the content has changed we always reindex.
        // if we are not hashing then we only index if we have never seen this doc
        if (isHashing()) {
          shouldIndex = isFreshContent(doc, scannerName, id, session);
        } else {
          shouldIndex = !seenPreviously(scannerName, id, session);
        }
      }
    } else {
      shouldIndex = true;
      log.trace("Not Remembering");
    }

    shouldIndex = shouldIndex || isHeuristicallyDirty(doc);

    log.trace("Memory complete");
    if (shouldIndex) {
      log.trace("Need to index {}", id);
      // if we get here and steps have not been previously determined, then we have
      // found a new version of a doc via a scan and all steps are eligible. This case
      // would be universally true if not remembering so short circuit that with a simple boolean check.
      if (!isRemembering() || !doc.alreadyHasIncompleteStepList()) {
        // This was a document found by the scanner during a scan and either we are not hashing, not remembering
        // or a new hash (i.e. new content) was found, so ALL downstream steps should be eligible
        ((DocumentImpl) doc).initDestinations(getOutputDestinationNames(), getName());
      }
      sendToNext(doc);
    } else {
      log.trace("Did not need to index {}", id);
    }
    return shouldIndex;
  }

  boolean seenPreviously(String scannerName, String id, CqlSession session) {
    // never legal for getOutputDestinationNames to have no elements...
    String anyStep = getOutputDestinationNames().iterator().next();
    String keySpace = keySpace(anyStep);
    String actualQuery = String.format(FIND_LATEST_STATUS, keySpace);
    PreparedStatement seenDocQuery = getCassandra().getPreparedQuery(FIND_LATEST_STATUS_Q + "_" + keySpace(anyStep), actualQuery);
    BoundStatement bs = seenDocQuery.bind(id);
    ResultSet lastStatus = session.execute(bs);
    if (lastStatus.getAvailableWithoutFetching() > 0) {
      log.trace("{} ignoring document previously seen {}", scannerName, id);
      return true;
    }
    return false;
  }

  boolean isFreshContent(Document doc, String scannerName, String id, CqlSession session) {
    String prevHash = findPreviousHash(doc, id, session);
    if (doc.getHash().equals(prevHash)) {
      log.trace("{} ignoring document with previously seen content {}", scannerName, id);
      return false;
    } else {
      updateHash(doc, session);
      return true;
    }
  }

  private void updateHash(Document doc, CqlSession session) {
    // doc hashing only needs to be determined once per scanner, not for every down stream step
    String actualQuery = String.format(FTI_DOC_HASH, keySpace(null));
    PreparedStatement updateHash = getCassandra().getPreparedQuery(FTI_DOC_HASH_U + "_" + keySpace(null), actualQuery);
    BoundStatement bs = updateHash.bind(doc.getId(),
        Instant.now(), (int) (System.nanoTime() % 1_000_000),
        CassandraSupport.antiCollision.get().nextInt(), doc.getHashAlg(), doc.getHash(), FTI_TTL);
    session.execute(bs);
  }

  @Nullable
  private String findPreviousHash(Document doc, String id, CqlSession session) {
    log.trace("We are using hashing to detect new versions");
    String actualQuery = String.format(FTI_CHECK_DOC_HASH, keySpace(null));
    PreparedStatement preparedQuery = getCassandra().getPreparedQuery(FTI_CHECK_DOC_HASH_Q + "_" + keySpace(null), actualQuery);

    BoundStatement bind = preparedQuery.bind(id);
    ResultSet statusRs = session.execute(bind);
    printErrors(statusRs);
    String previousHash = null;
    if (statusRs.getAvailableWithoutFetching() > 0) {
      Row next = statusRs.all().iterator().next();
      previousHash = next.getString(0);
      log.trace("Found '{}' with hash {}, current hash is {}", id, previousHash, doc.getHash());
    }
    return previousHash;
  }

  protected void setInterval(long interval) {
    this.interval = interval;
  }


  @Override
  @SuppressWarnings("unused")
  public boolean isHeuristicallyDirty(Document doc) {
    return false;
  }

  /**
   * The default scan operation is to check the cassandra database for records marked dirty or restart and
   * process those records using the scanner's document fetching logic (empty by default)
   */
  @Override
  public abstract ScanOp getScanOperation();


  /**
   * Force processing of documents in the specified status (except Dirty which will receive normal hash
   * and memory checks) Note: this method scales O(n) with the number of documents returned for each status
   * processed. In JesterJ all documents should eventually end up in terminal statuses (INDEXED,DEAD,DROPPED,SEARCHABLE)
   * It is very dangerous to pass in any terminal status because then N is the size of the entire corpus, whereas the
   * transient statuses will relate only to "in flight" documents. Thus, so long as plans don't cause an accumulation
   * of never resolving transients, the FTI system will scale dependent on the number of inflight documents rather
   * primarily, and secondarily as cassandra scales vs the number of events seen during the TTL period. Furthermore,
   * that scaling will only relate to the scanning for FTI documents, and primary processing should be write-only
   * and bound only by cassandra's write behavior. That's the theory at least :)
   *
   * @param ftiQueryContext   An object providing some context for the FTI queries
   * @param statusesToProcess The list of statuses that we want to reprocess.
   * @param force             determines if the document produced should set {@link Document#setForceReprocess(boolean)} to true
   */
  protected void processPendingDocs(FTIQueryContext ftiQueryContext, List<Status> statusesToProcess, boolean force) {
    boolean activeAtStart = isActive();
    if (this.isShutdown()) {
      return;
    }
    ensurePersistence();
    Set<String> sentAlready = ftiQueryContext.getSentAlready();
    BoundStatement bs;
    PreparedStatement pq;
    ResultSet rs;
    int i = 0;

    // Sadly to avoid allow filtering we have to iterate here instead of just using a single IN()
    CassandraSupport cStar = getCassandra();
    CqlSession session = cStar.getSession();
    Map<String, Set<LatestStatus>> needToProcess = new HashMap<>();
    Map<String, LatestStatus> statusCheckCache = new HashMap<>();
    for (String stepName : getOutputDestinationNames()) {
      String keySpace = keySpace(stepName);
      for (Status status : statusesToProcess) {
        String actualQuery = String.format(FIND_STRANDED_STATUS, keySpace);
        pq = cStar.getPreparedQuery(FIND_STRANDED_DOCS + "_" + keySpace, actualQuery);
        bs = pq.bind(String.valueOf(status));
        bs = bs.setTimeout(Duration.ofSeconds(TIMEOUT));
        rs = session.execute(bs);
        log.trace("found {} using {}", rs, actualQuery);

        for (Row r : rs) {
          if (isShutdown() || !isActive() && activeAtStart) {
            // shutdown was initiated during processing.
            break;
          }
          String id = r.getString(0);
          LatestStatus latestStatus = findLatestSatus(actualQuery, id, stepName, statusCheckCache);
          if (status.toString().equals(latestStatus.getStatus())) {
            log.trace("{} found for reprocessing with status={}", id, status);
            needToProcess.computeIfAbsent(id, (docid) -> new HashSet<>()).add(latestStatus);
          } else {
            log.trace("{} not processed for status of {}, latest status is {}", id, status, latestStatus);
          }
        }
      }
    }
    // if there are no statuses that require processing, we skip this loop and the doc is not sent.
    for (Map.Entry<String, Set<LatestStatus>> toProcess : needToProcess.entrySet()) {
      process(force, sentAlready, toProcess, FTI_ORIGIN);
      i++;
    }
    log.info("Found and restarted processing for {} FTI records", i);
  }


  void process(boolean force, Set<String> sentAlready, Map.Entry<String, Set<LatestStatus>> toProcess, String origination) {
    String docId = toProcess.getKey();
    if (sentAlready != null) {
      sentAlready.add(docId);
    }
    fetchById(docId, origination).ifPresentOrElse((d) -> {
          d.setForceReprocess(force);
          Set<LatestStatus> statuses = toProcess.getValue();
          Map<String, DocDestinationStatus> downstream = new HashMap<>();
          statuses.forEach((status) -> downstream.put(status.getoutputStepName(),
              new DocDestinationStatus(PROCESSING, status.getoutputStepName(),
                  "Prior status:" + status.getoutputStepName() + ">" + status.getStatus() + "@" + status.getTimestamp()
              )));
          d.setIncompleteOutputDestinations(downstream);
          docFound(d);
        },
        () -> log.error("Unable to load previously scanned (stranded) document {}", docId));
  }

  static class LatestStatus {
    private final String status;
    private final String timestamp;

    private final String outputStepName;

    LatestStatus(String status, String timestamp, String outputStepName) {
      this.status = status;
      this.timestamp = timestamp;
      this.outputStepName = outputStepName;
    }

    @Override
    public String toString() {
      return "LatestStatus{" +
          "status='" + getStatus() + '\'' +
          ", timestamp='" + getTimestamp() + '\'' +
          ", outputStepName='" + getoutputStepName() + '\'' +
          '}';
    }

    public String getStatus() {
      return status;
    }

    public String getTimestamp() {
      return timestamp;
    }

    public String getoutputStepName() {
      return outputStepName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      LatestStatus that = (LatestStatus) o;
      return Objects.equals(status, that.status) && Objects.equals(timestamp, that.timestamp) && Objects.equals(outputStepName, that.outputStepName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(status, timestamp, outputStepName);
    }
  }

  LatestStatus findLatestSatus(String priorQuery, String docId, String outputStepName, Map<String, LatestStatus> cache) {
    if (cache.containsKey(docId + outputStepName)) {
      return cache.get(docId + outputStepName);
    }
    String histQuery = String.format(FIND_HIST, keySpace(outputStepName));
    PreparedStatement phq = getCassandra().getPreparedQuery(FIND_HISTORY + "_" + keySpace(outputStepName), histQuery);
    BoundStatement bhq = phq.bind(docId, 1);
    ResultSet histRS = getCassandra().getSession().execute(bhq);
    Row one = histRS.one();
    LatestStatus latestStatus;
    if (one == null) {
      log.error("{} appeared in {} but not in {}", docId, priorQuery, histQuery);
      latestStatus = new LatestStatus("NO PRIOR STATUS FOUND", Instant.now().toString(), outputStepName);
    } else {
      latestStatus = new LatestStatus(one.getString(1), String.valueOf(one.getInstant(2)), outputStepName);
    }
    cache.put(docId + outputStepName, latestStatus);
    return latestStatus;
  }

  void ensurePersistence() {
    if (!this.persistenceCreated) {
      // no need for synchronization should ony be one thread, and if exists is safe anyway.
      CqlSession session = cassandra.getSession();

      for (String name : getOutputDestinationNames()) {
        executeWithTimoutSecs(session, CREATE_FT_KEYSPACE, name, DDL_TIMEOUT);
        executeWithTimoutSecs(session, CREATE_FT_TABLE, name, DDL_TIMEOUT);
        executeWithTimoutSecs(session, CREATE_INDEX_STATUS, name, DDL_TIMEOUT);
      }
      executeWithTimoutSecs(session, CREATE_FT_KEYSPACE, null, DDL_TIMEOUT);
      executeWithTimoutSecs(session, CREATE_DOC_HASH, null, DDL_TIMEOUT);
      this.persistenceCreated = true;
    }
  }

  @SuppressWarnings("SameParameterValue")
  private void executeWithTimoutSecs(CqlSession session, String cqlTemplate, String destinationName, int seconds) {
    String format = String.format(cqlTemplate, keySpace(destinationName));
    SimpleStatement simpleStatement = SimpleStatement.builder(format).setTimeout(Duration.of(seconds, ChronoUnit.SECONDS)).build();
    session.execute(simpleStatement);
  }

  void processErrors(FTIQueryContext scanContext) {
    log.info("Checking for Errored docs");
    Set<DocumentImpl> deadDocs = new HashSet<>();
    Map<String, Set<LatestStatus>> forceReprocess = new HashMap<>();
    for (String outputStepName : getOutputDestinationNames()) {
      ResultSet rs;
      PreparedStatement pq;
      BoundStatement bs;
      String actualQuery = String.format(FIND_ERRORS, keySpace(outputStepName));
      pq = getCassandra().getPreparedQuery(FIND_ERROR_DOCS + "_" + keySpace(outputStepName), actualQuery);
      bs = pq.bind();
      bs = bs.setTimeout(Duration.ofSeconds(TIMEOUT));
      rs = getCassandra().getSession().execute(bs);
      for (Row r : rs) {
        if (!isActive()) {
          break;
        }
        String id = r.getString(0);

        if (scanContext.getSentAlready().contains(id)) {
          log.trace("Skipping error for document already submitted during this FTI processing round");
          continue;
        }
        log.trace("Found Errored document:{}", id);
        String findErrorHistory = String.format(FIND_HIST, keySpace(outputStepName));
        PreparedStatement pq2 = cassandra.getPreparedQuery(FIND_HISTORY + "_" + keySpace(outputStepName), findErrorHistory);
        // Need to allow for a PROCESSING event between each error
        ResultSet hist = cassandra.getSession().execute(pq2.bind(id, 2 * retryErrors - 1));

        // In most cases the first row is an error because that's how we got a row in the previous
        // query that has partition limit = 1, but concurrency could bite us, so we double-check...
        int errorCount = 0;
        boolean firstRow = true;
        boolean errorMostRecent = true;
        boolean alreadyDropped = false;
        Instant mostRecent = null;

        // This becomes the vehicle for marking a document dead if necessary (without actually loading it from disk/db/whatever)
        DocumentImpl tempDoc = new DocumentImpl(new byte[]{}, id, this.getPlan(), Document.Operation.UPDATE, this, FTI_ORIGIN);

        int loop = 0;
        for (Row row : hist) {
          String status = row.getString(1);
          if (mostRecent == null) {
            mostRecent = row.getInstant(2);
          }
          log.trace("Observing status of {} for {} on row {}", status, id, ++loop);
          switch (Status.valueOf(status)) {
            case ERROR:
              errorCount++;
              break;
            case PROCESSING: if (!firstRow) break;
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
          log.trace("after switch:{}", status);
          firstRow = false;
          if (!errorMostRecent) {
            break;
          }
        }
        log.trace("ERROR COUNT {} of {} for {}", errorCount, retryErrors, id);
        if (errorMostRecent && errorCount < retryErrors) {
          log.info("Re-feeding errored document {}", id);
          LatestStatus latestStatus = new LatestStatus(ERROR.toString(), String.valueOf(mostRecent), outputStepName);
          forceReprocess.computeIfAbsent(id, (k) -> new HashSet<>()).add(latestStatus);
        } else {
          if (!alreadyDropped && errorCount >= retryErrors) {
            log.warn("Marking document dead id={} due to too many error retries ({})", id, errorCount);
            tempDoc.initDestinations(Set.of(outputStepName),getName());
            tempDoc.setStatus(DEAD, "Retry limit of {} exceeded", retryErrors);
            tempDoc.stepStarted(this);
            deadDocs.add(tempDoc);
          } else {
            log.trace("Ignoring {} because errorMostRecent = {} and errorCount = {}", id, errorMostRecent, errorCount);
          }
        }
      }
    }
    if (forceReprocess.size() > 0 ||  deadDocs.size() > 0) {
      log.info("Found Errored docs. Reprocess:{} Mark Dead:{}",forceReprocess.size(), deadDocs.size());
    } else {
      log.info("No errored documents found.");
    }
    for (DocumentImpl d : deadDocs) {
      log.trace("REPORTING DEAD STATUS {}", d.getId());
      d.reportDocStatus();
    }

    for (Map.Entry<String, Set<LatestStatus>> toReproc : forceReprocess.entrySet()) {
      process(true, null, toReproc, FTI_ORIGIN);
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
  public String keySpace(String outputStep) {
    return this.keySpaces.computeIfAbsent(String.valueOf(outputStep), (ps) -> "jj_" + keySpaceHash(ps, this));
  }

  @NotNull
  private static String keySpaceHash(String outputStep, Scanner s) {
    String baseName = "jj_" + s.getName() + "_" + s.getPlan().getName() + "_" + s.getPlan().getVersion() +
        // below test for "null" is due to ConcurrentHashMap not accepting null keys, so we don't support steps named "null"
        (outputStep != null && !"null".equals(outputStep) ? "_" + outputStep : "");
    // Sadly we are limited to 48 char for keyspace names, so we must hash the info making our keyspace
    // names very sad and ugly.
    MessageDigest md;
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
    String result = new String(Hex.encodeHex(md.digest(baseName.getBytes())));
    log.info("Hash for {} keyspace is {}", baseName, result);
    return result;
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
    @SuppressWarnings({"unused", "UnusedReturnValue"})
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
      if (scanner.isRemembering() && (cassandra == null || Cassandra.isBooting())) {
        log.error("Cassandra null or still starting for scan operation, Invocation skipped");
        return;
      }
      try {
        if (isScanActive()) {
          log.info("Skipping scan, there is already an active scan");
          return;
        } else {
          log.info("{} of plan {} Starting scan at {} on {}", scanner.getName(), getPlan().getName(), new Date(),
              Thread.currentThread().getName());
        }
        // set up our watcher if needed
        scanStarted();
        processDirty();
        custom.run();
        log.info("{} of plan {} Finishing scan at {} on {}", scanner.getName(), getPlan().getName(), new Date(),
            Thread.currentThread().getName());
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
      log.trace("processing dirty");
      Set<String> sentAlready = new HashSet<>();
      FTIQueryContext ftiQueryContext = new FTIQueryContext(sentAlready);
      processPendingDocs(ftiQueryContext, List.of(DIRTY, FORCE, RESTART), false);
      processErrors(ftiQueryContext);
    }
  }
}
