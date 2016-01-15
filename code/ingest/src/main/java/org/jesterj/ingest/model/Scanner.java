package org.jesterj.ingest.model;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 11/10/13
 */


/**
 * Monitors a document source for changes on a regular basis. When new files are found, they are submitted
 * to the supplied queue. Note that Scanners do not normally support the methods from
 * {@link java.util.concurrent.BlockingQueue} since they normally only output documents, and never receive them.
 * These methods may throw {@link java.lang.UnsupportedOperationException}
 */
public interface Scanner extends Step {

  /**
   * The interval for the scanner to fire. Scanners implementations must not begin a new scan more
   * frequently than this interval. There is no guarantee that the scan will begin this frequently
   * although implementations are encouraged to report any occasions on which scans are started
   * later than this interval would imply as warnings. An interval of less than zero indicates
   * that the scanner should only run once.
   *
   * @return the scan interval. Defaults to 30 minutes
   */
  public long getInterval();

}
