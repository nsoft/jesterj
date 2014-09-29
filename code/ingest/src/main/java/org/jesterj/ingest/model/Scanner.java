package org.jesterj.ingest.model;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 11/10/13
 */

/**
 * Monitors a document source for changes on a regular basis. When new files are found, they are submitted
 * to the supplied queue.
 */
public interface Scanner extends Step {

  /**
   * Set the interval for the scanner to fire in milliseconds.
   *
   * @see #getInterval()
   * @param milliseconds minnimum miliseconds between scans
   */
  public void setInterval(long milliseconds);

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

  /**
   * Set the minimum time between scans. See {@link #getPause()} for details.
   *
   * @param milliseconds the interval to pause between scans.
   */
  public void setPause(long milliseconds);

  /**
   * The minimum time between the end of a scan and the beginning of a new scan by this scanner.
   * Setting a non-zero pause value may allow downstream components to complete processing of the
   * scan output before a new scan begins, and reduce the chances that the overall system will become
   * overloaded.
   *
   * @return The pause interval. Defaults to 10 minutes
   */
  public long getPause();


  public void updateStatus(Item item, ItemProcessor processor);

}
