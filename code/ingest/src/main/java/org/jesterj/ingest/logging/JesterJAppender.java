/*
 * Copyright 2014 Needham Software LLC
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

package org.jesterj.ingest.logging;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 11/14/14
 */
@Plugin(name = "JesterJAppender", category = "Core", elementType = "appender", printObject = false)
public class JesterJAppender extends AbstractAppender {

  private static final String INSERT_REG =
      "INSERT INTO jj_logging.regular " +
          "(id, logger, tstamp, level, thread, message) " +
          "VALUES(?,?,?,?,?,?)";

  private static final String INSERT_FTI =
      "INSERT INTO jj_logging.fault_tolerant " +
          "(id, logger, tstamp, level, thread, status, docid, message) " +
          "VALUES(?,?,?,?,?,?,?,?)";

  public static final String JJ_INGEST_DOCID = "jj_ingest.docid";

  // per datastax reccomendation these are one per application.
  // http://www.datastax.com/dev/blog/4-simple-rules-when-using-the-datastax-drivers-for-cassandra
  private static Session regular;
  private static PreparedStatement regularInsert;
  private static Session faultTolerance;
  private static PreparedStatement ftiInsert;
  private static CassandraManager manager;

  // we need to delay startup of cassandra until after logger initialization, because when cassandra code
  // tries to log messges we get a deadlock. Therefore the manager does not create cassandra until after the first
  // logging event, and then queues the events until cassandra is ready to accept them. This variable is then
  // nullified and the queue should eventually be garbage collected.
  private static volatile Queue<LogEvent> startupQueue = new ConcurrentLinkedQueue<>();
  private static volatile Iterator<LogEvent> drainIterator;
  private static Cluster cluster;

  @SuppressWarnings("UnusedDeclaration")
  protected JesterJAppender(String name, Filter filter, Layout<? extends Serializable> layout) {
    super(name, filter, null);
  }

  @SuppressWarnings("UnusedDeclaration")
  protected JesterJAppender(String name, Filter filter, Layout<? extends Serializable> layout, boolean ignoreExceptions) {
    super(name, filter, null, ignoreExceptions);
  }

  public JesterJAppender(String name, @SuppressWarnings("UnusedParameters") Layout layout, Filter filter, CassandraManager manager, boolean ignoreExceptions) {
    // perhaps support layout and format the message?... later
    super(name, filter, null, ignoreExceptions);
    JesterJAppender.manager = manager;
  }


  @PluginFactory
  public static JesterJAppender createAppender(@PluginAttribute("name") String name,
                                               @PluginAttribute("ignoreExceptions") boolean ignoreExceptions,
                                               @PluginElement("Layout") Layout layout,
                                               @PluginElement("Filters") Filter filter) {

    if (name == null) {
      LOGGER.error("No name provided for JesterJAppender");
      return null;
    }

    manager = createManager();
    if (manager == null) {
      return null;
    }
    if (layout == null) {
      layout = PatternLayout.createDefaultLayout();
    }
    return new JesterJAppender(name, layout, filter, manager, ignoreExceptions);
  }

  private static CassandraManager createManager() {
    return new CassandraManagerFactory().createManager("jjCassandraManager", null);
  }

  /**
   * Write events to cassandra. If cassandra is booting log events are cached. Once cassandra has booted
   * the first subsequent event will synchronize on the queue and begin draining the queue. During this
   * drain all logging events will need to aquire this lock. At the end of the drain the queue will be
   * nullified. Subsequent writes will then be
   *
   * @param event the event to write to the log.
   */
  @Override
  public void append(LogEvent event) {
    if (!manager.isReady()) {
      startupQueue.add(event);
    } else {
      if (startupQueue != null && startupQueue.peek() != null) {
        //noinspection SynchronizeOnNonFinalField
        synchronized (startupQueue) {
          while (drainIterator != null) {
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
          drainIterator = startupQueue.iterator();
        }
        while (drainIterator.hasNext()) {
          writeEvent(drainIterator.next());
        }
        writeEvent(event);
        drainIterator = null;
        startupQueue = null;
      } else {
        writeEvent(event);
      }
    }
  }

  private Session getRegularSession() {
    synchronized (Cluster.class) {
      boolean newCluster = false;
      if (cluster == null) {
        cluster = Cluster.builder()
            .addContactPoint(Cassandra.getListenAddress())
            .withCredentials("cassandra", "cassandra")
            .build();
        newCluster = true;
      }
      synchronized (Session.class) {
        // The closed case represents a bug in our code or someone abusing our api's via reflection
        // but try to keep going... risk is a short period of dropped log statements
        if (newCluster || regular == null || regular.isClosed()) {
          regular = cluster.newSession();
          regularInsert = regular.prepare(INSERT_REG);
        }
      }
    }
    return regular;
  }

  private Session getFtiSession() {
    synchronized (Cluster.class) {
      boolean newCluster = false;
      if (cluster == null) {
        cluster = Cluster.builder()
            .addContactPoint(Cassandra.getListenAddress())
            .withCredentials("cassandra", "cassandra")
            .build();
        newCluster = true;
      }
      synchronized (Session.class) {
        // The closed session case represents a bug in our code or someone abusing our api's via reflection
        // but try to keep going... The risk is that we drop a log statement, which in turn
        // only risks that a document will be processed twice. Since solr overwrites rather than updates
        // this would only be an issue if the scanner is scanning with access to versions of a document,
        // but not including the version in the docId, and then only if a later version processes and
        // is added before than an earlier version... but that's already a problem with the indexing design
        // and the dropped status just increases the likelihood they will be processed in proximity.
        if (newCluster || faultTolerance == null || faultTolerance.isClosed()) {
          faultTolerance = cluster.newSession();
          ftiInsert = faultTolerance.prepare(INSERT_FTI);
        }
      }
    }
    return faultTolerance;
  }

  private void writeEvent(LogEvent e) {
    Marker m = e.getMarker();
    new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss,'Z'").format(e.getTimeMillis());
    // everything wrapped in String.valueOf to avoid any issues with null.
    String logger  = String.valueOf(e.getLoggerName());
    Date tstamp = new Date(e.getTimeMillis());
    String level = String.valueOf(e.getLevel());
    String thread = String.valueOf(Thread.currentThread().getName());
    String message = String.valueOf(e.getMessage());

    if (m == null || m.isInstanceOf(Markers.LOG_MARKER)) {
      Session s = getRegularSession();
      BoundStatement bs = new BoundStatement(regularInsert);

      UUID id = UUID.randomUUID();  // maybe we can skip this for regular logs?

      s.execute(bs.bind(id, logger, tstamp, level,  thread, message));

      return; // never want non FTI logging to write to the FTI table.
    }

    if (m.isInstanceOf(Markers.FTI_MARKER)) {
      Session s = getFtiSession();
      BoundStatement bs = new BoundStatement(ftiInsert);

      //TODO: this is wrong... should actually be the same for every log for that document on a trip through the pipeline.
      // but that has to wait until I move this to a wide row... if I do that...
      UUID id = UUID.randomUUID();

      // everything wrapped in String.valueOf to avoid any issues with null.
      String status = String.valueOf(e.getMarker().getName());
      String docid = String.valueOf(e.getContextMap().get(JJ_INGEST_DOCID));

      s.execute(bs.bind(id, logger, tstamp, level, thread, status, docid, message));

    }

  }
}
