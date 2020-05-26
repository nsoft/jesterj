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


import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
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
import org.jesterj.ingest.persistence.CassandraSupport;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;

@Plugin(name = "JesterJAppender", category = "Core", elementType = "appender")
public class JesterJAppender extends AbstractAppender {

  private static final String INSERT_REG =
      "INSERT INTO jj_logging.regular " +
          "(id, logger, tstamp, level, thread, message) " +
          "VALUES(?,?,?,?,?,?)";

  @SuppressWarnings("SpellCheckingInspection")
  private static final String INSERT_FTI =
      "INSERT INTO jj_logging.fault_tolerant " +
          "(docid, scanner, logger, tstamp, level, thread, status, message) " +
          "VALUES(?,?,?,?,?,?,?,?)";
  public static final String REG_INSERT_Q = "REG_INSERT_Q";
  public static final String FTI_INSERT_Q = "FTI_INSERT_Q";

  private static CassandraSupport cassandra = new CassandraSupport();

  @SuppressWarnings("SpellCheckingInspection")
  public static final String JJ_INGEST_DOCID = "jj_ingest.docid";
  public static final String JJ_INGEST_SOURCE_SCANNER = "JJ_INGEST_SOURCE_SCANNER";

  private static CassandraLog4JManager manager;

  // we need to delay startup of cassandra until after logger initialization, because when cassandra code
  // tries to log messages we get a deadlock. Therefore the manager does not create cassandra until after the first
  // logging event, and then queues the events until cassandra is ready to accept them. This variable is then
  // empties and the queue items should eventually be garbage collected.
  private static final Queue<LogEvent> startupQueue = new ConcurrentLinkedQueue<>();


  @SuppressWarnings("UnusedDeclaration")
  protected JesterJAppender(String name, Filter filter, Layout<? extends Serializable> layout) {
    super(name, filter, null);
  }

  @SuppressWarnings("UnusedDeclaration")
  protected JesterJAppender(String name, Filter filter, Layout<? extends Serializable> layout, boolean ignoreExceptions) {
    super(name, filter, null, ignoreExceptions);
  }

  public JesterJAppender(String name, @SuppressWarnings("UnusedParameters") Layout layout, Filter filter, CassandraLog4JManager manager, boolean ignoreExceptions) {
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
      return null; // should never happen
    }
    if (layout == null) {
      layout = PatternLayout.createDefaultLayout();
    }
    cassandra.addStatement(FTI_INSERT_Q, INSERT_FTI);
    cassandra.addStatement(REG_INSERT_Q, INSERT_REG);
    return new JesterJAppender(name, layout, filter, manager, ignoreExceptions);
  }

  private static CassandraLog4JManager createManager() {
    return new CassandraLog4JManagerFactory().createManager("jjCassandraManager", null);
  }

  /**
   * Write events to cassandra. If cassandra is booting log events are cached. Once cassandra has booted
   * the first subsequent event will synchronize on the queue and begin draining the queue. During this
   * drain all logging events will need to acquire this lock. At the end of the drain the queue will be
   * nullified. Subsequent writes will then be processed immediately.
   *
   * @param event the event to write to the log.
   */
  @Override
  public void append(LogEvent event) {
    if (!manager.isReady()) {
      //System.out.println("Logging event added to startup queue");
      startupQueue.add(event);
    } else {
      if (startupQueue.isEmpty()) {
        writeEvent(event);
      } else {
        // one time occurrence post startup. Need to ensure incoming message is not written ahead of queued messages
        synchronized (startupQueue) {
          if (startupQueue.peek() != null) {
            for (LogEvent logEvent : startupQueue) {
              System.out.println("Logging event removed from startup queue");
              writeEvent(logEvent);
            }
            startupQueue.clear();
          }
        }
        writeEvent(event);
      }
    }
  }

  private void writeEvent(LogEvent e) {
    Marker m = e.getMarker();
    new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss,'Z'").format(e.getTimeMillis());
    // everything wrapped in String.valueOf to avoid any issues with null.
    String logger = String.valueOf(e.getLoggerName());
    Instant timeStamp = Instant.ofEpochMilli(e.getTimeMillis());
    String level = String.valueOf(e.getLevel());
    String thread = String.valueOf(Thread.currentThread().getName());
    String message = String.valueOf(e.getMessage().getFormattedMessage());

    if (m == null || m.isInstanceOf(Markers.LOG_MARKER)) {
      CqlSession s = cassandra.getSession();
      PreparedStatement pq = cassandra.getPreparedQuery(REG_INSERT_Q);

      // This should be good enough. The chances of collision are very very very small.
      // if we get into processing trillions of documents each producing hundreds of log
      // events, we'll begin to worry about the 1 in a billion chance of collision during that
      // ingest... https://en.wikipedia.org/wiki/Universally_unique_identifier#Collisions
      // The biggest risk is from some sort of seed overlap, so specify a secure seed
      // generation mechanism. Second biggest risk is a flaw in the PRNG, but that's java's
      // affair to manage.

      // at some time in the futre, the strategy here might become configurable, but
      // good enough for now.

      System.setProperty("java.util.secureRandomSeed", "true");
      ThreadLocalRandom current = ThreadLocalRandom.current();

      byte[] rand = new byte[16];
      current.nextBytes(rand);
      // version
      rand[6] &= 0x0f;
      rand[6] |= 0x40;
      //variant
      rand[8] &= 0x3f;
      rand[8] |= 0x80;

      long mostSignificantBits = 0;
      long leastSignificantBits = 0;
      for (int i = 0; i < 8; i++)
        mostSignificantBits = (mostSignificantBits << 8) | (rand[i] & 0xff);
      for (int i = 8; i < 16; i++)
        leastSignificantBits = (leastSignificantBits << 8) | (rand[i] & 0xff);

      UUID id = new UUID(mostSignificantBits, leastSignificantBits);

      s.execute(pq.bind(id, logger, timeStamp, level, thread, message));

      return;
    }

    if (m.isInstanceOf(Markers.FTI_MARKER)) {
      CqlSession s = cassandra.getSession();
      PreparedStatement pq = cassandra.getPreparedQuery(FTI_INSERT_Q);

      // everything wrapped in String.valueOf to avoid any issues with null.
      String status = String.valueOf(e.getMarker().getName());
      String docId = String.valueOf(e.getContextData().toMap().get(JJ_INGEST_DOCID));
      String scanner = String.valueOf(e.getContextData().toMap().get(JJ_INGEST_SOURCE_SCANNER));
      s.execute(pq.bind(docId, scanner, logger, timeStamp, level, thread, status, message));
    }

  }
}
