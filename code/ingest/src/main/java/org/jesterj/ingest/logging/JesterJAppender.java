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

import com.datastax.driver.core.Session;
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
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 11/14/14
 */
@Plugin(name = "JesterJAppender", category = "Core", elementType = "appender", printObject = false)
public class JesterJAppender extends AbstractAppender {


  private Session session;
  private static CassandraManager manager;

  // we need to delay startup of cassandra until after logger initialization, because when cassandra code
  // tries to log messges we get a deadlock. Therefore the manager does not create cassandra until after the first
  // logging event, and then queues the events until cassandra is ready to accept them. This variable is then
  // nullified and the queue should eventually be garbage collected.
  private static volatile Queue<LogEvent> startupQueue = new ConcurrentLinkedQueue<>();
  private static volatile Iterator<LogEvent> drainIterator;

  protected JesterJAppender(String name, Filter filter, Layout<? extends Serializable> layout) {
    super(name, filter, null);
  }

  protected JesterJAppender(String name, Filter filter, Layout<? extends Serializable> layout, boolean ignoreExceptions) {
    super(name, filter, null, ignoreExceptions);
  }

  public JesterJAppender(String name, Layout layout, Filter filter, CassandraManager manager, boolean ignoreExceptions) {
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
   * @param event
   */
  @Override
  public void append(LogEvent event) {
    if (manager.isReady()) {
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


  private void writeEvent(LogEvent e) {
    //TODO soemthing real...
    System.out.println(e);
  }
}
