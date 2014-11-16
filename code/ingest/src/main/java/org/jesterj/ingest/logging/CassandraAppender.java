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

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 11/14/14
 */
@Plugin(name = "CassandraAppender", category = "Core", elementType = "appender", printObject = false)
public class CassandraAppender extends AbstractAppender {


  private Session session;
  private static CassandraManager manager;

  protected CassandraAppender(String name, Filter filter, Layout<? extends Serializable> layout) {
    super(name, filter, null);
  }

  protected CassandraAppender(String name, Filter filter, Layout<? extends Serializable> layout, boolean ignoreExceptions) {
    super(name, filter, null, ignoreExceptions);
  }

  public CassandraAppender(String name, Layout layout, Filter filter, CassandraManager manager, boolean ignoreExceptions) {
    super(name, filter, null, ignoreExceptions);
    CassandraAppender.manager = manager;
  }

  @PluginFactory
  public static CassandraAppender createAppender(@PluginAttribute("name") String name,
                                                 @PluginAttribute("ignoreExceptions") boolean ignoreExceptions,
                                                 @PluginElement("Layout") Layout layout,
                                                 @PluginElement("Filters") Filter filter) {

    if (name == null) {
      LOGGER.error("No name provided for CassandraAppender");
      return null;
    }

    CassandraManager manager = createManager();
    if (manager == null) {
      return null;
    }
    if (layout == null) {
      layout = PatternLayout.createDefaultLayout();
    }
    return new CassandraAppender(name, layout, filter, manager, ignoreExceptions);


  }

  private static CassandraManager createManager() {
    return new CassandraManagerFactory().createManager("jjCassandraManager", null);
  }

  @Override
  public void append(LogEvent event) {

  }
}
