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

import org.apache.logging.log4j.core.appender.ManagerFactory;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 11/15/14
 */
public class CassandraLog4JManagerFactory implements ManagerFactory<CassandraLog4JManager, Object> {

  // N.B. this probably can't be standard singleton because new CassandraLog4JManager() does complicated stuff and
  // can cause deadlocks if not coordinated properly... So I don't want it firing on class load.
  private static CassandraLog4JManager manager;

  @Override
  public synchronized CassandraLog4JManager createManager(String name, Object data) {
    if (manager == null) {
      manager = new CassandraLog4JManager("name");
    }
    return manager;
  }
}
