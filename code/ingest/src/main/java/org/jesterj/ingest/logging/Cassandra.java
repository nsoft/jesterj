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

import org.apache.cassandra.service.CassandraDaemon;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.Main;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 11/8/14
 */

/**
 * Starts a Casandra Daemon after creating or loading the cassandra config. The main purpose of this
 * class is to wrap the CassandraDaemon and feed it a programmatically created configuration file.
 */
public class Cassandra implements Runnable {

  private static final Logger log = LogManager.getLogger();

  private static CassandraDaemon cassandra;

  /**
   * Indicates whether cassandra has finished booting. Does NOT indicate if
   * cassandra has subsequently been stopped. This method is not synchronized because
   * it is not meant to continuously track the state of cassandra, only serve as a latch
   * to release code that must not execute while cassandra has not yet booted.
   *
   * @return true indicating that cassandra has definitely finished booting, false indicates that cassandra may or
   *              may not have booted yet.
   */
  public boolean isBooting() {
    return booting;
  }


  private volatile boolean booting = true;

  @Override
  public void run() {
    try {
      File f = new File(Main.JJ_DIR + "/cassandra");
      if (!f.exists() && !f.mkdirs()) {
        throw new RuntimeException("could not create" + f);
      }
      f = new File(f, "cassandra.yaml");
      if (!f.exists()) {
        CassandraConfig cfg = new CassandraConfig();
        cfg.guessIp();
        String cfgStr = new Yaml().dumpAsMap(cfg);
        log.debug(cfgStr);
        Files.write(f.toPath(), cfgStr.getBytes(), StandardOpenOption.CREATE);
      }
      System.setProperty("cassandra.config", f.toURI().toString());
    } catch (IOException e) {
      e.printStackTrace();
    }
    cassandra = new CassandraDaemon();
    cassandra.activate();
    booting = false;
  }

  public void stop() {
    if (cassandra != null) {
      cassandra.deactivate();
    }
  }
}
