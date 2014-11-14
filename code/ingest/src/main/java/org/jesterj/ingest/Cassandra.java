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

package org.jesterj.ingest;

import org.apache.cassandra.service.CassandraDaemon;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
public class Cassandra implements Runnable {

  private static final Logger log = LogManager.getLogger();

  @Override
  public void run() {
    try {
      CassandraConfig cfg = new CassandraConfig();
      cfg.guessIp();
      String cfgStr = new Yaml().dumpAsMap(cfg);
      log.debug(cfgStr);
      File f = new File(Main.JJ_DIR + "/cassandra");
      if (!f.exists() && !f.mkdirs()) {
        throw new RuntimeException("could not create" + f);
      }
      f = new File(f, "cassandra.yaml");
      if (f.exists()) {
        //noinspection ResultOfMethodCallIgnored
        f.delete();
      }
      long start = System.currentTimeMillis();
      while(f.exists() && (System.currentTimeMillis() - start) < 5000) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      Files.write(f.toPath(), cfgStr.getBytes(), StandardOpenOption.CREATE);
      System.setProperty("cassandra.config", f.toURI().toString());
    } catch (IOException e) {
      e.printStackTrace();
    }
    CassandraDaemon daemon = new CassandraDaemon();
    daemon.activate();
  }
}
