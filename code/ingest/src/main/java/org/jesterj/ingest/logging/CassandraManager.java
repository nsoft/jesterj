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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.logging.log4j.core.appender.AbstractManager;
import org.jesterj.ingest.Main;

import java.io.File;
import java.util.concurrent.Executor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 11/15/14
 */
public class CassandraManager extends AbstractManager {

  public static final String CREATE_LOG_KEYSPACE =
      "CREATE KEYSPACE IF NOT EXISTS jj_logging WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }";

  private CassandraManagerFactory INSTANCE = new CassandraManagerFactory();

  private Cassandra cassandra;
  private static final Executor exec = new ThreadPoolExecutor(1,10,1000, TimeUnit.SECONDS,new SynchronousQueue<>());

  protected CassandraManager(String name) {
    super(name);
    cassandra = new Cassandra();
    exec.execute(cassandra);
    Config config;
    try {
      File f = new File(Main.JJ_DIR + "/cassandra/cassandra.yaml");
      long start = System.currentTimeMillis();
      // On first startup we need to wait for the cassandra thread to create this....
      // but if it takes more than 5 sec, something is wrong and failing is appropriate.
      while(!f.exists() && (System.currentTimeMillis() - start) < 5000) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
          System.exit(2);
        }
      }
      String configURI = f.toURI().toString().replaceAll("file:/([^/])", "file:///$1");
      System.out.println(configURI);
      System.setProperty("cassandra.config", configURI);
      config = (new YamlConfigurationLoader()).loadConfig();
    } catch (ConfigurationException e) {
      throw new RuntimeException("Could not find cassandra config", e);
    }

    while(cassandra.isBooting()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    Session session = Cluster.builder()
        .addContactPoint(config.listen_address)
        .withCredentials("cassandra", "cassandra")
        .build().newSession();

    ResultSet rs = session.execute(CREATE_LOG_KEYSPACE);

    System.out.println(rs);
  }

  @Override
  protected void releaseSub() {
    cassandra.stop();
  }

  public Cassandra getCassandra() {
    return cassandra;
  }

  public void setCassandra(Cassandra cassandra) {
    this.cassandra = cassandra;
  }

}
