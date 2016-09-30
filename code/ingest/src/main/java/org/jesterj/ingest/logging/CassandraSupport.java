/*
 * Copyright 2016 Needham Software LLC
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

import com.datastax.driver.core.CloseFuture;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * A class to globalize the cluster and session objects, while providing query caches on a per-instance basis.
 * These objects are light weight and can be
 */
public class CassandraSupport {

  private Map<String, Future<PreparedStatement>> preparedQueries = new ConcurrentHashMap<>();

  /**
   * Add a query to the list of prepared queries maintained by this instance. Queries may be added before
   * Cassandra is booted, but will not become available until after the Cassandra boot cycle completes.
   *
   * @param name      A name with which to retrieve the prepared statement instance
   * @param statement A string to be prepared as a CQL statement.
   */
  public void addStatement(String name, String statement) {
    //noinspection unchecked
    preparedQueries.put(name, Cassandra.whenBooted(() -> getSession().prepare(statement)));
  }

  /**
   * Returns a cassandra session wrapt to protect it from being closed.
   *
   * @return a <code>NonClosableSession</code> object.
   */
  public Session getSession() {
    return new NonClosableSession();
  }

  /**
   * Retreive a prepared statement added via {@link #addStatement(String, String)}. This method will block until
   * cassandra has finished booting, a session has been created and the statement has been prepared.
   *
   * @param qName the name of the statement to retrieve
   * @return the prepared statement ready for use.
   */
  public PreparedStatement getPreparedQuery(String qName) {
    try {
      return preparedQueries.get(qName).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  // per Datastax recommendation these are one per application.
  // http://www.datastax.com/dev/blog/4-simple-rules-when-using-the-datastax-drivers-for-cassandra
  private static class ClusterHolder {
    private static final Cluster INSTANCE = Cluster.builder()
        .addContactPoint(Cassandra.getListenAddress())
        .withCredentials("cassandra", "cassandra")
        .build();
  }

  private static class SessionHolder {
    private static final Session INSTANCE = ClusterHolder.INSTANCE.newSession();
  }

  private class NonClosableSession implements Session {
    @Override
    public String getLoggedKeyspace() {
      return sessionRef.getLoggedKeyspace();
    }

    @Override
    public Session init() {
      return sessionRef.init();
    }

    @Override
    public ResultSet execute(String query) {
      return sessionRef.execute(query);
    }

    @Override
    public ResultSet execute(String query, Object... values) {
      return sessionRef.execute(query, values);
    }

    @Override
    public ResultSet execute(Statement statement) {
      return sessionRef.execute(statement);
    }

    @Override
    public ResultSetFuture executeAsync(String query) {
      return sessionRef.executeAsync(query);
    }

    @Override
    public ResultSetFuture executeAsync(String query, Object... values) {
      return sessionRef.executeAsync(query, values);
    }

    @Override
    public ResultSetFuture executeAsync(Statement statement) {
      return sessionRef.executeAsync(statement);
    }

    @Override
    public PreparedStatement prepare(String query) {
      return sessionRef.prepare(query);
    }

    @Override
    public PreparedStatement prepare(RegularStatement statement) {
      return sessionRef.prepare(statement);
    }

    @Override
    public ListenableFuture<PreparedStatement> prepareAsync(String query) {
      return sessionRef.prepareAsync(query);
    }

    @Override
    public ListenableFuture<PreparedStatement> prepareAsync(RegularStatement statement) {
      return sessionRef.prepareAsync(statement);
    }

    @Override
    public CloseFuture closeAsync() {
      throw new UnsupportedOperationException("Do not close the sessions handed out from CassandraSupport");
    }

    @Override
    public void close() {
      throw new UnsupportedOperationException("Do not close the sessions handed out from CassandraSupport");
    }

    @Override
    public boolean isClosed() {
      return sessionRef.isClosed();
    }

    @Override
    public Cluster getCluster() {
      return sessionRef.getCluster();
    }

    @Override
    public State getState() {
      return sessionRef.getState();
    }

    private Session sessionRef = SessionHolder.INSTANCE;
  }
}
