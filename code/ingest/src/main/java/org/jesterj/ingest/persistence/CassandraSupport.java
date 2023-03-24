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

package org.jesterj.ingest.persistence;


import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import java.util.Map;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * A class to globalize the cluster and session objects, while providing query caches on a per-instance basis.
 * These objects are lightweight and can be
 */
public class CassandraSupport {

  public static final SplittableRandom rootRand = new SplittableRandom();
  public static final ThreadLocal<SplittableRandom> antiCollision = ThreadLocal.withInitial(rootRand::split);
  private static final Map<String, Future<PreparedStatement>> preparedQueries = new ConcurrentHashMap<>();
  public static NonClosableSession NON_CLOSABLE_SESSION;

  /**
   * Add a query to the list of prepared queries maintained by this instance. Queries may be added before
   * Cassandra is booted, but will not become available until after the Cassandra boot cycle completes.
   * Attempts to add a query with the same name more than once will be ignored. Queries are cached globally.
   *
   * @param name      A name with which to retrieve the prepared statement instance
   * @param statement A string to be prepared as a CQL statement.
   * @return a future that will complete after cassandra has completed its boot cycle and the statement has been prepared.
   */
  public Future<PreparedStatement> addStatement(String name, String statement) {
    synchronized (preparedQueries) {
      if (!preparedQueries.containsKey(name)) {
        Future<PreparedStatement> result = Cassandra.whenBooted(() -> getSession().prepare(statement));
        preparedQueries.put(name, result);
        return result;
      }
    }
    return null;
  }

  /**
   * Returns a cassandra session wrapped to protect it from being closed.
   *
   * @return a <code>NonClosableSession</code> object.
   */
  public CqlSession getSession() {
    if (NON_CLOSABLE_SESSION == null && Cassandra.getListenAddress() != null)
    {
      NON_CLOSABLE_SESSION = new NonClosableSession();
    }
    if (NON_CLOSABLE_SESSION == null){
      System.out.println("WARNING: returning null session!!");
    }
    return NON_CLOSABLE_SESSION;
  }

  /**
   * Retrieve a prepared statement added via {@link #addStatement(String, String)}. This method will block until
   * cassandra has finished booting, a session has been created and the statement has been prepared. This method
   * may return null if no statement with that name has been prepared previously.
   *
   * @param qName the name of the statement to retrieve
   * @return the prepared statement ready for use.
   */
  public PreparedStatement getPreparedQuery(String qName) {
    try {
      Future<PreparedStatement> preparedStatementFuture = preparedQueries.get(qName);
      return preparedStatementFuture.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Retrieve a prepared statement added via {@link #addStatement(String, String)}. This method will block until
   * cassandra has finished booting, a session has been created and the statement has been prepared. If the statement q
   * was not previously prepared, this method will first prepare it and then return the prepared statement.
   *
   * @param qName the name of the statement to retrieve or prepare if not already prepared
   * @param q the query to prepare (if not already prepared).
   * @return the prepared statement ready for use.
   */
  public PreparedStatement getPreparedQuery(String qName, String q) {
    try {
      Future<PreparedStatement> preparedStatementFuture = preparedQueries.get(qName);
      if (preparedStatementFuture != null) {
        return preparedStatementFuture.get();
      } else {
        addStatement(qName,q);
        return preparedQueries.get(qName).get();
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public Future<Object> whenBooted(Callable<Object> makeTables) {
    return Cassandra.whenBooted(makeTables);
  }

  private static class SessionHolder {
    private static final Session INSTANCE;

    static {
      Session instance = null;
      try {
        instance = CqlSession.builder()
            .addContactPoint(Cassandra.getSocketAddress())
            .withLocalDatacenter("datacenter1")
            .withAuthCredentials("cassandra", JJCassandraDaemon.getPwDefault())
            .build();
      } catch (Throwable e) {
        e.printStackTrace();
      } finally {
        INSTANCE = instance;
      }
    }
  }

  public static class NonClosableSession implements CqlSession {

    private final Session sessionRef = SessionHolder.INSTANCE;

    @Override
    public CompletionStage<Void> closeAsync() {
      throw new UnsupportedOperationException("Do not close the sessions handed out from CassandraSupport");
    }

    @NonNull
    @Override
    public CompletionStage<Void> forceCloseAsync() {
      throw new UnsupportedOperationException("Do not close the sessions handed out from CassandraSupport");
    }

    @Override
    public void close() {
      throw new UnsupportedOperationException("Do not close the sessions handed out from CassandraSupport");
    }

    @NonNull
    @Override
    public CompletionStage<Void> closeFuture() {
      throw new UnsupportedOperationException("Do not close the sessions handed out from CassandraSupport");
    }

    @Override
    public boolean isClosed() {
      return sessionRef.isClosed();
    }



    // Only to be called when shutting down cassandra entirely.
    // This would only ever be done on JVM shutdown.
    public void deactivate() {
      System.out.println("CLOSING CASSANDRA:");
      Thread.dumpStack();
      sessionRef.close();
    }

    @NonNull
    @Override
    public String getName() {
      return sessionRef.getName();
    }

    @NonNull
    @Override
    public Metadata getMetadata() {
      return sessionRef.getMetadata();
    }

    @Override
    public boolean isSchemaMetadataEnabled() {
      return sessionRef.isSchemaMetadataEnabled();
    }

    @NonNull
    @Override
    public CompletionStage<Metadata> setSchemaMetadataEnabled(@Nullable Boolean newValue) {
      return sessionRef.setSchemaMetadataEnabled(newValue);
    }

    @NonNull
    @Override
    public CompletionStage<Metadata> refreshSchemaAsync() {
      return sessionRef.refreshSchemaAsync();
    }

    @NonNull
    @Override
    public CompletionStage<Boolean> checkSchemaAgreementAsync() {
      return sessionRef.checkSchemaAgreementAsync();
    }

    @NonNull
    @Override
    public DriverContext getContext() {
      return sessionRef.getContext();
    }

    @NonNull
    @Override
    public Optional<CqlIdentifier> getKeyspace() {
      return sessionRef.getKeyspace();
    }

    @NonNull
    @Override
    public Optional<Metrics> getMetrics() {
      return sessionRef.getMetrics();
    }

    @Nullable
    @Override
    public <RequestT extends Request, ResultT> ResultT execute(@NonNull RequestT request, @NonNull GenericType<ResultT> resultType) {
      return sessionRef.execute(request,resultType);
    }
  }
}
