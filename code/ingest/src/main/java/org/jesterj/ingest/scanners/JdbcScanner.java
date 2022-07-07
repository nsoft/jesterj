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

package org.jesterj.ingest.scanners;

import com.copyright.easiertest.SimpleProperty;
import com.google.common.io.CharStreams;
import net.jini.space.JavaSpace;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.ConfiguredBuildable;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.Router;
import org.jesterj.ingest.model.exception.ConfigurationException;
import org.jesterj.ingest.model.exception.PersistenceException;
import org.jesterj.ingest.model.impl.DocumentImpl;
import org.jesterj.ingest.model.impl.ScannerImpl;
import org.jesterj.ingest.utils.SqlUtils;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Optional;

/**
 * Scans a JDBC source such as an RDBMS (e.g. MySQL). Obtains a connection through the specified
 * JDBC driver and extracts rows using the specified SQL statement; converts extracted rows to
 * documents and passes the documents downstream.
 *
 * @author dgoldenberg
 */
public class JdbcScanner extends ScannerImpl {

  private static final Logger log = LogManager.getLogger();

  private String jdbcDriver;
  private String jdbcUrl;
  private String jdbcUser;
  private String jdbcPassword;
  private String sqlStatement;
  private String table;
  private String pkColumn;
  private String connectionTestQuery;
  private transient volatile boolean ready;

  // For MySQL, this may need to be set by user to Integer.MIN_VALUE:
  // http://stackoverflow.com/questions/2447324/streaming-large-result-sets-with-mysql
  // http://stackoverflow.com/questions/24098247/jdbc-select-batching-fetch-size-with-mysql
  // http://stackoverflow.com/questions/20899977/what-and-when-should-i-specify-setfetchsize
  private int fetchSize = -1;

  private boolean autoCommit;
  private int queryTimeout = -1;

  // The (optional) name for the column that contains the document content
  private String contentColumn;

  private final SqlUtils sqlUtils = new SqlUtils();

  // Use the ISO 8601 date format supported by Lucene, e.g. 2011-12-03T10:15:30Z
  private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_INSTANT;

  private volatile Connection connection;

  @Override
  public synchronized void activate() {
    ready = true;
    super.activate();
  }

  @Override
  public synchronized void deactivate() {
    ready = false;
    super.deactivate();
    try {
      if (isConnected()) {
        // yes minor race potential here but shutting down so don't care, and also don't care if we kill
        // in progress activities, (we have FTI to handle half done stuff)
        connection.close();
      }
    } catch (Throwable e) {
      log.error("Could not close database connection on deactivate!", e);
      if (e instanceof Error) {
        throw (Error) e;
      }
    }
  }

  @Override
  public ScanOp getScanOperation() {
    return new ScanOp(() -> {
      synchronized (this) {
        setReady(false); // ensure initial walk completes before new scans are started.

        // Remainder of operation is implemented here instead of relying on DefaultOp to avoid spamming the DB with
        // queries for individual rows.
        int count = 0;
        try {
          log.info("{} connecting to database {}", getName(), jdbcUrl);
          if (!isConnected()) {
            connection = sqlUtils.createJdbcConnection(jdbcDriver, jdbcUrl, jdbcUser, jdbcPassword, autoCommit);
          }
          try (Statement statement = createStatement(connection);
               ResultSet rs = statement.executeQuery(sqlStatement)) {
            processDirtyAndRestartStatuses(getCassandra());
            log.info("{} successfully queried database {}", getName(), jdbcUrl);

            String[] columnNames = getColumnNames(rs);
            int docIdColumnIdx = getDocIdColumnIndex(columnNames, getDatabasePkColumnName());

            // For each row
            while (rs.next() && isActive()) {
              if (count == 0) {
                log.debug("{} beginning processing of result set", getName());
              }
              String docId = rs.getString(docIdColumnIdx);
              docId = docIdFromPkVal(docId);
              Document doc = makeDoc(rs, columnNames, docId);
              JdbcScanner.this.docFound(doc);
              count++;
            }

          } catch (PersistenceException | SQLException ex) {
            log.error(getName() + " JDBC scanner error, rows processed=" + count, ex);
          }
        } catch (Exception e) {
          log.error("JDBC operation for {} failed.", getName());
          log.error(e);
        } finally {
          this.ready = true;
          log.debug("{} Database rows queued by {}", count, getName());
        }
      }
    }, this);
  }

  private void setReady(boolean b) {
    this.ready = b;
  }

  @NotNull
  private String docIdFromPkVal(String docId) {
    docId = jdbcUrl + "/" + table + "/" + docId;
    return docId;
  }

  private boolean isConnected() {
    if (connection == null) {
      return false;
    } else {
      try {
        connection.prepareStatement(connectionTestQuery).executeQuery();
      } catch (SQLException throwables) {
        return false;
      }
    }
    return true;
  }

  /**
   * The name of the column containing the database primary key.
   *
   * @return the configured pk column name or the docIdField name for the plan by default (which is 'id' by default).
   */
  protected String getDatabasePkColumnName() {
    return this.pkColumn == null ? getPlan().getDocIdField() : this.pkColumn;
  }

  /**
   * Creates a document from a result set row.
   *
   * @param rs          the result set
   * @param columnNames the column names
   * @param docId       the document ID to use
   * @return the document instance
   * @throws SQLException if the connection to the database has a problem
   */
  Document makeDoc(ResultSet rs, String[] columnNames, String docId) throws SQLException {
    // TODO - deletion tracking. Configure a separate query to identify soft deletes.
    // TODO - query Cassandra for whether the ID is in it, if so then it's an update

    byte[] rawBytes = getContentBytes(rs);

    DocumentImpl doc = new DocumentImpl(
        rawBytes,
        docId,
        getPlan(),
        Document.Operation.NEW,
        JdbcScanner.this);

    // For each column value
    for (int i = 1; i <= columnNames.length; i++) {
      // Skip over the content column; if any is specified, we've already processed it as raw bytes.
      // similarly skip over the ID field as well.
      String columnName = columnNames[i - 1];
      if (!columnName.equals(contentColumn) && !columnName.equals(doc.getIdField())) {
        Object value = rs.getObject(i);
        String strValue;
        if (value != null) {
          // Take care of java.sql.Date, java.sql.Time, and java.sql.Timestamp
          if (value instanceof Date) {
            strValue = convertDateToString(value);
          } else {
            strValue = value.toString();
          }
          doc.put(columnName, strValue);
        }
      }
    }

    return doc;
  }

  private byte[] getContentBytes(ResultSet rs) throws SQLException {
    byte[] rawBytes = null;

    // If the content column was specified
    if (StringUtils.isNotBlank(contentColumn)) {
      // Get its value.
      Object content = rs.getObject(contentColumn);

      if (content != null) {
        // Clob
        if (content instanceof Clob) {
          Clob clob = (Clob) content;
          try (Reader reader = clob.getCharacterStream()) {
            //noinspection UnstableApiUsage
            rawBytes = CharStreams.toString(reader).getBytes();
          } catch (IOException ex) {
            String msg = String.format("I/O error while reading value of content column '%s'.", contentColumn);
            log.error(msg, ex);
          }
        }
        // Blob
        else if (content instanceof Blob) {
          Blob blob = (Blob) content;
          try (InputStream stream = blob.getBinaryStream()) {
            rawBytes = IOUtils.toByteArray(stream);
          } catch (IOException ex) {
            String msg = String.format("I/O error while reading value of content column '%s'.", contentColumn);
            log.error(msg, ex);
          }
        }
        // Date (unlikely, but)
        else if (content instanceof Date) {
          rawBytes = convertDateToString(content).getBytes();
        }
        // Anything else
        else {
          rawBytes = content.toString().getBytes();
        }
      }
    }
    return rawBytes;
  }

  private static String convertDateToString(Object value) {
    Instant instant = Instant.ofEpochMilli(((Date) value).getTime());
    return DATE_FORMATTER.format(instant);
  }

  // Creates a statement to execute
  private Statement createStatement(Connection conn) throws SQLException {
    Statement statement = conn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
    if (fetchSize != -1) {
      statement.setFetchSize(fetchSize);
    }
    if (queryTimeout > 0) {
      statement.setQueryTimeout(queryTimeout);
    }
    return statement;
  }

  // Gets the list of columns from the result set
  private String[] getColumnNames(ResultSet rs) throws SQLException {
    ResultSetMetaData meta = rs.getMetaData();
    String[] names = new String[meta.getColumnCount()];

    for (int i = 0; i < names.length; i++) {
      // NB: we're getting the label here, to support the SQL's "select a AS b".
      // If label not available, we'll get the plain column name.
      names[i] = meta.getColumnLabel(i + 1);
    }
    return names;
  }

  // Gets the document ID column index
  private int getDocIdColumnIndex(String[] columnNames, String docIdColumnName) throws PersistenceException {
    int itemIdColNum = -1;
    if (docIdColumnName != null) {
      for (int i = 0; i < columnNames.length; i++) {
        if (columnNames[i].equals(docIdColumnName)) {
          itemIdColNum = i + 1; // + 1 for JDBC
          break;
        }
      }
      if (itemIdColNum == -1) {
        throw new PersistenceException(
            String.format("The document ID column could not be found in the SQL result set. docIdColumn: '%s', SQL: %s, columns: %s.",
                docIdColumnName, sqlStatement, String.join(", ", columnNames)));
      }
    }
    return itemIdColNum;
  }

  @Override
  public boolean isScanning() {
    return ready;
  }

  @Override
  public Optional<Document> fetchById(String id) {
    // TODO: implement this, something like:
    String sql = "select * from " + this.table + " where " + this.getDatabasePkColumnName() + " = ?";

    int slash = id.lastIndexOf("/") + 1;
    int hash = id.indexOf('#');
    int endOfParentDocId = hash < 0 ? id.length() : hash;
    // theoretically could be some other unique indexed column but usually it's the PK
    String pkValue = id.substring(slash, endOfParentDocId);
    try (PreparedStatement preparedStatement = getConnection().prepareStatement(sql)) {
      preparedStatement.setString(1, pkValue);
      ResultSet resultSet = preparedStatement.executeQuery();
      Document value = null;
      if (resultSet.next()) {
        value = makeDoc(resultSet, getColumnNames(resultSet), docIdFromPkVal(pkValue));
      }
      if (value != null) {
        return Optional.of(value);
      } else {
        log.warn("Did not find {}", id);
        return Optional.empty();
      }
    } catch (SQLException e) {
      log.error("Error in sql to fetch document:[{}] args was:{}", sql, pkValue);
      log.error("Exception was:", e);
    } catch (ConfigurationException | PersistenceException e) {
      log.error("JDBC operation for {} failed in fetchById", getName());
      log.error(e);
    }
    return Optional.empty();
  }

  private Connection getConnection() throws ConfigurationException, PersistenceException {
    if (!isConnected()) {
      connection = sqlUtils.createJdbcConnection(jdbcDriver, jdbcUrl, jdbcUser, jdbcPassword, autoCommit);
    }
    return connection;
  }

  /**
   * Handles configuration parameters for the JDBC scanner.
   *
   * @author dgoldenberg
   */
  public static class Builder extends ScannerImpl.Builder {

    private JdbcScanner obj;

    public Builder() {
      if (whoAmI() == this.getClass()) {
        obj = new JdbcScanner();
      }
    }

    private Class<?> whoAmI() {
      return new Object() {
      }.getClass().getEnclosingMethod().getDeclaringClass();
    }

    // TODO DG - validate method

    public Builder withJdbcDriver(String jdbcDriver) {
      getObj().jdbcDriver = jdbcDriver;
      return this;
    }

    public Builder withJdbcUrl(String jdbcUrl) {
      getObj().jdbcUrl = jdbcUrl;
      return this;
    }

    public Builder withJdbcUser(String jdbcUser) {
      getObj().jdbcUser = jdbcUser;
      return this;
    }

    public Builder withJdbcPassword(String jdbcPassword) {
      getObj().jdbcPassword = jdbcPassword;
      return this;
    }

    public Builder withSqlStatement(String sqlStatement) {
      getObj().sqlStatement = sqlStatement;
      return this;
    }

    public Builder representingTable(String table) {
      getObj().table = table;
      return this;
    }

    public Builder withContentColumn(String contentColumn) {
      getObj().contentColumn = contentColumn;
      return this;
    }

    public Builder withPKColumn(String pkCol) {
      getObj().pkColumn = pkCol;
      return this;
    }

    public Builder testingConnectionWith(String query) {
      getObj().connectionTestQuery = query;
      return this;
    }

    public Builder withFetchSize(int fetchSize) {
      getObj().fetchSize = fetchSize;
      return this;
    }

    public Builder withAutoCommit(boolean autoCommit) {
      getObj().autoCommit = autoCommit;
      return this;
    }

    public Builder withQueryTimeout(int queryTimeout) {
      getObj().queryTimeout = queryTimeout;
      return this;
    }

    @Override
    public JdbcScanner.Builder batchSize(int size) {
      super.batchSize(size);
      return this;
    }

    @Override
    public JdbcScanner.Builder outputSpace(JavaSpace outputSpace) {
      super.outputSpace(outputSpace);
      return this;
    }

    @Override
    public JdbcScanner.Builder inputSpace(JavaSpace inputSpace) {
      super.inputSpace(inputSpace);
      return this;
    }

    @Override
    public JdbcScanner.Builder named(String stepName) {
      super.named(stepName);
      return this;
    }

    @Override
    public JdbcScanner.Builder routingBy(ConfiguredBuildable<? extends Router> router) {
      super.routingBy(router);
      return this;
    }

    @Override
    public JdbcScanner.Builder scanFreqMS(long interval) {
      super.scanFreqMS(interval);
      return this;
    }

    @Override
    public ScannerImpl build() {
      if (obj.jdbcDriver == null
          || obj.jdbcPassword == null
          || obj.jdbcUser == null
          || obj.jdbcUrl == null
          || obj.table == null) {
        throw new IllegalStateException("jdbc driver, password, user, url, and the table being represented " +
            "must be supplied");
      }
      super.build();
      JdbcScanner tmp = getObj();
      this.obj = new JdbcScanner();
      return tmp;
    }

    @Override
    protected JdbcScanner getObj() {
      return obj;
    }
  }

  @SimpleProperty
  public String getJdbcDriver() {
    return jdbcDriver;
  }

  @SimpleProperty
  public String getJdbcUrl() {
    return jdbcUrl;
  }

  @SimpleProperty
  public String getJdbcUser() {
    return jdbcUser;
  }

  @SimpleProperty
  public String getJdbcPassword() {
    return jdbcPassword;
  }

  @SimpleProperty
  public String getSqlStatement() {
    return sqlStatement;
  }

  @SimpleProperty
  public int getFetchSize() {
    return fetchSize;
  }

  @SimpleProperty
  public boolean isAutoCommit() {
    return autoCommit;
  }

  @SimpleProperty
  public int getQueryTimeout() {
    return queryTimeout;
  }

  @SimpleProperty
  public String getContentColumn() {
    return contentColumn;
  }
}
