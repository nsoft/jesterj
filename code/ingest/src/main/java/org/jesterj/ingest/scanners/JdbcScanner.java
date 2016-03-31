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
import net.jini.space.JavaSpace;
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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.function.Consumer;
import java.util.function.Function;

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

  // For MySQL, this may need to be set by user to Integer.MIN_VALUE:
  // http://stackoverflow.com/questions/2447324/streaming-large-result-sets-with-mysql
  // http://stackoverflow.com/questions/24098247/jdbc-select-batching-fetch-size-with-mysql
  // http://stackoverflow.com/questions/20899977/what-and-when-should-i-specify-setfetchsize
  private int fetchSize = -1;

  private boolean autoCommit;
  private int queryTimeout = -1;
  
  private SqlUtils sqlUtils = new SqlUtils();
  
  // Use the ISO 8601 date format supported by Lucene, e.g. 2011-12-03T10:15:30Z
  private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_INSTANT;

  @Override
  public Function<String, String> getIdFunction() {
    return s -> s;
  }

  @Override
  public Consumer<Document> getDocumentTracker() {
    return document -> {
    };
  }

  @Override
  public Runnable getScanOperation() {
    return () -> {

      try (
          // Establish a connection and execute the query.
          Connection conn = sqlUtils.createJdbcConnection(jdbcDriver, jdbcUrl, jdbcUser, jdbcPassword, autoCommit);

          Statement statement = createStatement(conn);
          ResultSet rs = statement.executeQuery(sqlStatement)) {
        
        String[] columnNames = getColumnNames(rs);
        int docIdColumnIdx = getDocIdColumnIndex(columnNames, getPlan().getDocIdField());

        // For each row
        while (rs.next()) {
          String docId = rs.getString(docIdColumnIdx);
          Document doc = makeDoc(rs, columnNames, docId);
          JdbcScanner.this.docFound(doc);
        }

      } catch (ConfigurationException | PersistenceException | SQLException ex) {
        log.error("JDBC scanner error.", ex);
      }

    };
  }

  /**
   * Creates a document from a result set row.
   * 
   * @param rs
   *          the result set
   * @param columnNames
   *          the column names
   * @param docId
   *          the document ID to use
   * @return the document instance
   * @throws SQLException
   */
  Document makeDoc(ResultSet rs, String[] columnNames, String docId) throws SQLException {
    // TODO DG - optional rawData field in the config
    // blobField, clobField - not both
    // set file_size if blob/clob there
    
    // TODO - deletion tracking. Configure a separate query to identify soft deletes.
    // TODO - query Cassandra for whether the ID is in it, if so then it's an update
    
    DocumentImpl doc = new DocumentImpl(
      null,
      docId,
      getPlan(),
      Document.Operation.NEW,
      JdbcScanner.this);

    // For each column value
    for (int i = 1; i <= columnNames.length; i++) {
      Object value = rs.getObject(i);
      String strValue;
      if (value != null) {
        // Take care of java.sql.Date, java.sql.Time, and java.sql.Timestamp
        if (value instanceof Date) {
          Instant instant = Instant.ofEpochMilli(((Date) value).getTime());
          strValue = DATE_FORMATTER.format(instant);
        } else {
          strValue = value.toString();
        }
        doc.put(columnNames[i - 1], strValue);
      }
    }

    return doc;
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
      getObject().jdbcDriver = jdbcDriver;
      return this;
    }

    public Builder withJdbcUrl(String jdbcUrl) {
      getObject().jdbcUrl = jdbcUrl;
      return this;
    }

    public Builder withJdbcUser(String jdbcUser) {
      getObject().jdbcUser = jdbcUser;
      return this;
    }

    public Builder withJdbcPassword(String jdbcPassword) {
      getObject().jdbcPassword = jdbcPassword;
      return this;
    }

    public Builder withSqlStatement(String sqlStatement) {
      getObject().sqlStatement = sqlStatement;
      return this;
    }

    public Builder withFetchSize(int fetchSize) {
      getObject().fetchSize = fetchSize;
      return this;
    }

    public Builder withAutoCommit(boolean autoCommit) {
      getObject().autoCommit = autoCommit;
      return this;
    }

    public Builder withQueryTimeout(int queryTimeout) {
      getObject().queryTimeout = queryTimeout;
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
      JdbcScanner tmp = obj;
      this.obj = new JdbcScanner();
      return tmp;
    }

    @Override
    protected JdbcScanner getObject() {
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
}
