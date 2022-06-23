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

package org.jesterj.ingest.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.exception.ConfigurationException;
import org.jesterj.ingest.model.exception.PersistenceException;

/**
 * Contains helper methods related to SQL usage.
 * 
 * @author dgoldenberg
 */
public class SqlUtils {

  private static Logger log = LogManager.getLogger();

  /**
   * Creates a JDBC connection, given JDBC connection parameters.
   * 
   * @param jdbcDriver
   *          the JDBC driver
   * @param jdbcUrl
   *          the JDBC URL
   * @param jdbcUser
   *          the JDBC user
   * @param jdbcPassword
   *          the JDBC password
   * @param autoCommit
   *          whether or not the connection should autocommit
   * @return a connection JDBC connection
   * @throws ConfigurationException
   *           on configuration error
   * @throws PersistenceException
   *           on JDBC/SQL error
   */
  public Connection createJdbcConnection(
    String jdbcDriver,
    String jdbcUrl,
    String jdbcUser,
    String jdbcPassword,
    boolean autoCommit)
    throws ConfigurationException, PersistenceException {

    Connection connection;

    try {
      // Register the driver
      Class.forName(jdbcDriver);
    } catch (ClassNotFoundException ex) {
      throw new ConfigurationException(String.format("JDBC Driver could not be found: '%s'.", jdbcDriver), ex);
    }

    try {

      log.debug("Establishing JDBC connection to '{}' on thread {}", jdbcUrl, Thread.currentThread().getName());

      // Get a connection
      connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
      connection.setAutoCommit(autoCommit);

      log.debug("Successfully established JDBC connection to '{}'", jdbcUrl);

    } catch (SQLException ex) {
      throw new PersistenceException(
        String.format("Error connecting to JDBC data source: '%s' with driver: '%s'.", jdbcUrl, jdbcDriver), ex);
    }
    return connection;
  }

}
