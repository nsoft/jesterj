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

import com.google.common.io.Files;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.model.impl.NamedBuilder;
import org.jesterj.ingest.model.impl.PlanImpl;
import org.jesterj.ingest.model.impl.StepImpl;
import org.jesterj.ingest.persistence.Cassandra;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;

import static com.copyright.easiertest.EasierMocks.prepareMocks;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests the JDBC scanner.
 *
 * @author dgoldenberg
 */
public class JdbcScannerImplTest extends ScannerImplTest {

  private static final String SQL_1 = "SELECT * FROM employee";

  public JdbcScannerImplTest() {
    prepareMocks(this);
  }

  @Test
  public void testBuild() {
    JdbcScanner.Builder builder = build (true);
    JdbcScanner built = (JdbcScanner) builder.build();

    assertEquals("JDBC_Scanner", built.getName());
    assertTrue(built.isAutoCommit());
    assertEquals("title", built.getContentColumn());
    assertEquals(1000, built.getFetchSize());
    assertEquals("org.hsqldb.jdbc.JDBCDriver", built.getJdbcDriver());
    assertEquals("", built.getJdbcPassword());
    assertEquals("jdbc:hsqldb:mem:employees;ifexists=true", built.getJdbcUrl());
    assertEquals("SA", built.getJdbcUser());
    assertEquals(3600, built.getQueryTimeout());
    assertEquals("ID",built.getDatabasePkColumnName());
    assertEquals(SQL_1, built.getSqlStatement());
  }

  private JdbcScanner.Builder build(boolean contentCol) {
    JdbcScanner.Builder builder = new JdbcScanner.Builder();

    builder
        .batchSize(100)
        .named("JDBC_Scanner")
        .withAutoCommit(true)
        .withFetchSize(1000)
        .withJdbcDriver("org.hsqldb.jdbc.JDBCDriver")
        .withJdbcPassword("")
        .withJdbcUrl("jdbc:hsqldb:mem:employees;ifexists=true")
        .withJdbcUser("SA")
        .withPKColumn("ID")
        .representingTable("employee")
        .withQueryTimeout(3600)
        .withSqlStatement(SQL_1);
    if (contentCol) {
      builder        .withContentColumn("title"); // simplistic "content column"
    }
    return builder;
  }

  @SuppressWarnings("SqlResolve")
  @Test
  public void testScan() throws InterruptedException, SQLException {
    @SuppressWarnings({"deprecation", "UnstableApiUsage"})
    File tempDir = Files.createTempDir();
    Cassandra.start(tempDir, "127.0.0.1");

    Connection c = DriverManager.getConnection("jdbc:hsqldb:mem:employees", "SA", "");
    PreparedStatement createTable = c.prepareStatement(
        "CREATE TABLE employee (id varchar(16),name varchar(64),title varchar(256))");
    createTable.execute();
    PreparedStatement insertRow1 = c.prepareStatement("insert into employee values ('1','John Doe','CEO,Janitor')");
    PreparedStatement insertRow2 = c.prepareStatement("insert into employee values ('2','Jane Doe','CFO,CTO')");
    insertRow1.execute();
    insertRow2.execute();

    HashMap<String, Document> scannedDocs = new LinkedHashMap<>();

    NamedBuilder<? extends DocumentProcessor> scannedDocRecorder = getScannedDocRecorder(scannedDocs);
    StepImpl.Builder capture = new StepImpl.Builder().named("capture").withProcessor(scannedDocRecorder);
    JdbcScanner.Builder scanStep = build(false);
    Plan plan = new PlanImpl.Builder().named("testScan").withIdField("ID")
        .addStep(scanStep)
        .addStep(capture,"JDBC_Scanner").build();
    plan.activate();
    Thread.sleep(5000);
    plan.deactivate();
    assertEquals("Should have 2 docs", 2, scannedDocs.size());
    assertTrue(scannedDocs.containsKey("jdbc:hsqldb:mem:employees;ifexists=true/employee/1"));
    assertTrue(scannedDocs.containsKey("jdbc:hsqldb:mem:employees;ifexists=true/employee/2"));
    Document doc1 = scannedDocs.get("jdbc:hsqldb:mem:employees;ifexists=true/employee/1");
    assertEquals("jdbc:hsqldb:mem:employees;ifexists=true/employee/1", doc1.getId());
    assertEquals("[John Doe]",doc1.get("NAME").toString());
    assertEquals("[CEO,Janitor]",doc1.get("TITLE").toString());

    Document doc2 = scannedDocs.get("jdbc:hsqldb:mem:employees;ifexists=true/employee/2");
    assertEquals("jdbc:hsqldb:mem:employees;ifexists=true/employee/2", doc2.getId());
    assertEquals("[Jane Doe]",doc2.get("NAME").toString());
    assertEquals("[CFO,CTO]",doc2.get("TITLE").toString());
  }
}
