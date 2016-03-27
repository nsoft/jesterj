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

import static org.junit.Assert.assertEquals;

import java.util.HashMap;

import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.model.impl.PlanImpl;
import org.jesterj.ingest.model.impl.ScannerImpl;
import org.jesterj.ingest.model.impl.StepImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests the JDBC scanner.
 * 
 * @author dgoldenberg
 */
public class JdbcScannerImplTest {

//  private static final String SQL = "SELECT " +
//    "e.emp_no as empno, e.birth_date as birthdate, e.first_name as firstname, e.last_name as lastname, " +
//    "e.gender as gender, e.hire_date as hiredate, " +
//    "s.salary as salary " +
//    "FROM employees as e " +
//    "LEFT JOIN salaries as s " +
//    "ON e.emp_no=e.emp_no " +
//    "limit 3;";
  
  private static final String SQL_2 = "SELECT " +
    "emp_no as empno, birth_date as birthdate, first_name as firstname, last_name as lastname, " +
    "gender as gender, hire_date as hiredate " +
    "FROM employees " +
    "limit 3;";

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
  }

  @Test
  public void testBuild() {
    JdbcScanner.Builder builder = new JdbcScanner.Builder();
    ScannerImpl build = builder.build();
    assertEquals(JdbcScanner.class, build.getClass());
  }

  @Ignore("not ready yet")
  @Test
  public void testScan() throws InterruptedException {
    PlanImpl.Builder planBuilder = new PlanImpl.Builder();
    JdbcScanner.Builder scannerBuilder = new JdbcScanner.Builder();
    StepImpl.Builder testStepBuilder = new StepImpl.Builder();

    scannerBuilder
      .withAutoCommit(false)
      .withFetchSize(Integer.MIN_VALUE)
      .withJdbcDriver("com.mysql.jdbc.Driver")
      .withJdbcPassword("root")
      .withJdbcUrl("jdbc:mysql://localhost/employees?autoReconnect=true&useSSL=false")
      .withJdbcUser("root")
      .withQueryTimeout(-1)
      .withSqlStatement(SQL_2)
      .stepName("JDBC Scanner"); // .scanFreqMS(100);

    HashMap<String, Document> scannedDocs = new HashMap<>();

    testStepBuilder.stepName("test").batchSize(10).withProcessor(
      document -> {
        scannedDocs.put(document.getId(), document);
        
        System.out.println(">> ==============================");
        System.out.println(">> DOC: " + document);
        System.out.println(">> ==============================");
        
        return new Document[] { document };
      });

    planBuilder
      .addStep(null, scannerBuilder)
      .addStep(new String[] { "JDBC Scanner" }, testStepBuilder)
      .withIdField("empno"); // TODO if this is set wrong, generates lots of errors
    Plan plan = planBuilder.build();

    plan.activate();

    Thread.sleep(5000);

    plan.deactivate();
  }
}
