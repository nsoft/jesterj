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

import java.util.HashMap;

import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.model.impl.NamedBuilder;
import org.jesterj.ingest.model.impl.PlanImpl;
import org.jesterj.ingest.model.impl.StepImpl;

/**
 * Represents a simple command line utility to test the JDBC scanner.
 * 
 * @author dgoldenberg
 */
public class JdbcScannerTester {

  // Use the sample 'employees' database for MySQL:
  // https://dev.mysql.com/doc/employee/en/employees-installation.html
  // https://launchpad.net/test-db/
  
  private static final String SQL_1 = "SELECT " +
    "emp_no as empno, birth_date as birthdate, first_name as firstname, last_name as lastname, " +
    "gender as gender, hire_date as hiredate " +
    "FROM employees " +
    "limit 3;";

  public static void main(String[] args) throws InterruptedException {
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
      .withSqlStatement(SQL_1)
      .named("JDBC Scanner").scanFreqMS(1000);

    HashMap<String, Document> scannedDocs = new HashMap<>();

    testStepBuilder.named("test").batchSize(10).withProcessor(
      new NamedBuilder<DocumentProcessor>() {
        @Override
        public NamedBuilder<DocumentProcessor> named(String name) {
          return null;
        }

        @Override
        public DocumentProcessor build() {
          return new DocumentProcessor() {
            @Override
            public String getName() {
              return null;
            }

            @Override
            public Document[] processDocument(Document document) {
              scannedDocs.put(document.getId(), document);

              System.out.println(">> ==============================");
              System.out.println(">> DOC: " + document);
              System.out.println(">> ==============================");

              return new Document[] { document };
            }
          };
        }
      });

    planBuilder
      .addStep(null, scannerBuilder)
      .addStep(new String[] { "JDBC Scanner" }, testStepBuilder)
      .withIdField("empno");

    Plan plan = planBuilder.build();

    plan.activate();

    Thread.sleep(5000);
    
    plan.deactivate();
  }

}
