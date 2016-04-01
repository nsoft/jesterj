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

import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.model.impl.NamedBuilder;
import org.jesterj.ingest.model.impl.PlanImpl;
import org.jesterj.ingest.model.impl.StepImpl;

import java.util.HashMap;

/**
 * Represents a simple command line utility to test the JDBC scanner.
 * 
 * @author dgoldenberg
 */
public class JdbcScannerRunner {

  // Use the sample 'employees' database for MySQL:
  // https://dev.mysql.com/doc/employee/en/employees-installation.html
  // https://launchpad.net/test-db/

  private static final String SQL_1 = "SELECT " +
    "e.emp_no as empno, e.birth_date as birthdate, e.first_name as firstname, e.last_name as lastname, e.hire_date as hiredate, t.title as title " +
    "FROM employees as e LEFT JOIN titles as t ON e.emp_no=t.emp_no limit 25;";

  public static void main(String[] args) throws InterruptedException {
    PlanImpl.Builder planBuilder = new PlanImpl.Builder();
    JdbcScanner.Builder scannerBuilder = new JdbcScanner.Builder();
    StepImpl.Builder testStepBuilder = new StepImpl.Builder();

    scannerBuilder
      .withAutoCommit(false)
      .withContentColumn("title") // simplistic "content column"
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
              System.out.println(">> === DOC: ");
              System.out.println(">> ID: " + document.getId());
              System.out.println(document);
              byte[] rawData = document.getRawData();
              if (rawData != null) {
                System.out.println(">> 'raw data': " + new String(rawData));
              }
              System.out.println(">> ==============================");

              return new Document[] { document };
            }
          };
        }
      });

    planBuilder
      .named("JdbcScannerPlan")
      .addStep(null, scannerBuilder)
      .addStep(new String[] { "JDBC Scanner" }, testStepBuilder)
      .withIdField("empno");

    Plan plan = planBuilder.build();

    plan.activate();

    Thread.sleep(5000);

    plan.deactivate();
  }

}
