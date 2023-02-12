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
import org.jesterj.ingest.processors.ErrorFourthTestProcessor;
import org.jesterj.ingest.processors.PauseEveryFiveTestProcessor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class JdbcScannerImplFTITest extends ScannerImplTest {

  private static final String SHAKESPEAR = "Shakespear_scanner";
  public static final String SQL_1 = "SELECT * FROM play";
  public static final int PAUSE_MILLIS = 2000; // values smaller than this become flaky

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
  }

  private void loadShakespeareToHSQL() throws SQLException, IOException {
    AtomicInteger idNext = new AtomicInteger(1);
    Connection c = DriverManager.getConnection("jdbc:hsqldb:mem:shakespeare", "SA", "");
    PreparedStatement createTable = c.prepareStatement(
        "CREATE TABLE play (id varchar(16),name varchar(64),play_text clob)");
    createTable.execute();
    @SuppressWarnings("SqlResolve")
    PreparedStatement insert = c.prepareStatement("INSERT INTO play VALUES (?,?,?)");

    File tragedies = new File("src/test/resources/test-data");
    java.nio.file.Files.walkFileTree(tragedies.toPath(), new FileVisitor<>() {
      @Override
      public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        try {
          insert.setString(1, String.valueOf(idNext.getAndIncrement()));
          String uri = file.toUri().toString();
          insert.setString(2, uri.substring(uri.lastIndexOf(File.separator) + 1));
          String text = new String(java.nio.file.Files.readAllBytes(file));
          insert.setString(3, text);
          insert.execute();
        } catch (SQLException e) {
          throw new IOException(e);
        }
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
        throw new IOException();
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
        return FileVisitResult.CONTINUE;
      }
    });
  }

  // this has been segregated to it's own test because something about starting cassandra after
  // using logging without it hoses the event contexts in logging. For now, same process stop/start
  // and use without cassandra configured is not a valid use case, so punt...
  @Test
//  @Ignore(value = "seems to experience cross talk with and interfere with other tests, passes solo locally, " +
//      "expect cassandra is not playing nice here")
  public void testScanWithMemory() throws InterruptedException, SQLException, IOException {
    loadShakespeareToHSQL();
    //noinspection UnstableApiUsage,deprecation
    File tempDir = Files.createTempDir();
    HashMap<String, Document> scannedDocs = new LinkedHashMap<>();
    Cassandra.start(tempDir, "127.0.0.1");

    String[] errorId = new String[1];

    NamedBuilder<? extends DocumentProcessor> scannedDocRecorder = getScannedDocRecorder(scannedDocs);
    PauseEveryFiveTestProcessor.Builder pause30Every5 = new PauseEveryFiveTestProcessor.Builder()
        .named("pause5")
        .pausingFor(PAUSE_MILLIS);
    NamedBuilder<? extends DocumentProcessor> error4thof5 =
        new ErrorFourthTestProcessor.Builder().named("error4").withErrorReporter(errorId).erroringFromStart(false);

    Plan plan1 = getPlan("plan1", pause30Every5,error4thof5,scannedDocRecorder);

    try {
      plan1.activate();
      // now scanner should find all docs, attempt to index them, all marked
      // as processing...
      Thread.sleep(3*PAUSE_MILLIS/4);
      // the pause ever 5 should have let 5 through and then paused for 30 sec
      assertEquals(5, scannedDocs.size());
      plan1.deactivate();

      // plan has been deactivated, leaving 5 as indexed and the rest as processing

      Thread.sleep(3*PAUSE_MILLIS/4);
      assertEquals(5, scannedDocs.size());

      plan1.activate();
      // plan should first queue all processing docs (from prior scan) and then proceed with new
      // scan, but that scan should never start because only the first 5 docs queued up will be
      // processed before pausing another 30 seconds. Since the map is keyed by ID an increase in
      // the size of the map shows that the previous documents were not processed.
      Thread.sleep(3*PAUSE_MILLIS/4);
      plan1.deactivate();
      assertEquals(String.valueOf(scannedDocs.keySet()).replaceAll(", ","\n"), 10,scannedDocs.size()); // test that 5 NEW docs were scanned

      Thread.sleep(3*PAUSE_MILLIS/4);
      assertEquals(10, scannedDocs.size()); // test plan really deactivated
      startErrors(plan1,"test1");
      plan1.activate();
      Thread.sleep(3*PAUSE_MILLIS/4);
      assertEquals(14, scannedDocs.size()); // test that 4 NEW docs were seen (a 5th will have errored but not been counted)
      plan1.deactivate();
      stopErrors(plan1,"test1");

      String eid = errorId[0];
      assertNotNull(eid);

      Thread.sleep(3*PAUSE_MILLIS/4);
      assertEquals(14, scannedDocs.size()); // test plan really deactivated

      plan1.activate();
      Thread.sleep(3*PAUSE_MILLIS/4);
      assertEquals(19, scannedDocs.size()); // test that 5 NEW docs were scanned
      plan1.deactivate();

      Thread.sleep(3*PAUSE_MILLIS/4);
      shortPauses(plan1,"test0");
      plan1.activate();
      Thread.sleep(3*PAUSE_MILLIS/4);
      plan1.deactivate();
      assertTrue("key:" + eid + " docs:\n" + String.valueOf(scannedDocs).replaceAll("'},", "'}\n") ,scannedDocs.containsKey(eid)); // AND the error doc was one of them
      assertEquals(44, scannedDocs.size());
      scannedDocs.clear();
      // the documents will have been scanned, but since they are unchanged
      // they do not get sent down the pipeline, and so the counter
      // step won't see them

      Thread.sleep(3*PAUSE_MILLIS/4);

      plan1.activate();
      Thread.sleep(3*PAUSE_MILLIS/4);
      plan1.deactivate();
      Thread.sleep(3*PAUSE_MILLIS/4);
      assertEquals(0, scannedDocs.size());
    } finally {
      // Thread.sleep(600000); // useful if you want to query cassandra with cqlsh when debugging
      Cassandra.stop();
    }
  }

  @SuppressWarnings("SameParameterValue")
  @SafeVarargs
  private Plan getPlan(String planName, NamedBuilder<? extends DocumentProcessor>... processors) {
    PlanImpl.Builder planBuilder = new PlanImpl.Builder();
    JdbcScanner.Builder scannerBuilder = new JdbcScanner.Builder();

    scannerBuilder.named("test_scanner")
        .withAutoCommit(true)
        .withFetchSize(1000)
        .withJdbcDriver("org.hsqldb.jdbc.JDBCDriver")
        .withJdbcPassword("")
        .withJdbcUrl("jdbc:hsqldb:mem:shakespeare;ifexists=true")
        .withJdbcUser("SA")
        .withPKColumn("ID")
        .representingTable("play")
        .named(SHAKESPEAR)
        .withQueryTimeout(3600)
        .withSqlStatement(SQL_1)
        .rememberScannedIds(true)
        .detectChangesViaHashing(true)
        .withContentColumn("play_text")
        .scanFreqMS(PAUSE_MILLIS/4);

    planBuilder
        .named(planName)
        .addStep(scannerBuilder)
        .withIdField("id");
    String prior = SHAKESPEAR;
    int count=0;
    for (NamedBuilder<? extends DocumentProcessor> processor : processors) {
      StepImpl.Builder testStepBuilder = new StepImpl.Builder();
      testStepBuilder.named("test" + count++)
          .withShutdownWait(50)
          .withProcessor(
              processor
          );
      planBuilder.addStep(testStepBuilder, prior);
      prior = testStepBuilder.getStepName();
    }

    return planBuilder.build();
  }
}
