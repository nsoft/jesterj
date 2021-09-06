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

package org.jesterj.ingest.model.impl;

import com.copyright.easiertest.Mock;
import com.copyright.easiertest.ObjectUnderTest;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.session.Session;
import org.apache.logging.log4j.Level;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.model.Scanner;
import org.jesterj.ingest.model.Step;
import org.jesterj.ingest.processors.LogAndDrop;
import org.jesterj.ingest.routers.DuplicateToAll;
import org.jesterj.ingest.scanners.SimpleFileScanner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.copyright.easiertest.EasierMocks.prepareMocks;
import static com.copyright.easiertest.EasierMocks.replay;
import static com.copyright.easiertest.EasierMocks.reset;
import static com.copyright.easiertest.EasierMocks.verify;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class PlanImplTest {

  private static final String LOG_AND_DROP = "log_and_drop";
  private static final String SCAN_FOO_BAR = "scan_foo_bar";

  @ObjectUnderTest PlanImpl plan;
  @Mock private Session sessionMock;
  @Mock private PreparedStatement prepStatementMock;
  @Mock private BoundStatement boundMock;
  @Mock private ResultSet rsMock;
  @Mock private Step stepMock;

  public PlanImplTest() {
    prepareMocks(this);
  }

  @Before
  public void setUp() {
    reset();
  }

  @After
  public void tearDown() {
    verify();
  }

  @Test
  public void testSimple2Step() throws NoSuchFieldException, IllegalAccessException {
    replay();
    PlanImpl.Builder planBuilder = new PlanImpl.Builder();
    SimpleFileScanner.Builder scannerBuilder = new SimpleFileScanner.Builder();
    StepImpl.Builder dropStepBuilder = new StepImpl.Builder();

    scannerBuilder.withRoot(new File("/Users/gus/foo/bar")).named(SCAN_FOO_BAR).batchSize(10)
    .routingBy(new DuplicateToAll.Builder().named("foo-dup"));

    dropStepBuilder.named(LOG_AND_DROP).batchSize(10).withProcessor(
        new LogAndDrop.Builder().withLogLevel(Level.ERROR)
    );

    planBuilder
        .named("testSimple2Step")
        .addStep(scannerBuilder)
        .addStep(dropStepBuilder, SCAN_FOO_BAR)
        .withIdField("id");
    Plan plan = planBuilder.build();

    // silly coverage stuff...
    plan.acceptJiniRequests();
    plan.denyJiniRequests();
    plan.advertise();
    plan.stopAdvertising();
    assertNotNull(plan.visualize());
    assertFalse(plan.readyForJiniRequests());
    assertFalse(plan.isActive());

    assertNull(plan.findStep(null));
    assertNull(plan.findStep("foo"));

    Step[] exes = plan.getExecutableSteps();
    assertEquals(exes.length, 2);
    System.out.println(exes[0].getName());
    System.out.println(exes[1].getName());
    assertEquals(1, Stream.of(exes).filter(foo -> SCAN_FOO_BAR.equals(foo.getName())).collect(Collectors.toList()).size());
    assertEquals(1, Stream.of(exes).filter(foo -> LOG_AND_DROP.equals(foo.getName())).collect(Collectors.toList()).size());

    Step scanStep = plan.findStep(SCAN_FOO_BAR);
    Step dropStep = plan.findStep(LOG_AND_DROP);
    assertNotNull(scanStep);
    assertNotNull(dropStep);
    Field router = StepImpl.class.getDeclaredField("router");
    router.setAccessible(true);
    Object object = router.get(scanStep);
    assertNotNull(object);
    assertEquals(object.getClass(), DuplicateToAll.class);
    assertEquals(SCAN_FOO_BAR, scanStep.getName());
    assertEquals(LOG_AND_DROP, dropStep.getName());
    Step[] foo = scanStep.getNext(new DocumentImpl(null, "foo", plan, Document.Operation.NEW, (Scanner) scanStep));
    assertEquals(LOG_AND_DROP, foo[0].getName());

  }

  @Test(expected = RuntimeException.class)
  public void testFailInvalidName() {
    replay();
    PlanImpl.Builder planBuilder = new PlanImpl.Builder();
    SimpleFileScanner.Builder scannerBuilder = new SimpleFileScanner.Builder();
    StepImpl.Builder dropStepBuilder = new StepImpl.Builder();

    scannerBuilder.withRoot(new File("/Users/gus/foo/bar")).named(SCAN_FOO_BAR).batchSize(10);

    dropStepBuilder.named(LOG_AND_DROP).batchSize(10).withProcessor(
        new LogAndDrop.Builder().withLogLevel(Level.ERROR)
    );

    planBuilder
        .named("2testSimple2Step")
        .addStep(scannerBuilder)
        .addStep(dropStepBuilder, SCAN_FOO_BAR)
        .withIdField("id");
    planBuilder.build();

  }

  @Test(expected = RuntimeException.class)
  public void testFailNullStepName() {
    replay();
    PlanImpl.Builder planBuilder = new PlanImpl.Builder();
    SimpleFileScanner.Builder scannerBuilder = new SimpleFileScanner.Builder();
    StepImpl.Builder dropStepBuilder = new StepImpl.Builder();

    scannerBuilder.withRoot(new File("/Users/gus/foo/bar")).named("legal_name").batchSize(10);

    dropStepBuilder.batchSize(10).withProcessor(             /// ERROR! step should have a name
        new LogAndDrop.Builder().withLogLevel(Level.ERROR)
    );

    planBuilder
        .named("testSimple2Step")
        .addStep(scannerBuilder)
        .addStep(dropStepBuilder, "legal_name")
        .withIdField("id");
    planBuilder.build();

  }


  @Test(expected = IllegalArgumentException.class)
  public void testScannerPredecessorNotAllowed() {
    replay();
    PlanImpl.Builder planBuilder = new PlanImpl.Builder();
    SimpleFileScanner.Builder scannerBuilder = new SimpleFileScanner.Builder();
    StepImpl.Builder dropStepBuilder = new StepImpl.Builder();

    scannerBuilder.withRoot(new File("/Users/gus/foo/bar")).named(SCAN_FOO_BAR).batchSize(10);

    dropStepBuilder.named(LOG_AND_DROP).batchSize(10).withProcessor(
        new LogAndDrop.Builder().withLogLevel(Level.ERROR)
    );

    planBuilder
        .named("testSimple2Step")
        .addStep(scannerBuilder, LOG_AND_DROP)
        .addStep(dropStepBuilder);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNonScannerPredecessorRequired() {
    replay();
    PlanImpl.Builder planBuilder = new PlanImpl.Builder();
    SimpleFileScanner.Builder scannerBuilder = new SimpleFileScanner.Builder();
    StepImpl.Builder dropStepBuilder = new StepImpl.Builder();

    scannerBuilder.withRoot(new File("/Users/gus/foo/bar")).named(SCAN_FOO_BAR).batchSize(10);

    dropStepBuilder.named(LOG_AND_DROP).batchSize(10).withProcessor(
        new LogAndDrop.Builder().withLogLevel(Level.ERROR)
    );

    planBuilder
        .named("testSimple2Step")
        .addStep(scannerBuilder)
        .addStep(dropStepBuilder);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRejectDuplicateName() {
    replay();
    PlanImpl.Builder planBuilder = new PlanImpl.Builder();
    SimpleFileScanner.Builder scannerBuilder = new SimpleFileScanner.Builder();
    StepImpl.Builder dropStepBuilder = new StepImpl.Builder();

    scannerBuilder.withRoot(new File("/Users/gus/foo/bar")).named(SCAN_FOO_BAR).batchSize(10);

    dropStepBuilder.named(SCAN_FOO_BAR).batchSize(10).withProcessor(
        new LogAndDrop.Builder().withLogLevel(Level.ERROR)
    );

    planBuilder
        .named("testSimple2Step")
        .addStep(scannerBuilder)
        .addStep(dropStepBuilder, LOG_AND_DROP);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRejectUnknownName() {
    replay();
    PlanImpl.Builder planBuilder = new PlanImpl.Builder();
    SimpleFileScanner.Builder scannerBuilder = new SimpleFileScanner.Builder();
    StepImpl.Builder dropStepBuilder = new StepImpl.Builder();

    scannerBuilder.withRoot(new File("/Users/gus/foo/bar")).named(SCAN_FOO_BAR).batchSize(10);

    dropStepBuilder.named(SCAN_FOO_BAR).batchSize(10).withProcessor(
        new LogAndDrop.Builder().withLogLevel(Level.ERROR)
    );

    planBuilder
        .named("testSimple2Step")
        .addStep(scannerBuilder)
        .addStep(dropStepBuilder, "foo");
  }
  @Test
  public void testActivate() {
    LinkedHashMap<String, Step> stringStepLinkedHashMap = new LinkedHashMap<>();
    stringStepLinkedHashMap.put("foo", stepMock);
    expect(plan.getStepsMap()).andReturn(stringStepLinkedHashMap);
    stepMock.activate();
    plan.setActive(true);
    replay();
    plan.activate();
  }

  @Test
  public void testDeactivate() {
    LinkedHashMap<String, Step> stringStepLinkedHashMap = new LinkedHashMap<>();
    stringStepLinkedHashMap.put("foo", stepMock);
    expect(plan.getStepsMap()).andReturn(stringStepLinkedHashMap);
    stepMock.deactivate();
    plan.setActive(false);
    replay();
    plan.deactivate();
  }

}
