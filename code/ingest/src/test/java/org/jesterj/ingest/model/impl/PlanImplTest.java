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
import guru.nidi.graphviz.engine.Format;
import org.apache.logging.log4j.Level;
import org.jesterj.ingest.model.*;
import org.jesterj.ingest.model.Scanner;
import org.jesterj.ingest.processors.LogAndDrop;
import org.jesterj.ingest.routers.DuplicateToAll;
import org.jesterj.ingest.scanners.SimpleFileScanner;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.copyright.easiertest.EasierMocks.prepareMocks;
import static com.copyright.easiertest.EasierMocks.replay;
import static com.copyright.easiertest.EasierMocks.reset;
import static com.copyright.easiertest.EasierMocks.verify;
import static org.easymock.EasyMock.expect;
import static org.jesterj.ingest.model.impl.ScannerImpl.SCAN_ORIGIN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PlanImplTest {

  private static final String LOG_AND_DROP = "log_and_drop";
  private static final String SCAN_FOO_BAR = "scan_foo_bar";

  @ObjectUnderTest
  PlanImpl plan;
  @Mock
  private Session sessionMock;
  @Mock
  private PreparedStatement prepStatementMock;
  @Mock
  private BoundStatement boundMock;
  @Mock
  private ResultSet rsMock;
  @Mock
  private Step stepMock;
  @Mock
  private Step stepMockA;
  @Mock
  private Step stepMockB;
  @Mock
  private Step stepMockC;
  @Mock
  private Step stepMockD;
  @Mock
  private Step stepMockE;
  @Mock
  private Step stepMockF;
  @Mock
  private Scanner scannerMock;
  private int cHasBeenDeactivated = 0;
  private int eHasBeenDeactivated = 0;

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

    assertNotNull(plan.visualize(Format.PNG)); // todo test this more explicitly
    assertFalse(plan.isActive());

    assertNull(plan.findStep(null));
    assertNull(plan.findStep("foo"));

    Step[] exes = plan.getExecutableSteps();
    assertEquals(exes.length, 2);
    System.out.println(exes[0].getName());
    System.out.println(exes[1].getName());
    assertEquals(1, Stream.of(exes).filter(foo -> SCAN_FOO_BAR.equals(foo.getName())).count());
    assertEquals(1, Stream.of(exes).filter(foo -> LOG_AND_DROP.equals(foo.getName())).count());

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
    NextSteps foo = scanStep.getNextSteps(new DocumentImpl(null, "foo", plan, Document.Operation.NEW, (Scanner) scanStep, SCAN_ORIGIN));
    assertEquals(LOG_AND_DROP, foo.list().get(0).getName());

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
    expect(plan.getName()).andReturn("My_Plan");
    expect(plan.getStepsMap()).andReturn(stringStepLinkedHashMap);
    stepMock.activate();
    plan.register();
    plan.setActive(true);
    replay();
    plan.activate();
  }

  @Test
  public void testDeactivateMinimal() {
    LinkedHashMap<String, Step> stringStepLinkedHashMap = new LinkedHashMap<>();
    stringStepLinkedHashMap.put("foo", stepMock);
    stringStepLinkedHashMap.put("bar", scannerMock);
    expect(scannerMock.isActive()).andReturn(true);
    expect(stepMock.isActive()).andReturn(true);
    expect(stepMock.isActivePriorSteps()).andReturn(false);
    expect(plan.getStepsMap()).andReturn(stringStepLinkedHashMap).anyTimes();
    scannerMock.deactivate();
    LinkedHashMap<String, Step> nextFromScanner = new LinkedHashMap<>();
    nextFromScanner.put("foo", stepMock);
    expect(scannerMock.getNextSteps()).andReturn(nextFromScanner);
    expect(stepMock.getNextSteps()).andReturn(new LinkedHashMap<>());
    expect(plan.isActive()).andReturn(true);
    expect(plan.getName()).andReturn("My_Plan").anyTimes();
    expect(scannerMock.getName()).andReturn("My_Scanner").anyTimes();
    stepMock.deactivate();
    plan.setActive(false);
    replay();
    plan.deactivate();
  }

  @Test
  public void testDeactivateWithJoinPointBFirst() {
    testDeactivateWithJoinPoint(true);
  }

  @Test
  public void testDeactivateWithJoinPointDFirst() {
    testDeactivateWithJoinPoint(false);
  }

  public void testDeactivateWithJoinPoint(boolean bFirst) {
    // Below, E is a join point, and C should be deactivated before E
    //
    //  Scanner --> A --->B --> C ---> E ---> F
    //               \--->D-----------/
    //
    // should proceed in order: Scanner, A, B, C, D  (where D has been added to next level)

    StepMockWrapper wrappedA = new StepMockWrapper(stepMockA, false, false, "A");
    StepMockWrapper wrappedB = new StepMockWrapper(stepMockB, false, false, "B");
    StepMockWrapper wrappedC = new StepMockWrapper(stepMockC, true, false, "C");
    StepMockWrapper wrappedD = new StepMockWrapper(stepMockD, false, false, "D");
    StepMockWrapper wrappedE = new StepMockWrapper(stepMockE, false, true, "E");
    StepMockWrapper wrappedF = new StepMockWrapper(stepMockF, false, false, "F");

    LinkedHashMap<String, Step> stringStepLinkedHashMap = new LinkedHashMap<>();
    stringStepLinkedHashMap.put("A", wrappedA);
    stringStepLinkedHashMap.put("B", wrappedB);
    stringStepLinkedHashMap.put("C", wrappedC);
    stringStepLinkedHashMap.put("D", wrappedD);
    stringStepLinkedHashMap.put("E", wrappedE);
    stringStepLinkedHashMap.put("F", wrappedF);
    stringStepLinkedHashMap.put("Scanner", scannerMock);

    LinkedHashMap<String, Step> nextFromScanner = new LinkedHashMap<>();
    nextFromScanner.put("A", wrappedA);

    LinkedHashMap<String, Step> nextFromA = new LinkedHashMap<>();
    if (bFirst) {
      nextFromA.put("B", wrappedB);
      nextFromA.put("D", wrappedD);
    } else {
      nextFromA.put("D", wrappedD);
      nextFromA.put("B", wrappedB);
    }
    LinkedHashMap<String, Step> nextFromB = new LinkedHashMap<>();
    nextFromB.put("C", wrappedC);
    LinkedHashMap<String, Step> nextFromC = new LinkedHashMap<>();
    nextFromC.put("E", wrappedE);
    LinkedHashMap<String, Step> nextFromD = new LinkedHashMap<>();
    nextFromD.put("E", wrappedE);
    LinkedHashMap<String, Step> nextFromE = new LinkedHashMap<>();
    nextFromE.put("F", wrappedF);
    LinkedHashMap<String, Step> nextFromF = new LinkedHashMap<>();

    expect(scannerMock.getNextSteps()).andReturn(nextFromScanner).anyTimes();
    expect(stepMockA.getNextSteps()).andReturn(nextFromA).anyTimes();
    expect(stepMockB.getNextSteps()).andReturn(nextFromB).anyTimes();
    expect(stepMockC.getNextSteps()).andReturn(nextFromC).anyTimes();
    expect(stepMockD.getNextSteps()).andReturn(nextFromD).anyTimes();
    expect(stepMockE.getNextSteps()).andReturn(nextFromE).anyTimes();
    expect(stepMockF.getNextSteps()).andReturn(nextFromF).anyTimes();

    expect(plan.getStepsMap()).andReturn(stringStepLinkedHashMap).anyTimes();
    expect(stepMockA.isActivePriorSteps()).andReturn(false);
    expect(stepMockB.isActivePriorSteps()).andReturn(false);
    expect(stepMockC.isActivePriorSteps()).andReturn(false);
    expect(stepMockD.isActivePriorSteps()).andReturn(false);
    expect(stepMockE.isActivePriorSteps()).andReturn(true); // first time
    expect(stepMockE.isActivePriorSteps()).andReturn(false); // second time
    expect(stepMockF.isActivePriorSteps()).andReturn(false).times(2); //second time for "next level"

    expect(scannerMock.isActive()).andReturn(true);
    expect(stepMockA.isActive()).andReturn(true);
    expect(stepMockB.isActive()).andReturn(true);
    expect(stepMockC.isActive()).andReturn(true);
    expect(stepMockD.isActive()).andReturn(true);
    expect(stepMockE.isActive()).andReturn(true);
    expect(stepMockF.isActive()).andReturn(true);
    expect(stepMockF.isActive()).andReturn(false);// second time
    expect(stepMockE.isActive()).andReturn(false);

    scannerMock.deactivate();
    stepMockA.deactivate();
    stepMockB.deactivate();
    stepMockC.deactivate();
    stepMockD.deactivate();
    stepMockE.deactivate();
    stepMockF.deactivate();

    expect(plan.isActive()).andReturn(true);
    expect(plan.getName()).andReturn("My_Plan").anyTimes();
    expect(scannerMock.getName()).andReturn("My_Scanner").anyTimes();
    plan.setActive(false);
    replay();
    plan.deactivate();
  }


  private class StepMockWrapper implements Step {
    private final Step mock;
    private final boolean isC;
    private final boolean isE;
    private final String name;


    private StepMockWrapper(Step mock, boolean isC, boolean isE, String name) {
      this.mock = mock;
      this.isC = isC;
      this.isE = isE;
      this.name = name;
    }

    @Override
    public void activate() {
      mock.activate();
    }

    @Override
    public String toString() {
      return this.name;
    }

    @Override
    public void deactivate() {
      System.out.println("Deactivating " + this.name);
      if (isC) {
        assertTrue("c=" + cHasBeenDeactivated + " e=" + eHasBeenDeactivated, cHasBeenDeactivated == 0 || eHasBeenDeactivated == 0);
        cHasBeenDeactivated++;
      }
      if (isE) {
        eHasBeenDeactivated++;
      }
      mock.deactivate();
    }

    @Override
    public boolean isActive() {
      return mock.isActive();
    }

    @Override
    public String getName() {
      return mock.getName();
    }

    @Override
    public boolean isValidName(String name) {
      return mock.isValidName(name);
    }

    @Override
    public int getBatchSize() {
      return mock.getBatchSize();
    }

    @Override
    public NextSteps getNextSteps(Document d) {
      return mock.getNextSteps(d);
    }

    @Override
    public Plan getPlan() {
      return mock.getPlan();
    }

    @Override
    public void sendToNext(Document doc) {
      mock.sendToNext(doc);
    }

    @Override
    public Set<String> getOutputDestinationNames() {
      return mock.getOutputDestinationNames();
    }

    @Override
    public Set<Step> getDownstreamOutputSteps() {
      return mock.getDownstreamOutputSteps();
    }

    @Override
    public boolean isOutputStep() {
      return mock.isOutputStep();
    }


    @Override
    public LinkedHashMap<String, Step> getNextSteps() {
      return mock.getNextSteps();
    }

    @Override
    public boolean isActivePriorSteps() {
      return mock.isActivePriorSteps();
    }

    @Override
    public List<Step> getPriorSteps() {
      return mock.getPriorSteps();
    }

    @Override
    public void addPredecessor(StepImpl obj) {
      mock.addPredecessor(obj);
    }

    @Override
    public Router getRouter() {
      return mock.getRouter();
    }

    @Override
    public boolean add(@NotNull Document document) {
      return mock.add(document);
    }

    @Override
    public boolean offer(@NotNull Document document) {
      return mock.offer(document);
    }

    @Override
    public void put(@NotNull Document document) throws InterruptedException {
      mock.put(document);
    }

    @Override
    public boolean offer(Document document, long timeout, @NotNull TimeUnit unit) throws InterruptedException {
      return mock.offer(document, timeout, unit);
    }

    @NotNull
    @Override
    public Document take() throws InterruptedException {
      return mock.take();
    }

    @Nullable
    @Override
    public Document poll(long timeout, @NotNull TimeUnit unit) throws InterruptedException {
      return mock.poll(timeout, unit);
    }

    @Override
    public int remainingCapacity() {
      return mock.remainingCapacity();
    }

    @Override
    public boolean remove(Object o) {
      return mock.remove(o);
    }

    @Override
    public boolean contains(Object o) {
      return mock.contains(o);
    }

    @Override
    public int drainTo(@NotNull Collection<? super Document> c) {
      return mock.drainTo(c);
    }

    @Override
    public int drainTo(@NotNull Collection<? super Document> c, int maxElements) {
      return mock.drainTo(c, maxElements);
    }

    @Override
    public Document remove() {
      return mock.remove();
    }

    @Override
    public Document poll() {
      return mock.poll();
    }

    @Override
    public Document element() {
      return mock.element();
    }

    @Override
    public Document peek() {
      return mock.peek();
    }

    @Override
    public int size() {
      return mock.size();
    }

    @Override
    public boolean isEmpty() {
      return mock.isEmpty();
    }

    @NotNull
    @Override
    public Iterator<Document> iterator() {
      return mock.iterator();
    }

    @NotNull
    @Override
    public Object[] toArray() {
      return mock.toArray();
    }

    @NotNull
    @Override
    public <T> T[] toArray(@NotNull T[] a) {
      return mock.toArray(a);
    }

    @Override
    public <T> T[] toArray(IntFunction<T[]> generator) {
      return mock.toArray(generator);
    }

    @Override
    public boolean containsAll(@NotNull Collection<?> c) {
      return mock.containsAll(c);
    }

    @Override
    public boolean addAll(@NotNull Collection<? extends Document> c) {
      return mock.addAll(c);
    }

    @Override
    public boolean removeAll(@NotNull Collection<?> c) {
      return mock.removeAll(c);
    }

    @Override
    public boolean removeIf(Predicate<? super Document> filter) {
      return mock.removeIf(filter);
    }

    @Override
    public boolean retainAll(@NotNull Collection<?> c) {
      return mock.retainAll(c);
    }

    @Override
    public void clear() {
      mock.clear();
    }

    @Override
    public Spliterator<Document> spliterator() {
      return mock.spliterator();
    }

    @Override
    public Stream<Document> stream() {
      return mock.stream();
    }

    @Override
    public Stream<Document> parallelStream() {
      return mock.parallelStream();
    }

    @Override
    public void forEach(Consumer<? super Document> action) {
      mock.forEach(action);
    }

    @Override
    public void run() {
      mock.run();
    }

    @Override
    public void executeDeferred() {
      mock.executeDeferred();
    }

    @Override
    public void addDeferred(Runnable builderAction) {
      mock.addDeferred(builderAction);
    }

  }
}
