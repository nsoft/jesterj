package org.jesterj.ingest.routers;


import com.copyright.easiertest.Mock;
import com.copyright.easiertest.ObjectUnderTest;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.NextSteps;
import org.jesterj.ingest.model.Status;
import org.jesterj.ingest.model.Step;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static com.copyright.easiertest.EasierMocks.prepareMocks;
import static com.copyright.easiertest.EasierMocks.replay;
import static com.copyright.easiertest.EasierMocks.reset;
import static com.copyright.easiertest.EasierMocks.verify;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RouteByStepNameTest {

  @ObjectUnderTest RouteByStepName router;
  @Mock Step stepMock1;
  @Mock Step stepMock2;
  @Mock Step stepMock3;
  @Mock Step stepMock4;
  @Mock Step stepMockNext1;
  @Mock Step stepMockNext2;
  @Mock Document docMock1;
  @Mock Document docMock2;
  @Mock Step stepMock; // step to which router is attached

  public RouteByStepNameTest() {
    prepareMocks(this);
  }

  @Before
  public void setUp() {
    reset();
  }

  @After
  public void tearDown(){
    verify();
  }

  @Test
  public void testRouteToFoo() {
    LinkedHashMap<String,Step> stepList = new LinkedHashMap<>();
    stepList.put("foo",stepMock1);
    stepList.put("bar",stepMock2);
    expect(router.getStep()).andReturn(stepMock);
    expect(stepMock.getNextSteps()).andReturn(stepList);
    expect(router.getKeyFieldName()).andReturn(RouteByStepName.JESTERJ_NEXT_STEP_NAME);
    expect(docMock1.getFirstValue(RouteByStepName.JESTERJ_NEXT_STEP_NAME)).andReturn("foo");
    expect(router.getValueToStepNameMap()).andReturn(new HashMap<>());
    router.updateExcludedDestinations(docMock1, stepMock1);
    docMock1.reportDocStatus();
    replay();
    NextSteps steps = router.route(docMock1);
    assertEquals(1, steps.size());
    assertTrue(steps.list().contains(stepMock1));

  }
  @Test
  public void testRouteToBarr() {
    LinkedHashMap<String,Step> stepList = new LinkedHashMap<>();
    stepList.put("foo",stepMock1);
    stepList.put("bar",stepMock2);
    expect(router.getStep()).andReturn(stepMock);
    expect(stepMock.getNextSteps()).andReturn(stepList);
    expect(router.getKeyFieldName()).andReturn(RouteByStepName.JESTERJ_NEXT_STEP_NAME);
    expect(docMock2.getFirstValue(RouteByStepName.JESTERJ_NEXT_STEP_NAME)).andReturn("bar");
    expect(router.getValueToStepNameMap()).andReturn(new HashMap<>());
    router.updateExcludedDestinations(docMock2, stepMock2);
    docMock2.reportDocStatus();
    replay();
    NextSteps steps = router.route(docMock2);
    assertEquals(1, steps.size());
    assertTrue(steps.list().contains(stepMock2));
  }

  @Test
  public void testRouteToBarrUsingAlternateField() {
    LinkedHashMap<String,Step> stepList = new LinkedHashMap<>();
    stepList.put("foo",stepMock1);
    stepList.put("bar",stepMock2);
    expect(router.getStep()).andReturn(stepMock);
    expect(stepMock.getNextSteps()).andReturn(stepList);
    expect(router.getKeyFieldName()).andReturn("foobar");
    expect(docMock2.getFirstValue("foobar")).andReturn("bar");
    expect(router.getValueToStepNameMap()).andReturn(new HashMap<>());
    router.updateExcludedDestinations(docMock2, stepMock2);
    docMock2.reportDocStatus();
    replay();
    NextSteps steps = router.route(docMock2);
    assertEquals(1, steps.size());
    assertTrue(steps.list().contains(stepMock2));
  }

  @Test
  public void testRouteToBarrUsingAlternateFieldAndMappingValue() {
    LinkedHashMap<String,Step> stepList = new LinkedHashMap<>();
    stepList.put("foo",stepMock1);
    stepList.put("bar",stepMock2);
    expect(router.getStep()).andReturn(stepMock);
    expect(stepMock.getNextSteps()).andReturn(stepList);
    expect(router.getKeyFieldName()).andReturn("foobar");
    expect(docMock2.getFirstValue("foobar")).andReturn("step named bar");
    expect(router.getValueToStepNameMap()).andReturn(Map.of("step named bar", "bar"));
    router.updateExcludedDestinations(docMock2, stepMock2);
    docMock2.reportDocStatus();
    replay();
    NextSteps steps = router.route(docMock2);
    assertEquals(1, steps.size());
    assertTrue(steps.list().contains(stepMock2));
  }

  @Test
  public void testUpdateExcludedDestinations() {



    expect(router.getStep()).andReturn(stepMock);
    expect(stepMock1.getName()).andReturn("fooName1").anyTimes();
    expect(stepMock2.getName()).andReturn("fooName2").anyTimes();
    expect(stepMock3.getName()).andReturn("fooName3").anyTimes();
    expect(stepMock4.getName()).andReturn("fooName4").anyTimes();
    expect(router.getName()).andReturn("routerName").anyTimes();

    expect(docMock1.isPlanOutput("fooName2")).andReturn(true);
    expect(docMock1.isPlanOutput("fooName1")).andReturn(false);  // < reason only one drop issued!
    docMock1.setStatus(Status.DROPPED,"fooName2","Document routed down path not leading to {} by {}", "fooName2", "routerName");
    docMock1.removeDownStreamOutputStep(router, "fooName2");

    Set<String> destsCurrent = new HashSet<>();
    destsCurrent.add("fooName1");
    destsCurrent.add("fooName2");
    destsCurrent.add("fooName3");
    destsCurrent.add("fooName4");
    Set<String> dests1 = new HashSet<>();
    dests1.add("fooName3");
    Set<String> dests2 = new HashSet<>();
    dests2.add("fooName3");
    dests2.add("fooName4");
    expect(stepMock.getOutputDestinationNames()).andReturn(destsCurrent);
    expect(stepMockNext1.getOutputDestinationNames()).andReturn(dests1);
    expect(stepMockNext2.getOutputDestinationNames()).andReturn(dests2);

    replay();
    router.updateExcludedDestinations(docMock1,stepMockNext1,stepMockNext2);

  }

  @Test
  public void testBuilder() {
    replay();
    RouteByStepName router = new RouteByStepName.Builder()
        .named("foo")
        .build();
    assertEquals("foo",router.getName());

  }
}
