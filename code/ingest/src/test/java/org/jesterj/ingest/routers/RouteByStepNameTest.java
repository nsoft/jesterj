package org.jesterj.ingest.routers;


import com.copyright.easiertest.Mock;
import com.copyright.easiertest.ObjectUnderTest;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.NextSteps;
import org.jesterj.ingest.model.Step;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedHashMap;

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
    expect(docMock1.getFirstValue(RouteByStepName.JESTERJ_NEXT_STEP_NAME)).andReturn("foo");
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
    expect(docMock2.getFirstValue(RouteByStepName.JESTERJ_NEXT_STEP_NAME)).andReturn("bar");
    replay();
    NextSteps steps = router.route(docMock2);
    assertEquals(1, steps.size());
    assertTrue(steps.list().contains(stepMock2));
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
