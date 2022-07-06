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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DuplicateToAllTest {

  @ObjectUnderTest DuplicateToAll router;
  @Mock Step stepMock1;
  @Mock Step stepMock2;
  @Mock Document docMock;


  public DuplicateToAllTest() {
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
  public void testRoute() {
    LinkedHashMap<String,Step> stepList = new LinkedHashMap<>();
    stepList.put("foo",stepMock1);
    stepList.put("bar",stepMock2);
    replay();
    NextSteps steps = router.route(docMock, stepList);
    assertTrue(steps.list().contains(stepMock1));
    assertTrue(steps.list().contains(stepMock2));
  }

  @Test
  public void testBuilder() {
    replay();
    DuplicateToAll router = new DuplicateToAll.Builder().named("foo").build();
    assertEquals("foo", router.getName());
  }
}
