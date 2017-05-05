package org.jesterj.ingest.routers;


import com.copyright.easiertest.Mock;
import com.copyright.easiertest.ObjectUnderTest;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.Step;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;

import static com.copyright.easiertest.EasierMocks.*;
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
    Step[] steps = router.route(docMock, stepList);
    assertTrue(Arrays.asList(steps).contains(stepMock1));
    assertTrue(Arrays.asList(steps).contains(stepMock2));
  }
}
