package org.jesterj.ingest.routers;


import com.copyright.easiertest.Mock;
import com.copyright.easiertest.ObjectUnderTest;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.NextSteps;
import org.jesterj.ingest.model.Step;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.copyright.easiertest.EasierMocks.replay;
import static com.copyright.easiertest.EasierMocks.reset;
import static com.copyright.easiertest.EasierMocks.verify;
import static com.copyright.easiertest.EasierMocks.*;
import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;

public class DuplicateToAllTest {

  @ObjectUnderTest DuplicateToAll router;
  @Mock Step stepMock1;
  @Mock Step stepMock2;
  @Mock Document docMock;
  @Mock Step stepMock; // step to which router is attached
  @Mock private NextSteps nextStepsMock;


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
    expect(router.createNextSteps(docMock)).andReturn(nextStepsMock);

    replay();
    NextSteps steps = router.route(docMock);
    assertEquals(nextStepsMock, steps);
  }

  @Test
  public void testBuilder() {
    replay();
    DuplicateToAll router = new DuplicateToAll.Builder().named("foo").build();
    assertEquals("foo", router.getName());
  }
}
