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

package org.jesterj.ingest.processors;

import com.copyright.easiertest.Mock;
import com.copyright.easiertest.ObjectUnderTest;
import org.easymock.Capture;
import org.jesterj.ingest.model.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.copyright.easiertest.EasierMocks.replay;
import static com.copyright.easiertest.EasierMocks.reset;
import static com.copyright.easiertest.EasierMocks.verify;
import static com.copyright.easiertest.EasierMocks.*;
import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 9/11/16
 */
public class SetStaticValueTest {

  @ObjectUnderTest
  SetStaticValue processor;
  @Mock private Document document;

  public SetStaticValueTest() {
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
  public void testDefaluts() {
    expect(processor.isAddValueToExisting()).andReturn(true).anyTimes();
    expect(processor.isEditExisting()).andReturn(true).anyTimes();
    expect(processor.getFieldToInsert()).andReturn("foo").anyTimes();
    expect(processor.getValueToInsert()).andReturn("bar").anyTimes();
    Capture<? extends String> c = newCapture();
    expect(document.put(eq("foo"),capture(c))).andReturn(true);
    replay();
    processor.processDocument(document);
    assertEquals(c.getValue(), "bar");
  }

  @Test
  public void testReplace() {
    expect(processor.isAddValueToExisting()).andReturn(false).anyTimes();
    expect(processor.isEditExisting()).andReturn(true).anyTimes();
    expect(processor.getFieldToInsert()).andReturn("foo").anyTimes();
    expect(processor.getValueToInsert()).andReturn("bar").anyTimes();
    expect(document.removeAll("foo")).andReturn(new ArrayList<>());
    Capture<? extends String> c = newCapture();
    expect(document.put(eq("foo"),capture(c))).andReturn(true);
    replay();
    processor.processDocument(document);
    assertEquals(c.getValue(), "bar");
  }

  @Test
  public void testPreserveExisting() {
    expect(processor.isAddValueToExisting()).andReturn(true).anyTimes();
    expect(processor.isEditExisting()).andReturn(false).anyTimes();
    expect(processor.getFieldToInsert()).andReturn("foo").anyTimes();
    expect(processor.getValueToInsert()).andReturn("bar").anyTimes();
    expect(document.get("foo")).andReturn(List.of("something"));
    replay();
    processor.processDocument(document);
  }
}
