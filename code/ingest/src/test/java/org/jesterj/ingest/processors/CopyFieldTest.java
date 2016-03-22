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
/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 3/21/16
 */

import com.copyright.easiertest.BeanTester;
import com.copyright.easiertest.Mock;
import com.copyright.easiertest.ObjectUnderTest;
import org.jesterj.ingest.model.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.copyright.easiertest.EasierMocks.prepareMocks;
import static com.copyright.easiertest.EasierMocks.replay;
import static com.copyright.easiertest.EasierMocks.reset;
import static com.copyright.easiertest.EasierMocks.verify;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class CopyFieldTest {
  @ObjectUnderTest private CopyField obj;
  @Mock private Document mockDocument;

  public CopyFieldTest() {
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
  public void testProcessDocumentRetain() {
    expect(obj.getFrom()).andReturn("foo");
    expect(obj.getInto()).andReturn("bar");
    List<String> strings = new ArrayList<>();
    expect(mockDocument.get("foo")).andReturn(strings);
    expect(mockDocument.putAll("bar", strings)).andReturn(true);
    expect(obj.isRetainOriginal()).andReturn(true);
    replay();
    obj.processDocument(mockDocument);
  }

  @Test
  public void testProcessDocumentDoNotRetain() {
    expect(obj.getFrom()).andReturn("foo").anyTimes();
    expect(obj.getInto()).andReturn("bar");
    List<String> strings = new ArrayList<>();
    expect(mockDocument.get("foo")).andReturn(strings);
    expect(mockDocument.putAll("bar", strings)).andReturn(true);
    expect(mockDocument.removeAll("foo")).andReturn(strings);
    expect(obj.isRetainOriginal()).andReturn(false);
    replay();
    obj.processDocument(mockDocument);
  }

  @Test
  public void testBuild() {
    replay();
    CopyField.Builder builder = new CopyField.Builder();
    builder.from("foo").into("bar").retainingOriginal(true);
    CopyField built = builder.build();

    assertEquals("foo", built.getFrom());
    assertEquals("bar", built.getInto());
    assertEquals(true, built.isRetainOriginal());

  }

  @Test
  public void testSimpleProperties() {
    replay();
    BeanTester tester = new BeanTester();
    tester.testBean(new CopyField());
  }
}
