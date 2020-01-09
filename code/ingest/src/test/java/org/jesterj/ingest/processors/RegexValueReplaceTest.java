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
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import static com.copyright.easiertest.EasierMocks.prepareMocks;
import static com.copyright.easiertest.EasierMocks.replay;
import static com.copyright.easiertest.EasierMocks.reset;
import static com.copyright.easiertest.EasierMocks.verify;
import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 9/11/16
 */
public class RegexValueReplaceTest {

  @ObjectUnderTest RegexValueReplace processor;
  @Mock private Document document;

  public RegexValueReplaceTest() {
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
  public void testReplaceSingleValue() {
    expect(processor.getField()).andReturn("foo").anyTimes();
    expect(processor.getRegex()).andReturn(Pattern.compile("(.*)\\s+bar$"));
    expect(processor.getReplace()).andReturn("$1BAR");
    List<String> singleValue = new ArrayList<>();
    singleValue.add("FOO bar");
    expect(document.get("foo")).andReturn(singleValue);
    Capture<Iterable<? extends String>> c = newCapture();
    //noinspection ConstantConditions
    expect(document.replaceValues(eq("foo"), capture(c))).andReturn(singleValue);
    replay();
    processor.processDocument(document);
    assertEquals(c.getValue().iterator().next(), "FOOBAR");
  }

  @Test
  public void testReplaceMultiValue() {
    expect(processor.getField()).andReturn("foo").anyTimes();
    expect(processor.getRegex()).andReturn(Pattern.compile("(.*)\\s+bar$")).anyTimes();
    expect(processor.getReplace()).andReturn("$1BAR").anyTimes();
    List<String> multivalued = new ArrayList<>();
    multivalued.add("FOO bar"); // match 1
    multivalued.add("FEW bar"); // match 2
    multivalued.add("FEE baz"); // unmatched unmodified
    expect(document.get("foo")).andReturn(multivalued);
    Capture<Iterable<? extends String>> c = newCapture();
    //noinspection ConstantConditions
    expect(document.replaceValues(eq("foo"), capture(c))).andReturn(multivalued);
    replay();
    processor.processDocument(document);
    Iterable<? extends String> newValues = c.getValue();
    Iterator<? extends String> iterator = newValues.iterator();
    assertEquals(iterator.next(), "FOOBAR");
    assertEquals(iterator.next(), "FEWBAR");
    assertEquals(iterator.next(), "FEE baz");
  }
}
