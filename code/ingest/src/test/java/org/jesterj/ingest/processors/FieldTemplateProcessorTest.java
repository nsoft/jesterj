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
import org.jesterj.ingest.model.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.copyright.easiertest.EasierMocks.prepareMocks;
import static com.copyright.easiertest.EasierMocks.replay;
import static com.copyright.easiertest.EasierMocks.reset;
import static com.copyright.easiertest.EasierMocks.verify;
import static org.easymock.EasyMock.expect;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 9/23/16
 */
public class FieldTemplateProcessorTest {

  @Mock private Document docMock;

  public FieldTemplateProcessorTest() {
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
  public void testProcessSingleTemplateValue() {
    List<String> strings = new ArrayList<>();
    strings.add("This is all $foobar[0]");
    expect(docMock.removeAll("templ")).andReturn(strings);
    Map<String, Collection<String>> map = new HashMap<>();
    map.put("foobar", Collections.singletonList("good"));
    expect(docMock.getId()).andReturn("bar");
    expect(docMock.asMap()).andReturn(map);
    expect(docMock.put("templ", "This is all good")).andReturn(true);
    replay();
    FieldTemplateProcessor proc = new FieldTemplateProcessor.Builder().named("foo").withTemplatesIn("templ").build();
    proc.processDocument(docMock);
  }

  @Test
  public void testProcessMultipleTemplateValue() {
    List<String> strings = new ArrayList<>();
    strings.add("This is all $foobar[0].");
    strings.add("I like $foobar[1].");
    expect(docMock.removeAll("templ")).andReturn(strings);
    Map<String, Collection<String>> map = new HashMap<>();
    map.put("foobar", Arrays.asList("good", "pie"));
    expect(docMock.getId()).andReturn("bar").anyTimes();
    expect(docMock.asMap()).andReturn(map);
    expect(docMock.put("templ", "This is all good.")).andReturn(true);
    expect(docMock.put("templ", "I like pie.")).andReturn(true);
    replay();
    FieldTemplateProcessor proc = new FieldTemplateProcessor.Builder().named("foo").withTemplatesIn("templ").build();
    proc.processDocument(docMock);
  }
}
