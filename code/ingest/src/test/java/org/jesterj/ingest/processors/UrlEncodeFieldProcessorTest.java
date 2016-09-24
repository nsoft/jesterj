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
import java.util.List;

import static com.copyright.easiertest.EasierMocks.prepareMocks;
import static com.copyright.easiertest.EasierMocks.replay;
import static com.copyright.easiertest.EasierMocks.reset;
import static com.copyright.easiertest.EasierMocks.verify;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class UrlEncodeFieldProcessorTest {


  @Mock Document docMock;

  public UrlEncodeFieldProcessorTest() {
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
  public void testEncodeField() {
    UrlEncodeFieldProcessor proc = new UrlEncodeFieldProcessor.Builder()
        .named("foo")
        .encodingField("toEncode")
        .withCharacterEncoding("UTF-8")
        .build();

    List<String> values = new ArrayList<>();
    values.add("semi ;");
    values.add("slash /");
    values.add("question ?");
    values.add("colon :");
    values.add("at @");
    values.add("and &");
    values.add("equal =");
    values.add("plus +");
    values.add("dollar $");
    values.add("comma ,");

    expect(docMock.get("toEncode")).andReturn(values);
    replay();
    proc.processDocument(docMock);

    assertEquals("semi+%3B", values.get(0));
    assertEquals("slash+%2F", values.get(1));
    assertEquals("question+%3F", values.get(2));
    assertEquals("colon+%3A", values.get(3));
    assertEquals("at+%40", values.get(4));
    assertEquals("and+%26", values.get(5));
    assertEquals("equal+%3D", values.get(6));
    assertEquals("plus+%2B", values.get(7));
    assertEquals("dollar+%24", values.get(8));
    assertEquals("comma+%2C", values.get(9));
  }


  @Test(expected = RuntimeException.class)
  public void testCharsetNotFound() {
    UrlEncodeFieldProcessor proc = new UrlEncodeFieldProcessor.Builder()
        .named("foo")
        .encodingField("toEncode")
        .withCharacterEncoding("NO_SUCH_ENCODING!")
        .build();

    List<String> values = new ArrayList<>();
    values.add("semi ;");
    values.add("slash /");
    values.add("question ?");
    values.add("colon :");
    values.add("at @");
    values.add("and &");
    values.add("equal =");
    values.add("plus +");
    values.add("dollar $");
    values.add("comma ,");

    expect(docMock.get("toEncode")).andReturn(values);
    replay();
    proc.processDocument(docMock);
  }
}
