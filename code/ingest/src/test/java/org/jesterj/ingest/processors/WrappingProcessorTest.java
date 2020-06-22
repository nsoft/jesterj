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

import com.copyright.easiertest.BeanTester;
import com.copyright.easiertest.Mock;
import com.copyright.easiertest.ObjectUnderTest;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.copyright.easiertest.EasierMocks.prepareMocks;
import static com.copyright.easiertest.EasierMocks.replay;
import static com.copyright.easiertest.EasierMocks.reset;
import static com.copyright.easiertest.EasierMocks.verify;
import static org.apache.tinkerpop.gremlin.process.traversal.Compare.eq;
import static org.easymock.EasyMock.aryEq;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.or;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WrappingProcessorTest {
  @ObjectUnderTest private WrappingProcessor obj;
  @Mock private Document mockDocument;
  @Mock private Document mockProcessedDocument1;
  @Mock private Document mockProcessedDocument2;
  @Mock private DocumentProcessor wrapped;

  public WrappingProcessorTest() {
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
  public void testProcessDocumentSuccessful() {
    expect(obj.getWrapped()).andReturn(wrapped);
    obj.before(mockDocument);
    Document[] documents = {mockProcessedDocument1, mockProcessedDocument2};
    expect(wrapped.processDocument(mockDocument))
        .andReturn(documents);
    expect(obj.success(documents)).andReturn(documents);
    obj.always(mockDocument);

    replay();
    Document[] results = obj.processDocument(mockDocument);
    assertEquals(results[0], mockProcessedDocument1);
    assertEquals(results[1], mockProcessedDocument2);
  }

  @Test
  public void testProcessDocumentExceptionRethrow() {
    expect(obj.getWrapped()).andReturn(wrapped);
    obj.before(mockDocument);
    RuntimeException throwable = new RuntimeException();
    expect(wrapped.processDocument(mockDocument))
        .andThrow(throwable);
    Document[][] documents = {{null}};
    expect(obj.wrapRef(null)).andReturn(documents);
    expect(obj.error(throwable, mockDocument,  documents)).andReturn(true);
    obj.always(mockDocument);

    replay();
    try {
      obj.processDocument(mockDocument);
    } catch (RuntimeException e) {
      assertEquals(e, throwable);
    }
  }

  @Test
  public void testProcessDocumentExceptionHandleViolateNullResult() {
    expect(obj.getWrapped()).andReturn(wrapped);
    obj.before(mockDocument);
    RuntimeException throwable = new RuntimeException();
    expect(wrapped.processDocument(mockDocument))
        .andThrow(throwable);
    Document[][] fail = {null};
    expect(obj.wrapRef(null)).andReturn(fail);
    expect(obj.error(eq(throwable), eq(mockDocument),  aryEq(fail))).andReturn(false);
    obj.always(mockDocument);

    replay();
    try {
      obj.processDocument(mockDocument);
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().endsWith("results[0] is null"));
    }
  }

  @Test
  public void testProcessDocumentExceptionHandleViolateNullDocument() {
    expect(obj.getWrapped()).andReturn(wrapped);
    obj.before(mockDocument);
    RuntimeException throwable = new RuntimeException();
    expect(wrapped.processDocument(mockDocument))
        .andThrow(throwable);
    Document[][] fail = {new Document[]{null}};
    expect(obj.wrapRef(null)).andReturn(fail);
    expect(obj.error(eq(throwable), eq(mockDocument),  aryEq(fail))).andReturn(false);
    obj.always(mockDocument);

    replay();
    try {
      obj.processDocument(mockDocument);
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().endsWith("null document at position 0"));
    }
  }

  @Test
  public void testProcessDocumentExceptionHandleReturnOriginal() {
    expect(obj.getWrapped()).andReturn(wrapped);
    obj.before(mockDocument);
    RuntimeException throwable = new RuntimeException();
    expect(wrapped.processDocument(mockDocument))
        .andThrow(throwable);
    Document[][] fail = {new Document[]{mockDocument}};
    // note that this is slightly unrealistic because the modification normally happens in
    // error() but this works around the "write parameter" we are using here to avoid a
    // complicated return type for little gain.
    expect(obj.wrapRef(null)).andReturn(fail);
    expect(obj.error(eq(throwable), eq(mockDocument),  aryEq(fail))).andReturn(false);
    obj.always(mockDocument);

    replay();
    try {
      obj.processDocument(mockDocument);
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().endsWith("null document at position 0"));
    }
  }

  @Test
  public void testProcessDocumentExceptionInSuccess() {
    expect(obj.getWrapped()).andReturn(wrapped);
    obj.before(mockDocument);
    Document[] documents = {mockProcessedDocument1, mockProcessedDocument2};
    expect(wrapped.processDocument(mockDocument))
        .andReturn(documents);
    RuntimeException exc = new RuntimeException();
    expect(obj.success(documents)).andThrow(exc);
    Document[][] fail = {new Document[]{mockDocument}};
    // note that this is slightly unrealistic because the modification normally happens in
    // error() but this works around the "write parameter" we are using here to avoid a
    // complicated return type for little gain.
    expect(obj.wrapRef(documents)).andReturn(fail);
    expect(obj.error(eq(exc), eq(mockDocument),  aryEq(fail))).andReturn(false);
    obj.always(mockDocument);

    replay();
    try {
      obj.processDocument(mockDocument);
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().endsWith("null document at position 0"));
    }
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testSimpleProperties() {
    replay();
    BeanTester tester = new BeanTester();
    Map<Class, Object> exampleTypes = tester.getExampleTypes();
    exampleTypes.put(DocumentProcessor.class, wrapped);
    tester.setExampleTypes(exampleTypes);
    tester.testBean(new WrappingProcessor());
  }

}
