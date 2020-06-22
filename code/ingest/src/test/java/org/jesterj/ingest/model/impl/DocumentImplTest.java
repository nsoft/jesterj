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

package org.jesterj.ingest.model.impl;

import com.copyright.easiertest.Mock;
import com.copyright.easiertest.ObjectUnderTest;
import com.google.common.collect.ArrayListMultimap;
import org.apache.commons.codec.digest.DigestUtils;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.model.Scanner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static com.copyright.easiertest.EasierMocks.prepareMocks;
import static com.copyright.easiertest.EasierMocks.replay;
import static com.copyright.easiertest.EasierMocks.reset;
import static com.copyright.easiertest.EasierMocks.verify;
import static org.easymock.EasyMock.expect;
import static org.jesterj.ingest.model.Status.DROPPED;
import static org.jesterj.ingest.model.Status.PROCESSING;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class DocumentImplTest {

  @ObjectUnderTest private DocumentImpl obj;
  @Mock private Scanner scannerMock;
  @Mock private Plan planMock;
  @Mock private StepImpl stepMock;


  public DocumentImplTest() {
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
  public void testGetFirstValue() {
    expect(scannerMock.getName()).andReturn("my_name").anyTimes();
    expect(planMock.getDocIdField()).andReturn("id").anyTimes();
    replay();
    DocumentImpl document = new DocumentImpl(new byte[]{}, "foo", planMock, Document.Operation.NEW, scannerMock);
    document.put("string", "stringvalue");
    DocumentImpl document2 = new DocumentImpl(new byte[]{}, "foo", planMock, Document.Operation.NEW, scannerMock);
    document2.put("string", "stringvalue");
//    System.out.println(document.getHash());
//    System.out.println(document2.getHash());

    assertEquals("stringvalue", document.getFirstValue("string"));
    assertNull(document.getFirstValue("unknown"));

  }

  @Test
  public void testHash() {
    expect(obj.getDelegateString()).andReturn("CAFE");
    expect(obj.getRawData()).andReturn("BABE".getBytes(StandardCharsets.UTF_8)).anyTimes();
    replay();
    assertEquals(DigestUtils.md5Hex("CAFEBABE".getBytes(StandardCharsets.UTF_8)).toUpperCase(), obj.getHash());
  }

  @Test
  public void testHashRawDataNull() {
    expect(obj.getDelegateString()).andReturn("CAFE");
    expect(obj.getRawData()).andReturn(null).anyTimes();
    replay();
    assertEquals(DigestUtils.md5Hex("CAFE".getBytes(StandardCharsets.UTF_8)).toUpperCase(), obj.getHash());
  }

  @Test
  public void testToEntry() {
    ArrayListMultimap<String, String> stupidFinalClass = ArrayListMultimap.create();
    expect(obj.getDelegate()).andReturn(stupidFinalClass);
    expect(obj.getSourceScannerName()).andReturn("foo");
    expect(obj.getStatus()).andReturn(PROCESSING);
    expect(obj.getStatusMessage()).andReturn("ok");
    byte[] value = {};
    expect(obj.getRawData()).andReturn(value);
    expect(stepMock.getName()).andReturn("bar");
    expect(obj.getOperation()).andReturn(Document.Operation.UPDATE);
    replay();
    DocumentImpl.DocumentEntry e = (DocumentImpl.DocumentEntry) obj.toEntry(stepMock);
    assertEquals(stupidFinalClass, e.contents);
    assertEquals("foo", e.scannerName);
    assertEquals(PROCESSING, e.status);
    assertEquals("ok", e.statusMessage);
    assertEquals(value, e.data.data);
    assertEquals("bar", e.nextStepName);
    assertEquals(Document.Operation.UPDATE.toString(), e.operation);

  }

  @Test
  public void testBasicMethods() {
    expect(scannerMock.getName()).andReturn("scannerFoo");
    expect(planMock.getDocIdField()).andReturn("id");
    replay();
    byte[] rawData = new byte[] {1,2};
    DocumentImpl impl = new DocumentImpl(rawData, "fooId", planMock, Document.Operation.NEW, scannerMock);

    assertEquals(Document.Operation.NEW, impl.getOperation());

    ArrayListMultimap<String,String> mm = ArrayListMultimap.create();
    mm.put("foo","bar");
    mm.put("foo","baz");
    mm.put("fizz","buzz");
    mm.put("nullthing", null);

    impl.putAll(mm);

    assertTrue(impl.containsKey("foo"));
    assertTrue(impl.containsValue("bar"));
    assertTrue(impl.containsEntry("foo", "bar"));
    assertTrue(impl.containsEntry("foo", "baz"));
    assertTrue(impl.containsEntry("id", "fooId"));
    assertTrue(impl.containsEntry(Document.DOC_RAW_SIZE, "2"));
    assertFalse(impl.containsEntry("foo", "pub"));
    assertEquals(6,impl.size());
    assertEquals(5, impl.keySet().size());
    assertEquals(6, impl.entries().size());
    assertEquals(6, impl.values().size());
    assertEquals(5, impl.asMap().size());

    assertTrue(impl.remove("foo","bar"));

    assertTrue(impl.containsKey("foo"));
    assertFalse(impl.containsValue("bar"));
    assertFalse(impl.containsEntry("foo", "bar"));
    assertTrue(impl.containsEntry("foo", "baz"));
    assertFalse(impl.containsEntry("foo", "pub"));
    assertEquals(5,impl.size());
    assertEquals(5, impl.keySet().size());
    assertEquals(5, impl.entries().size());
    assertEquals(5, impl.values().size());
    assertEquals(5, impl.asMap().size());

    assertArrayEquals(new byte[] {1,2}, impl.getRawData());

    assertEquals(PROCESSING, impl.getStatus());
    impl.setStatus(DROPPED, "just because...");
    assertEquals(DROPPED, impl.getStatus());
    assertEquals(impl.getStatusMessage(), "just because...");

    assertEquals("DocumentImpl{" +
        "id=fooId, " +
        "delegate={doc_raw_size=[2], foo=[baz], fizz=[buzz], nullthing=[null], id=[fooId]}, " +
        "status=DROPPED, " +
        "statusMessage='just because...', " +
        "operation=NEW, " +
        "sourceScannerName='scannerFoo', " +
        "idField='id'}", impl.toString());

    assertFalse(impl.isEmpty());
    impl.clear();
    assertTrue(impl.isEmpty());
  }
}
