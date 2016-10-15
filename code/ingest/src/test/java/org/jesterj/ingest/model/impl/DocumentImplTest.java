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
import org.apache.commons.codec.digest.DigestUtils;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.model.Scanner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;

import static com.copyright.easiertest.EasierMocks.prepareMocks;
import static com.copyright.easiertest.EasierMocks.replay;
import static com.copyright.easiertest.EasierMocks.reset;
import static com.copyright.easiertest.EasierMocks.verify;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;


public class DocumentImplTest {

  @ObjectUnderTest private DocumentImpl obj;
  @Mock private Scanner scannerMock;
  @Mock private Plan planMock;


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
    System.out.println(document.getHash());
    System.out.println(document2.getHash());

    assertEquals("stringvalue", document.getFirstValue("string"));
    assertEquals(null, document.getFirstValue("unknown"));

  }

  @Test
  public void testHash() throws UnsupportedEncodingException, NoSuchAlgorithmException {
    expect(obj.getDelegateString()).andReturn("CAFE");
    expect(obj.getRawData()).andReturn("BABE".getBytes("UTF-8")).anyTimes();
    replay();
    assertEquals(DigestUtils.md5Hex("CAFEBABE".getBytes("UTF-8")).toUpperCase(), obj.getHash());
  }

  @Test
  public void testHashRawDataNull() throws UnsupportedEncodingException, NoSuchAlgorithmException {
    expect(obj.getDelegateString()).andReturn("CAFE");
    expect(obj.getRawData()).andReturn(null).anyTimes();
    replay();
    assertEquals(DigestUtils.md5Hex("CAFE".getBytes("UTF-8")).toUpperCase(), obj.getHash());
  }
}
