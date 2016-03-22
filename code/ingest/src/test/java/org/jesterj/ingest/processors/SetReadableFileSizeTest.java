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

import static com.copyright.easiertest.EasierMocks.prepareMocks;
import static com.copyright.easiertest.EasierMocks.replay;
import static com.copyright.easiertest.EasierMocks.reset;
import static com.copyright.easiertest.EasierMocks.verify;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

import org.jesterj.ingest.model.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.copyright.easiertest.BeanTester;
import com.copyright.easiertest.Mock;
import com.copyright.easiertest.ObjectUnderTest;

/**
 * Tests the <code>SetReadableFileSize</code> processor.
 * 
 * @author dgoldenberg
 */
public class SetReadableFileSizeTest {

  private static final String FIELD_FILE_SIZE = "file_size";
  private static final String FIELD_DISPLAY_FILE_SIZE_UNITS = "display_file_size_units";
  private static final String FIELD_DISPLAY_FILE_SIZE_NUM = "display_file_size_num";
  private static final String FIELD_DISPLAY_FILE_SIZE = "display_file_size";

  @ObjectUnderTest
  private SetReadableFileSize obj;
  @Mock
  private Document mockDocument;

  public SetReadableFileSizeTest() {
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

  /**
   * Tests whether we get output properly set if all the output fields are specified (i.e. the
   * use-case of output fields present).
   */
  @Test
  public void testProcessDocumentAllOutputFields() {
    expect(obj.getInputField()).andReturn(FIELD_FILE_SIZE).anyTimes();
    expect(obj.getNumericAndUnitsField()).andReturn(FIELD_DISPLAY_FILE_SIZE).anyTimes();
    expect(obj.getNumericField()).andReturn(FIELD_DISPLAY_FILE_SIZE_NUM).anyTimes();
    expect(obj.getUnitsField()).andReturn(FIELD_DISPLAY_FILE_SIZE_UNITS).anyTimes();

    expect(mockDocument.getFirstValue(FIELD_FILE_SIZE)).andReturn("1024");
    expect(mockDocument.put(FIELD_DISPLAY_FILE_SIZE, "1 KB")).andReturn(true);
    expect(mockDocument.put(FIELD_DISPLAY_FILE_SIZE_NUM, "1")).andReturn(true);
    expect(mockDocument.put(FIELD_DISPLAY_FILE_SIZE_UNITS, "KB")).andReturn(true);

    replay();
    obj.processDocument(mockDocument);
  }

  /**
   * Tests whether we get output <b>not set</b> if no output fields are specified (i.e. the use-case
   * of any output fields not present).
   */
  @Test
  public void testProcessDocumentNoOutputFields() {
    expect(obj.getInputField()).andReturn(FIELD_FILE_SIZE).anyTimes();
    expect(obj.getNumericAndUnitsField()).andReturn(null).anyTimes();
    expect(obj.getNumericField()).andReturn(null).anyTimes();
    expect(obj.getUnitsField()).andReturn(null).anyTimes();

    expect(mockDocument.getFirstValue(FIELD_FILE_SIZE)).andReturn("1024");

    replay();
    obj.processDocument(mockDocument);
  }

  /**
   * Tests the use-case of when the input field value is a number of bytes, e.g. 10 bytes.
   */
  @Test
  public void testProcessDocumentBytes() {
    expect(obj.getInputField()).andReturn(FIELD_FILE_SIZE).anyTimes();
    expect(obj.getNumericAndUnitsField()).andReturn(FIELD_DISPLAY_FILE_SIZE).anyTimes();
    expect(obj.getNumericField()).andReturn(FIELD_DISPLAY_FILE_SIZE_NUM).anyTimes();
    expect(obj.getUnitsField()).andReturn(FIELD_DISPLAY_FILE_SIZE_UNITS).anyTimes();

    expect(mockDocument.getFirstValue(FIELD_FILE_SIZE)).andReturn("10");
    expect(mockDocument.put(FIELD_DISPLAY_FILE_SIZE, "10 bytes")).andReturn(true);
    expect(mockDocument.put(FIELD_DISPLAY_FILE_SIZE_NUM, "10")).andReturn(true);
    expect(mockDocument.put(FIELD_DISPLAY_FILE_SIZE_UNITS, "bytes")).andReturn(true);

    replay();
    obj.processDocument(mockDocument);
  }

  /**
   * Tests the use-case of when the input field value is in kilobytes, e.g. 20 KB.
   */
  @Test
  public void testProcessDocumentKiloBytes() {
    expect(obj.getInputField()).andReturn(FIELD_FILE_SIZE).anyTimes();
    expect(obj.getNumericAndUnitsField()).andReturn(FIELD_DISPLAY_FILE_SIZE).anyTimes();
    expect(obj.getNumericField()).andReturn(FIELD_DISPLAY_FILE_SIZE_NUM).anyTimes();
    expect(obj.getUnitsField()).andReturn(FIELD_DISPLAY_FILE_SIZE_UNITS).anyTimes();

    expect(mockDocument.getFirstValue(FIELD_FILE_SIZE)).andReturn("20678");
    expect(mockDocument.put(FIELD_DISPLAY_FILE_SIZE, "20 KB")).andReturn(true);
    expect(mockDocument.put(FIELD_DISPLAY_FILE_SIZE_NUM, "20")).andReturn(true);
    expect(mockDocument.put(FIELD_DISPLAY_FILE_SIZE_UNITS, "KB")).andReturn(true);

    replay();
    obj.processDocument(mockDocument);
  }

  /**
   * Tests the use-case of when the input field value is in megabytes, e.g. 1 MB.
   */
  @Test
  public void testProcessDocumentMegaBytes() {
    expect(obj.getInputField()).andReturn(FIELD_FILE_SIZE).anyTimes();
    expect(obj.getNumericAndUnitsField()).andReturn(FIELD_DISPLAY_FILE_SIZE).anyTimes();
    expect(obj.getNumericField()).andReturn(FIELD_DISPLAY_FILE_SIZE_NUM).anyTimes();
    expect(obj.getUnitsField()).andReturn(FIELD_DISPLAY_FILE_SIZE_UNITS).anyTimes();

    expect(mockDocument.getFirstValue(FIELD_FILE_SIZE)).andReturn("1925099");
    expect(mockDocument.put(FIELD_DISPLAY_FILE_SIZE, "1 MB")).andReturn(true);
    expect(mockDocument.put(FIELD_DISPLAY_FILE_SIZE_NUM, "1")).andReturn(true);
    expect(mockDocument.put(FIELD_DISPLAY_FILE_SIZE_UNITS, "MB")).andReturn(true);

    replay();
    obj.processDocument(mockDocument);
  }

  /**
   * Tests the use-case of when the input field value is in gigabytes, e.g. 9 GB.
   */
  @Test
  public void testProcessDocumentGigaBytes() {
    expect(obj.getInputField()).andReturn(FIELD_FILE_SIZE).anyTimes();
    expect(obj.getNumericAndUnitsField()).andReturn(FIELD_DISPLAY_FILE_SIZE).anyTimes();
    expect(obj.getNumericField()).andReturn(FIELD_DISPLAY_FILE_SIZE_NUM).anyTimes();
    expect(obj.getUnitsField()).andReturn(FIELD_DISPLAY_FILE_SIZE_UNITS).anyTimes();

    expect(mockDocument.getFirstValue(FIELD_FILE_SIZE)).andReturn("9856506880");
    expect(mockDocument.put(FIELD_DISPLAY_FILE_SIZE, "9 GB")).andReturn(true);
    expect(mockDocument.put(FIELD_DISPLAY_FILE_SIZE_NUM, "9")).andReturn(true);
    expect(mockDocument.put(FIELD_DISPLAY_FILE_SIZE_UNITS, "GB")).andReturn(true);

    replay();
    obj.processDocument(mockDocument);
  }
  
  /**
   * Tests the use-case of when the input field value is in terabytes, e.g. 2 TB.
   */
  @Test
  public void testProcessDocumentTeraBytes() {
    expect(obj.getInputField()).andReturn(FIELD_FILE_SIZE).anyTimes();
    expect(obj.getNumericAndUnitsField()).andReturn(FIELD_DISPLAY_FILE_SIZE).anyTimes();
    expect(obj.getNumericField()).andReturn(FIELD_DISPLAY_FILE_SIZE_NUM).anyTimes();
    expect(obj.getUnitsField()).andReturn(FIELD_DISPLAY_FILE_SIZE_UNITS).anyTimes();

    expect(mockDocument.getFirstValue(FIELD_FILE_SIZE)).andReturn("2199023255552");
    expect(mockDocument.put(FIELD_DISPLAY_FILE_SIZE, "2 TB")).andReturn(true);
    expect(mockDocument.put(FIELD_DISPLAY_FILE_SIZE_NUM, "2")).andReturn(true);
    expect(mockDocument.put(FIELD_DISPLAY_FILE_SIZE_UNITS, "TB")).andReturn(true);

    replay();
    obj.processDocument(mockDocument);
  }

  @Test
  public void testBuild() {
    replay();

    SetReadableFileSize.Builder builder = new SetReadableFileSize.Builder();
    builder.from(FIELD_FILE_SIZE)
      .intoNumericAndUnitsField(FIELD_DISPLAY_FILE_SIZE)
      .intoNumericField(FIELD_DISPLAY_FILE_SIZE_NUM)
      .intoUnitsField(FIELD_DISPLAY_FILE_SIZE_UNITS);
    SetReadableFileSize built = builder.build();

    assertEquals(FIELD_FILE_SIZE, built.getInputField());
    assertEquals(FIELD_DISPLAY_FILE_SIZE, built.getNumericAndUnitsField());
    assertEquals(FIELD_DISPLAY_FILE_SIZE_NUM, built.getNumericField());
    assertEquals(FIELD_DISPLAY_FILE_SIZE_UNITS, built.getUnitsField());
  }

  @Test
  public void testSimpleProperties() {
    replay();
    BeanTester tester = new BeanTester();
    tester.testBean(new SetReadableFileSize());
  }

}
