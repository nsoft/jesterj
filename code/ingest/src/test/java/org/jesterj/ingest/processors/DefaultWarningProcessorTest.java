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

import com.copyright.easiertest.Mock;
import com.copyright.easiertest.ObjectUnderTest;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.copyright.easiertest.EasierMocks.prepareMocks;
import static com.copyright.easiertest.EasierMocks.replay;
import static com.copyright.easiertest.EasierMocks.reset;
import static com.copyright.easiertest.EasierMocks.verify;
import static org.easymock.EasyMock.isA;

public class DefaultWarningProcessorTest {
  @ObjectUnderTest private DefaultWarningProcessor obj;
  @Mock private Logger mock;
  @Mock private Document mockDocument;
  private Logger original;

  public DefaultWarningProcessorTest() {
    prepareMocks(this);
  }

  @Before
  public void setUp() {
    original = DefaultWarningProcessor.log;
    DefaultWarningProcessor.log = mock;
    reset();
  }

  @After
  public void tearDown() {
    verify();
    DefaultWarningProcessor.log = original;
  }

  @Test
  public void testObj() {
    mock.warn(isA(String.class));
    replay();
    obj.processDocument(mockDocument);
  }

}
