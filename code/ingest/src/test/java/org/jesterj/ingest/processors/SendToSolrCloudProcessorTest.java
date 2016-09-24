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
import org.apache.cassandra.utils.ConcurrentBiMap;
import org.apache.logging.log4j.Logger;
import org.apache.solr.common.SolrInputDocument;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.Status;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

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
public class SendToSolrCloudProcessorTest {
  @ObjectUnderTest
  SendToSolrCloudProcessor proc;
  @Mock private ConcurrentBiMap<Document, SolrInputDocument> batchMock;
  @Mock private Document docMock;
  @Mock private Logger logMock;

  public SendToSolrCloudProcessorTest() {
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
  public void testPerDocumentFailure() {
    Set<Document> documents = new HashSet<>();
    documents.add(docMock);
    expect(batchMock.keySet()).andReturn(documents);
    RuntimeException e = new RuntimeException("TEST EXCEPTION");
    proc.putIdInThreadContext(docMock);
    expect(proc.log()).andReturn(logMock).anyTimes();
    expect(docMock.getId()).andReturn("42");
    logMock.info(Status.ERROR.getMarker(), "{} could not be sent to solr because of {}", "42", "TEST EXCEPTION");
    logMock.error("Error communicating with solr!", e);
    replay();
    proc.perDocumentFailure(batchMock, e);
  }
}
