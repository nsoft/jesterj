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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.jesterj.ingest.logging.JesterJAppender;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.Status;

import java.net.InetAddress;
import java.net.UnknownHostException;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 4/5/16
 */
public class ElasticTransportClientSender extends ElasticSender {
  private static final Logger log = LogManager.getLogger();


  @Override
  protected void perDocumentFailure(Exception e) {
    // something's wrong with the network etc all documents must be errored out:
    for (Document doc : getBatch().keySet()) {
      ThreadContext.put(JesterJAppender.JJ_INGEST_DOCID, doc.getId());
      log.info(Status.ERROR.getMarker(), "{} could not be sent to elastic because of {}", doc.getId(), e.getMessage());
      log.error("Error communicating with elastic!", e);
    }
  }


  @Override
  protected boolean exceptionIndicatesDocumentIssue(Exception e) {
    // TODO figure out what causes might be due to a single document vs not available etc
    return e instanceof ESBulkFail;
  }

  @Override
  public String getName() {
    return name;
  }

  public static class Builder extends ElasticSender.Builder {

    private ElasticTransportClientSender obj = new ElasticTransportClientSender();

    @Override
    protected ElasticTransportClientSender getObj() {
      return obj;
    }

    @Override
    public ElasticTransportClientSender build() {
      ElasticTransportClientSender obj = getObj();
      try {
        obj.setClient(TransportClient.builder().build()
            .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300)));
      } catch (UnknownHostException e) {
        log.error("Could not find elastic!", e);
        throw new RuntimeException(e);
      }
      return obj;
    }

    @Override
    public Builder named(String name) {
      super.named(name);
      return this;
    }

    public Builder forIndex(String indexName) {
      super.forIndex(indexName);
      return this;
    }

    public Builder forObjectType(String objectType) {
      super.forObjectType(objectType);
      return this;
    }
  }

}
