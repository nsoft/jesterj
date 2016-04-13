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
import java.util.HashMap;
import java.util.Map;

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
    private Map<String, String> hosts = new HashMap<>();

    @Override
    protected ElasticTransportClientSender getObj() {
      return obj;
    }

    @Override
    public ElasticTransportClientSender build() {
      ElasticTransportClientSender obj = getObj();
      try {
        TransportClient transportClient = TransportClient.builder().build();
        for (Map.Entry<String, String> host : hosts.entrySet()) {
          int port = Integer.valueOf(host.getValue());
          transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host.getKey()), port));
        }
        obj.setClient(transportClient);
      } catch (UnknownHostException e) {
        log.error("Could not find elastic!", e);
        throw new RuntimeException(e);
      }
      return obj;
    }

    @Override
    public boolean isValid() {
      boolean nullHost = false;
      boolean nonIntegerPort = false;
      for (Map.Entry<String, String> host : hosts.entrySet()) {
        try {
          log.trace("{}:{}", host.getKey(), Integer.valueOf(host.getValue()));
        } catch (NumberFormatException nfe) {
          nonIntegerPort = true;
          log.error("Non-numeric port {} for host:{} in processor named {}",
              host.getValue(), host.getKey(), obj.getName());
        }
        if (host.getKey() == null) {
          nullHost = true;
          log.error("Null host in list of hosts for transportClient in processor named {}", obj.getName());
        }
      }
      if (hosts.size() == 0) {
        log.error("No hosts supplied for processor named {}", obj.getName());
      }
      return super.isValid() && !nonIntegerPort && !nullHost;
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

    public Builder withServer(String host, Object port) {
      this.hosts.put(host, String.valueOf(port));
      return this;
    }
  }

}
