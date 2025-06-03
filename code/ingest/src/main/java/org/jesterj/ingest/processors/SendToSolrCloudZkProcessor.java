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

import com.google.common.annotations.VisibleForTesting;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Processor that can be configured to send to Solr Cloud via a connection to Zookeeper.
 * This has become the less common modern paradigm, and this dependent version only
 * offers a minor performance advantage over the URL based versrion (by receiving updates
 * for cluster state changes directly instead of needing to refresh stale state) If the
 * cluster is stable there is little difference. Most installations find the security and
 * system administration burdens of making Zookeeper visible much larger than the benefits.
 */
public class SendToSolrCloudZkProcessor extends SendToSolrProcessor {

  private SolrClient solrClient;

  protected SendToSolrCloudZkProcessor() {
  }

  @Override
  @VisibleForTesting
  SolrClient getSolrClient() {
    return solrClient;
  }

  @Override
  @VisibleForTesting
  void setSolrClient(SolrClient solrClient) {
    this.solrClient = solrClient;
  }

  public static class Builder extends SendToSolrProcessor.Builder {

    SendToSolrCloudZkProcessor obj = new SendToSolrCloudZkProcessor();

    List<String> zkList = new ArrayList<>();
    String chroot;

    /**
     * Add a zookeeper host:port. If :port is omitted :2181 will be assumed
     *
     * @param zk a host name, and port specification
     * @return This builder for further configuration;
     */
    @SuppressWarnings("ConstantConditions")
    public SendToSolrCloudZkProcessor.Builder withZookeeper(String zk) {
      if (zk.indexOf(":") < -1) {
        zk += ":2181";
      }
      zkList.add(zk);
      return this;
    }

    @SuppressWarnings("unused")
    public SendToSolrCloudZkProcessor.Builder zkChroot(String chroot) {
      this.chroot = chroot;
      return this;
    }

    @Override
    public SendToSolrCloudZkProcessor.Builder placingTextContentIn(String field) {
      super.placingTextContentIn(field);
      return this;
    }

    @Override
    public SendToSolrCloudZkProcessor.Builder usingCollection(String collection) {
      super.usingCollection(collection);
      return this;
    }

    @Override
    public SendToSolrCloudZkProcessor.Builder withRequestParameters(Map<String, String> params) {
      super.withRequestParameters(params);
      return this;
    }

    @Override
    public SendToSolrCloudZkProcessor.Builder withDocFieldsIn(String fieldsField) {
      super.withDocFieldsIn(fieldsField);
      return this;
    }

    @Override
    public SendToSolrCloudZkProcessor.Builder transformIdsWith(Function<String, Object> transformer) {
      super.transformIdsWith(transformer);
      return this;
    }

    @Override
    public SendToSolrCloudZkProcessor.Builder named(String name) {
      super.named(name);
      return this;
    }

    @Override
    public SendToSolrCloudZkProcessor.Builder sendingBatchesOf(int batchSize) {
      super.sendingBatchesOf(batchSize);
      return this;
    }

    @Override
    public SendToSolrCloudZkProcessor.Builder sendingPartialBatchesAfterMs(int ms) {
      super.sendingPartialBatchesAfterMs(ms);
      return this;
    }

    protected SendToSolrCloudZkProcessor getObj() {
      return obj;
    }

    private  void setObj(SendToSolrCloudZkProcessor obj) {
      this.obj = obj;
    }

    public SendToSolrCloudZkProcessor build() {
      SendToSolrCloudZkProcessor tmp = getObj();
      setObj(new SendToSolrCloudZkProcessor());
      CloudSolrClient built = new CloudSolrClient.Builder(this.zkList, Optional.ofNullable(chroot)).build();
      tmp.setSolrClient(built);
      built.setDefaultCollection(tmp.collection);
      return tmp;
    }
  }
}
