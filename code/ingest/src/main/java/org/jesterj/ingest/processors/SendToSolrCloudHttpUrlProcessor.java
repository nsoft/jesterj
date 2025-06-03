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

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Processor that can be configured to send to Solr Cloud via a URL (Zookeeper access not required).
 * This has become the more common modern paradigm, and the Zookeeper dependent version only
 * offers a minor performance advantage (by receiving updates for cluster state changes directly
 * instead of needing to refresh stale state) If the cluster is stable there is little difference.
 * Unless you can think of a good reason, prefer this class over {@link SendToSolrCloudZkProcessor}
 */
public class SendToSolrCloudHttpUrlProcessor extends SendToSolrProcessor {

  private SolrClient solrClient;

  protected SendToSolrCloudHttpUrlProcessor() {
  }

  @Override
  SolrClient getSolrClient() {
    return solrClient;
  }

  @Override
  void setSolrClient(SolrClient solrClient) {
    this.solrClient = solrClient;
  }

  public static class Builder extends SendToSolrProcessor.Builder {

    SendToSolrCloudHttpUrlProcessor obj = new SendToSolrCloudHttpUrlProcessor();

    List<String> urls = new ArrayList<>();

    @SuppressWarnings("unused")
    public SendToSolrCloudHttpUrlProcessor.Builder solrUrl(String url) {
      try {
        new URL(url); // complain if this isn't actually a url
      } catch (MalformedURLException e) {
        throw new RuntimeException(e);
      }
      this.urls.add(url) ;
      return this;
    }

    @Override
    public SendToSolrCloudHttpUrlProcessor.Builder placingTextContentIn(String field) {
      super.placingTextContentIn(field);
      return this;
    }

    @Override
    public SendToSolrCloudHttpUrlProcessor.Builder usingCollection(String collection) {
      super.usingCollection(collection);
      return this;
    }

    @Override
    public SendToSolrCloudHttpUrlProcessor.Builder withRequestParameters(Map<String, String> params) {
      super.withRequestParameters(params);
      return this;
    }

    @Override
    public SendToSolrCloudHttpUrlProcessor.Builder withDocFieldsIn(String fieldsField) {
      super.withDocFieldsIn(fieldsField);
      return this;
    }

    @Override
    public SendToSolrCloudHttpUrlProcessor.Builder transformIdsWith(Function<String, Object> transformer) {
      super.transformIdsWith(transformer);
      return this;
    }

    @Override
    public SendToSolrCloudHttpUrlProcessor.Builder named(String name) {
      super.named(name);
      return this;
    }

    protected SendToSolrCloudHttpUrlProcessor getObj() {
      return obj;
    }

    protected void setObj(SendToSolrCloudHttpUrlProcessor obj) {
      this.obj = obj;
    }

    public SendToSolrCloudHttpUrlProcessor build() {
      SendToSolrCloudHttpUrlProcessor tmp = getObj();
      setObj(new SendToSolrCloudHttpUrlProcessor());
      CloudHttp2SolrClient built = new CloudSolrClient.Builder(urls).build();
      tmp.setSolrClient(built);
      built.setDefaultCollection(tmp.collection);
      return tmp;
    }
  }
}
