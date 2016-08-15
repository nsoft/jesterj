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

import org.apache.cassandra.utils.ConcurrentBiMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.jesterj.ingest.config.Required;
import org.jesterj.ingest.logging.JesterJAppender;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.Status;
import org.jesterj.ingest.utils.AnnotationUtil;

import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 3/30/16
 */
public class ElasticNodeSender extends ElasticSender {
  private static final Logger log = LogManager.getLogger();

  private String nodeName = "My beautiful node";
  private String clusterName = "my-cluster";

  private Settings settings;
  private Node node;
  private String home;


  @Override
  protected void perDocumentFailure(ConcurrentBiMap<Document, ActionRequest> oldBatch, Exception e) {
    // something's wrong with the network etc all documents must be errored out:
    for (Document doc : oldBatch.keySet()) {
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

    private ElasticNodeSender obj = new ElasticNodeSender();
    private AnnotationUtil util = new AnnotationUtil();

    @Override
    protected ElasticNodeSender getObj() {
      return obj;
    }

    @Override
    public boolean isValid() {
      return super.isValid()
          && getObj() != null
          && getObj().nodeName != null
          && getObj().home != null
          && getObj().indexName != null
          && getObj().clusterName != null;
    }

    @Override
    public ElasticNodeSender build() {
      if (!isValid()) {
        List<String> required = new ArrayList<>();
        Method[] methods = getClass().getMethods();
        for (Method meth : methods) {
          util.runIfMethodAnnotated(meth, () -> required.add(meth.getName()), true, Required.class);
        }
        throw new IllegalStateException("Required parameters have not all been set on the builder. Required Parameters are:" + required);
      }
      ElasticNodeSender obj = getObj();
      //noinspection ResultOfMethodCallIgnored
      new File(obj.home).mkdirs();
      obj.settings = Settings.settingsBuilder()
          .put("http.enabled", false)
          .put("node.name", obj.nodeName)
          .put("path.home", obj.home)
          .build();
      obj.node = new NodeBuilder()
          .data(false)
          .local(false)
          .client(true)
          .settings(obj.settings)
          .clusterName(obj.clusterName)
          .build().start();
      obj.setClient(obj.node.client());
      return obj;
    }

    @Override
    public Builder named(String name) {
      super.named(name);
      return this;
    }

    @Required
    public Builder usingCluster(String clusterName) {
      getObj().clusterName = clusterName;
      return this;
    }

    @Required
    public Builder nodeName(String nodeName) {
      getObj().nodeName = nodeName;
      return this;
    }

    @Required
    public Builder locatedInDir(String home) {
      getObj().home = home;
      return this;
    }

    @Required
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
