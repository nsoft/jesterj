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

package org.jesterj.ingest.routers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.*;

import java.util.*;

/**
 * A router that sends documents to subsequent steps by comparing the value in a standard field
 * in the document to the name of subsequent steps. Only the first value in the standard field is consulted, and
 * only one step may be returned (because all steps must hae a unique value for name). Therefore this router
 * will never duplicate the document. Also note that if this router does not find a next destination it will
 * cause an error for the document.
 */
public class RouteByStepName extends RouterBase {
  private static final Logger log = LogManager.getLogger();

  public static final String JESTERJ_NEXT_STEP_NAME = "__JESTERJ_NEXT_STEP_NAME__";
  private String name;

  private final Map<String, String> valueToStepNameMap = new HashMap<>();

  private String keyFieldName = JESTERJ_NEXT_STEP_NAME;

  @Override
  public boolean isDeterministic() {
    return true;
  }

  @Override
  public boolean isConstantNumberOfOutputDocs() {
    return true;
  }

  @Override
  public int getNumberOfOutputCopies() {
    return 1;
  }

  @Override
  public NextSteps route(Document doc) {
    String firstValue = doc.getFirstValue(getKeyFieldName());
    String possibleReplacement = getValueToStepNameMap().get(firstValue);
    if (possibleReplacement != null) {
      firstValue = possibleReplacement;
    }
    Step dest = getStep().getEligibleNextSteps(doc).get(firstValue);
    if (dest == null) {
      log.warn("Document {} dropped! no value for {} You probably want to either set a different " +
          "router or provide a value for that field in the document.", doc::getId, this::getKeyFieldName);
      updateExcludedDestinations(doc);
    } else {
      updateExcludedDestinations(doc, dest);
    }

    NextSteps nextSteps = dest == null ? null : new NextSteps(doc, dest);
    log.trace("Document {} Routed to {}, with statuses:{}", doc::getId, () -> nextSteps, doc::dumpStatus);
    return nextSteps;
  }

  @Override
  public String getName() {
    return name;
  }

  public String getKeyFieldName() {
    return keyFieldName;
  }

  protected void setKeyFieldName(String keyFieldName) {
    this.keyFieldName = keyFieldName;
  }

  public Map<String, String> getValueToStepNameMap() {
    return valueToStepNameMap;
  }

  public static class Builder extends RouterBase.Builder<RouteByStepName> {
    private RouteByStepName obj = new RouteByStepName();

    public Builder named(String name) {
      getObj().name = name;
      return this;
    }

    public Builder keyValuesInField(String fieldName) {
      getObj().setKeyFieldName(fieldName);
      return this;
    }

    public Builder mappingValueFromTo(String from, String to) {
      getObj().getValueToStepNameMap().put(from, to);
      return this;
    }

    protected RouteByStepName getObj() {
      return obj;
    }

    private void setObj(RouteByStepName obj) {
      this.obj = obj;
    }

    public RouteByStepName build() {
      if (getObj().name == null) {
        throw new IllegalStateException("Name of router must nto be null");
      }
      RouteByStepName object = getObj();
      setObj(new RouteByStepName());
      return object;
    }
  }
}
