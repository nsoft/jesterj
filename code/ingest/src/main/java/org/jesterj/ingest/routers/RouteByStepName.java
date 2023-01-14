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
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.NextSteps;
import org.jesterj.ingest.model.Step;
import org.jesterj.ingest.model.impl.NamedBuilder;

/**
 * A router that sends documents to subsequent steps by comparing the value in a standard field
 * in the document to the name of subsequent steps. Only the first value in the standard field is consulted, and
 * only one step may be returned (because all steps must hae a unique value for name). Therefore this router
 * will never duplicate the document.
 */
public class RouteByStepName extends RouterBase {
  private static final Logger log = LogManager.getLogger();

  public static final String JESTERJ_NEXT_STEP_NAME = "__JESTERJ_NEXT_STEP_NAME__";
  private String name;

  @Override
  public boolean isDeterministic() {
    return false;
  }

  @Override
  public boolean isConstantNumberOfOutputDocs() {
    return false;
  }

  @Override
  public int getNumberOfOutputCopies() {
    return 0;
  }

  @Override
  public NextSteps route(Document doc) {
    Step dest = getStep().getNextSteps().get(doc.getFirstValue(JESTERJ_NEXT_STEP_NAME));
    if (dest == null) {
      log.warn("Document " + doc.getId() + " dropped! no value for " + JESTERJ_NEXT_STEP_NAME +
          " You probably want to either set a different router or provide a value.");
    }
    return new NextSteps(doc, dest);
  }

  @Override
  public String getName() {
    return name;
  }

  public static class Builder extends NamedBuilder<RouteByStepName> {
    private RouteByStepName obj = new RouteByStepName();

    public Builder named(String name) {
      getObj().name = name;
      return this;
    }

    protected RouteByStepName getObj() {
      return obj;
    }

    private void setObj(RouteByStepName obj) {
      this.obj = obj;
    }

    public RouteByStepName build() {
      RouteByStepName object = getObj();
      setObj(new RouteByStepName());
      return object;
    }
  }
}
