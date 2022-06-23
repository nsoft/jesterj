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

import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.NextSteps;
import org.jesterj.ingest.model.Router;
import org.jesterj.ingest.model.Step;
import org.jesterj.ingest.model.impl.NamedBuilder;

import java.util.LinkedHashMap;

/**
 * A router that simply duplicates the document to all subsequent steps.
 */
public class DuplicateToAll implements Router {
  private String name;

  @Override
  public NextSteps route(Document doc, LinkedHashMap<String, Step> nextSteps) {
    return new NextSteps(doc, nextSteps.values().toArray(new Step[0]));
  }

  @Override
  public String getName() {
    return name;
  }

  public static class Builder extends NamedBuilder<DuplicateToAll> {
    private DuplicateToAll obj = new DuplicateToAll();

    public Builder named(String name) {
      getObj().name = name;
      return this;
    }

    protected DuplicateToAll getObj() {
      return obj;
    }

    private void setObj(DuplicateToAll obj) {
      this.obj = obj;
    }

    public DuplicateToAll build() {
      DuplicateToAll object = getObj();
      setObj(new DuplicateToAll());
      return object;
    }
  }

}
