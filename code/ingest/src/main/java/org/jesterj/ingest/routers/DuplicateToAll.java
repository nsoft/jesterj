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
import org.jesterj.ingest.model.Step;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;

/**
 * A router that simply duplicates the document to all subsequent steps. Note that like all routers
 * it is constrained to only select steps for which the document is eligible.
 *
 * @see Step#getEligibleNextSteps(Document)
 */
public class DuplicateToAll extends RouterBase {

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
    return getStep().getNextSteps().size();
  }

  @Override
  public NextSteps route(Document doc) {
    return createNextSteps(doc);
  }

  @NotNull
  NextSteps createNextSteps(Document doc) {
    LinkedHashMap<String, Step> eligibleNextSteps = getStep().getEligibleNextSteps(doc);
    return new NextSteps(doc, eligibleNextSteps.values().toArray(new Step[0]));
  }

  @Override
  public String getName() {
    return name;
  }

  public static class Builder extends RouterBase.Builder<DuplicateToAll> {
    private DuplicateToAll obj = new DuplicateToAll();

    protected DuplicateToAll getObj() {
      return obj;
    }

    private void setObj(DuplicateToAll obj) {
      this.obj = obj;
    }

    public DuplicateToAll build() {
      if (getObj().name == null) {
        throw new IllegalStateException("Name of router must nto be null");
      }
      DuplicateToAll object = getObj();
      setObj(new DuplicateToAll());
      return object;
    }
  }

}
