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

/**
 * A router that simply duplicates the document to all subsequent steps.
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
    return new NextSteps(doc, getStep().getNextSteps().values().toArray(new Step[0]));
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
      DuplicateToAll object = getObj();
      setObj(new DuplicateToAll());
      return object;
    }
  }

}
