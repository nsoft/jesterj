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

import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.impl.NamedBuilder;

public class SetStaticValue implements DocumentProcessor {
  private String name;

  private String valueToInsert;
  private String fieldToInsert;

  private boolean editExisting = true;

  private boolean addValueToExisting = true;

  @Override
  public Document[] processDocument(Document document) {
    if (!isEditExisting() && !document.get(getFieldToInsert()).isEmpty()) {
        return new Document[]{document};
    }
    if (!isAddValueToExisting()) {
      document.removeAll(getFieldToInsert());
    }
    document.put(getFieldToInsert(), getValueToInsert());
    return new Document[]{document};
  }

  @Override
  public String getName() {
    return this.name;
  }

  public String getFieldToInsert() {
    return fieldToInsert;
  }

  public boolean isEditExisting() {
    return editExisting;
  }

  public boolean isAddValueToExisting() {
    return addValueToExisting;
  }

  public String getValueToInsert() {
    return valueToInsert;
  }

  public static class Builder extends NamedBuilder<SetStaticValue> {

    SetStaticValue obj = new SetStaticValue();

    @Override
    public SetStaticValue.Builder named(String name) {
      getObj().name = name;
      return this;
    }

    @Override
    protected SetStaticValue getObj() {
      return obj;
    }

    public SetStaticValue.Builder adding(String fieldName) {
      getObj().fieldToInsert = fieldName;
      return this;
    }

    public SetStaticValue.Builder withValue(String value) {
      getObj().valueToInsert = value;
      return this;
    }

    public SetStaticValue.Builder skipIfHasValue() {
      getObj().editExisting = false;
      return this;
    }

    public SetStaticValue.Builder replaceExistingValue() {
      getObj().addValueToExisting = false;
      return this;
    }


    private void setObj(SetStaticValue obj) {
      this.obj = obj;
    }

    public SetStaticValue build() {
      SetStaticValue object = getObj();
      setObj(new SetStaticValue());
      return object;
    }

  }
}
