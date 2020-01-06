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

import java.util.stream.Collectors;

public class TrimValues implements DocumentProcessor {
  private String name;

  // Future enhancement: match multiple fields to avoid configuring a ton of these
  private String fieldToTrim;

  @Override
  public Document[] processDocument(Document document) {
    document.putAll(fieldToTrim, document.removeAll(fieldToTrim).stream().map(String::trim).collect(Collectors.toList()));
    return new Document[]{document};
  }

  @Override
  public String getName() {
    return this.name;
  }

  public String getFieldToTrim() {
    return fieldToTrim;
  }

  public static class Builder extends NamedBuilder<TrimValues> {

    TrimValues obj = new TrimValues();

    @Override
    public TrimValues.Builder named(String name) {
      getObj().name = name;
      return this;
    }

    @Override
    protected TrimValues getObj() {
      return obj;
    }

    public TrimValues.Builder trimming(String fieldName) {
      getObj().fieldToTrim = fieldName;
      return this;
    }



    private void setObj(TrimValues obj) {
      this.obj = obj;
    }

    public TrimValues build() {
      TrimValues object = getObj();
      setObj(new TrimValues());
      return object;
    }

  }
}
