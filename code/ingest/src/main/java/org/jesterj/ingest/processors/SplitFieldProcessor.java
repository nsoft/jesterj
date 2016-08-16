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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.impl.NamedBuilder;

import java.util.ArrayList;
import java.util.List;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 8/11/16
 */
public class SplitFieldProcessor implements DocumentProcessor {
  private String name;
  private String delimiter;
  private String field;
  private boolean trim;
  private static final Logger log = LogManager.getLogger();

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public Document[] processDocument(Document document) {
    List<String> oldValues = document.get(field);
    log.trace("old values: {} ({})", oldValues, oldValues.size());
    List<String> newValues = new ArrayList<>(oldValues.size() * 10);
    for (String v : oldValues) {
      String[] parts = v.split(delimiter);
      log.trace("split on delimiter '{}' gave {} parts", delimiter, parts.length);
      for (String part : parts) {
        if (trim) {
          newValues.add(part.trim());
        } else {
          newValues.add(part);
        }
      }
    }
    document.replaceValues(field, newValues);
    log.trace("new values: {} ({})", newValues, newValues.size());
    return new Document[]{document};
  }

  public static class Builder extends NamedBuilder<SplitFieldProcessor> {

    SplitFieldProcessor obj = new SplitFieldProcessor();

    @Override
    public SplitFieldProcessor.Builder named(String name) {
      getObj().name = name;
      return this;
    }

    public SplitFieldProcessor.Builder trimingWhiteSpace(boolean trim) {
      getObj().trim = trim;
      return this;
    }

    protected SplitFieldProcessor getObj() {
      return obj;
    }

    public SplitFieldProcessor.Builder splittingField(String field) {
      getObj().field = field;
      return this;
    }

    public SplitFieldProcessor.Builder onDelimiter(String delimiter) {
      getObj().delimiter = delimiter;
      return this;
    }

    private void setObj(SplitFieldProcessor obj) {
      this.obj = obj;
    }

    public SplitFieldProcessor build() {
      SplitFieldProcessor object = getObj();
      setObj(new SplitFieldProcessor());
      return object;
    }

  }
}
