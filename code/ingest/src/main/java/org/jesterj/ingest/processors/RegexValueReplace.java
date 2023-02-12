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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexValueReplace implements DocumentProcessor {

  private String name;
  private String field;
  private Pattern regex;
  private String replace;

  private boolean discardUnmatched = false;

  @Override
  public Document[] processDocument(Document document) {
    List<String> values = document.get(getField());
    List<String> newValues = new ArrayList<>();
    for (String value : values) {
      Matcher m = getRegex().matcher(value);
      if (isDiscardUnmatched()) {
        if (m.matches()) {
          newValues.add(m.replaceAll(getReplace()));
        }
      } else {
        newValues.add(m.replaceAll(getReplace()));
      }
    }
    document.replaceValues(getField(), newValues);
    return new Document[]{document};
  }

  @Override
  public String getName() {
    return name;
  }

  protected void setName(String name) {
    this.name = name;
  }

  public String getField() {
    return field;
  }

  protected void setField(String field) {
    this.field = field;
  }

  public Pattern getRegex() {
    return regex;
  }

  protected void setRegex(Pattern regex) {
    this.regex = regex;
  }

  public String getReplace() {
    return replace;
  }

  protected void setReplace(String replace) {
    this.replace = replace;
  }

  public boolean isDiscardUnmatched() {
    return discardUnmatched;
  }

  public void setDiscardUnmatched(boolean discardUnmatched) {
    this.discardUnmatched = discardUnmatched;
  }

  public static class Builder extends NamedBuilder<RegexValueReplace> {

    RegexValueReplace obj = new RegexValueReplace();

    @Override
    public RegexValueReplace.Builder named(String name) {
      getObj().setName(name);
      return this;
    }

    @Override
    protected RegexValueReplace getObj() {
      return obj;
    }

    public RegexValueReplace.Builder editingField(String fieldName) {
      getObj().setField(fieldName);
      return this;
    }

    public RegexValueReplace.Builder withRegex(String regex) {
      getObj().setRegex(Pattern.compile(regex));
      return this;
    }

    public RegexValueReplace.Builder andReplacement(String replace) {
      getObj().setReplace(replace);
      return this;
    }

    public RegexValueReplace.Builder discardingUnmatched() {
      getObj().discardUnmatched = true;
      return this;
    }

    private void setObj(RegexValueReplace obj) {
      this.obj = obj;
    }

    public RegexValueReplace build() {
      RegexValueReplace object = getObj();
      setObj(new RegexValueReplace());
      return object;
    }

  }

}
