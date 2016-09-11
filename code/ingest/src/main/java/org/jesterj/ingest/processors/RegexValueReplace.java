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

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 9/11/16
 */
public class RegexValueReplace implements DocumentProcessor {

  private String name;
  private String field;
  private Pattern regex;
  private String replace;


  @Override
  public String getName() {
    return name;
  }

  @Override
  public Document[] processDocument(Document document) {
    List<String> values = document.get(getField());
    List<String> newValues = new ArrayList<>();
    for (String value : values) {
      Matcher m = getRegex().matcher(value);
      newValues.add(m.replaceAll(getReplace()));
    }
    document.replaceValues(getField(), newValues);
    return new Document[]{document};
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getField() {
    return field;
  }

  public void setField(String field) {
    this.field = field;
  }

  public Pattern getRegex() {
    return regex;
  }

  public void setRegex(Pattern regex) {
    this.regex = regex;
  }

  public String getReplace() {
    return replace;
  }

  public void setReplace(String replace) {
    this.replace = replace;
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
