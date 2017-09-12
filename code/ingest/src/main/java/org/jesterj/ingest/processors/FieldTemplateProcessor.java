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

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.impl.NamedBuilder;

import java.io.StringWriter;
import java.util.List;

/**
 * Interpret the value of a field as a velocity template using the document as context. If the field has
 * multiple values all values will be interpreted and replaced. It's also important to remember that
 * the fields being referenced can contain multiple values, so one usually wants to write <code>$foobar[0]</code>, not
 * $<code>foobar</code>. The latter will lead to replacement with <code>[foo]</code> if only one value or
 * <code>[foo,bar,baz]</code> if 3 values are presently held in the field.
 * <p>&nbsp;
 * <p>WARNING: this uses the velocity templating engine which is a powerful, but potentially dangerous technique!!
 * You want to ensure that the template is NOT derived from and does NOT CONTAIN any text that is provided by users
 * or other untrustworthy sources before it is interpreted by this processor. If you allow user data to be interpreted
 * as a template, you have given the user the ability to run ARBITRARY code on the ingestion infrastructure.
 * Recommended usages include specifying the template field as a statically defined field, or drawn from a known
 * controlled and curated database containing templates. Users are also strongly cautioned against chaining
 * multiple instances of this step, since it becomes exponentially more difficult to ensure user controlled
 * data is not added to the template and then subsequently interpreted. With great power comes great
 * responsibility. Don't run with scissors... you have been warned!
 */
public class FieldTemplateProcessor implements DocumentProcessor {

  private String name;
  private String templateField;
  private VelocityEngine engine = new VelocityEngine();

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Document[] processDocument(Document document) {
    VelocityContext velocityContext = new VelocityContext(document.asMap());
    List<String> values = document.removeAll(templateField);

    for (String value : values) {
      StringWriter writer = new StringWriter();
      engine.evaluate(velocityContext, writer, document.getId(), value);
      document.put(templateField, writer.toString());
    }
    return new Document[]{document};
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setTemplateField(String templateField) {
    this.templateField = templateField;
  }

  public static class Builder extends NamedBuilder<FieldTemplateProcessor> {

    FieldTemplateProcessor obj = new FieldTemplateProcessor();

    @Override
    public FieldTemplateProcessor.Builder named(String name) {
      getObj().setName(name);
      return this;
    }

    @Override
    protected FieldTemplateProcessor getObj() {
      return obj;
    }

    public FieldTemplateProcessor.Builder withTemplatesIn(String fieldName) {
      getObj().setTemplateField(fieldName);
      return this;
    }

    private void setObj(FieldTemplateProcessor obj) {
      this.obj = obj;
    }

    public FieldTemplateProcessor build() {
      FieldTemplateProcessor object = getObj();
      setObj(new FieldTemplateProcessor());
      return object;
    }

  }

}
