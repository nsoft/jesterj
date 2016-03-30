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

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 3/20/16
 */
public class SimpleDateTimeReformatter implements DocumentProcessor {

  private static final Logger log = LogManager.getLogger();

  private String inputField;
  private String outputField;
  private DateTimeFormatter inputFormat;
  private DateTimeFormatter outputFormat = DateTimeFormatter.ISO_INSTANT; // default to what lucene likes
  private String name;

  @Override
  public Document[] processDocument(Document document) {
    List<String> input = document.get(inputField);
    if (inputField.equals(outputField)) {
      document.removeAll(inputField);
    }
    boolean updated = false;
    for (String s : input) {
      Instant instant = null;
      if (inputFormat != null) {
        instant = inputFormat.parse(s, Instant::from);
        log.warn("could not parse date field {} with value {} as a {}", inputField, s, inputFormat.toString());
      }
      if (instant == null) {
        try {
          instant = Instant.ofEpochMilli(Long.valueOf(s));
        } catch (NumberFormatException nfe) {
          log.warn("could not parse date field {} with value {} as a Long", inputField, s);
        }
      }
      if (instant == null) {
        continue;
      }
      document.put(outputField, outputFormat.format(instant));
      updated = true;
    }
    // if we failed, restore the old value
    if (!updated && inputField.equals(outputField)) {
      document.putAll(inputField, input);
      log.warn("reformatting of {} failed. Old value preserved");
    }
    return new Document[]{document};
  }

  @Override
  public String getName() {
    return name;
  }

  public static class Builder extends NamedBuilder<SimpleDateTimeReformatter> {

    SimpleDateTimeReformatter obj = new SimpleDateTimeReformatter();

    public Builder from(String fromField) {
      getObj().inputField = fromField;
      return this;
    }

    public Builder into(String toField) {
      getObj().outputField = toField;
      return this;
    }

    public Builder readingFormat(String inputFormat) {
      getObj().inputFormat = DateTimeFormatter.ofPattern(inputFormat);
      return this;
    }

    public Builder writingFormat(String outputFormat) {
      getObj().outputFormat = DateTimeFormatter.ofPattern(outputFormat);
      return this;
    }

    public Builder named(String name) {
      getObj().name = name;
      return this;
    }

    protected SimpleDateTimeReformatter getObj() {
      return obj;
    }

    private void setObj(SimpleDateTimeReformatter obj) {
      this.obj = obj;
    }

    public SimpleDateTimeReformatter build() {
      SimpleDateTimeReformatter object = getObj();
      setObj(new SimpleDateTimeReformatter());
      return object;
    }

  }

}
