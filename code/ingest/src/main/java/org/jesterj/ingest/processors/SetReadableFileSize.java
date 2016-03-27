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

import com.copyright.easiertest.SimpleProperty;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.impl.NamedBuilder;

/**
 * Sets readable file size field values, as follows:
 * <ul>
 * <li>reads the value of the specified input field, interprets it as a number, determines its
 * magnitude and expresses it as bytes, KB, MB, GB or TB;
 * <li>provides options to write a combined field ("200 KB"), a units field ("KB"), and/or a numeric
 * field ("200").
 * </ul>
 * If the size is over 1 GB, the size is returned as the number of whole GB, i.e. the size is
 * rounded down to the nearest GB boundary. Similarly for the 1 MB and 1 KB boundaries.
 *
 * @author dgoldenberg
 */
public class SetReadableFileSize implements DocumentProcessor {

  private String inputField;
  private String numericAndUnitsField;
  private String unitsField;
  private String numericField;
  private String name;

  @Override
  public Document[] processDocument(Document document) {

    String val = document.getFirstValue(getInputField());
    if (StringUtils.isNotBlank(val)) {
      long lFileSize = Long.parseLong(val);

      String readableFileSize = FileUtils.byteCountToDisplaySize(lFileSize);

      if (StringUtils.isNotBlank(getNumericAndUnitsField())) {
        document.put(getNumericAndUnitsField(), readableFileSize);
      }
      String[] parts = readableFileSize.split(" ");
      if (StringUtils.isNotBlank(getUnitsField())) {
        document.put(getUnitsField(), parts[1]);
      }
      if (StringUtils.isNotBlank(getNumericField())) {
        document.put(getNumericField(), parts[0]);
      }
    }

    return new Document[]{document};
  }

  @SimpleProperty
  public String getInputField() {
    return inputField;
  }

  @SimpleProperty
  public String getNumericAndUnitsField() {
    return numericAndUnitsField;
  }

  @SimpleProperty
  public String getUnitsField() {
    return unitsField;
  }

  @SimpleProperty
  public String getNumericField() {
    return numericField;
  }

  @Override
  public String getName() {
    return name;
  }

  /**
   * Represents a builder which sets configuration properties on the
   * <code>SetReadableFileSize</code> processor.
   *
   * @author dgoldenberg
   */
  public static class Builder extends NamedBuilder<SetReadableFileSize> {

    SetReadableFileSize obj = new SetReadableFileSize();

    public Builder from(String fromField) {
      getObj().inputField = fromField;
      return this;
    }

    public Builder intoNumericAndUnitsField(String toField) {
      getObj().numericAndUnitsField = toField;
      return this;
    }

    public Builder intoUnitsField(String toField) {
      getObj().unitsField = toField;
      return this;
    }

    public Builder intoNumericField(String toField) {
      getObj().numericField = toField;
      return this;
    }

    public Builder named(String name) {
      getObj().name = name;
      return this;
    }

    protected SetReadableFileSize getObj() {
      return obj;
    }

    private void setObj(SetReadableFileSize obj) {
      this.obj = obj;
    }

    public SetReadableFileSize build() {
      SetReadableFileSize object = getObj();
      setObj(new SetReadableFileSize());
      return object;
    }
  }
}
