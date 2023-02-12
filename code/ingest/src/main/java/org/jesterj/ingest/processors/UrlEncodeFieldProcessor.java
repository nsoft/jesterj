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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;

public class UrlEncodeFieldProcessor implements DocumentProcessor {

  private String name;
  private String fieldToEncode;
  private String charset;

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Document[] processDocument(Document document) {
    List<String> values = document.get(fieldToEncode);
    for (int i = 0; i < values.size(); i++) {
      String value = values.get(i);
      try {
        values.set(i, URLEncoder.encode(value, charset));
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }
    return new Document[]{document};
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setFieldToEncode(String fieldToEncode) {
    this.fieldToEncode = fieldToEncode;
  }

  public void setCharset(String encoding) {
    this.charset = encoding;
  }

  public static class Builder extends NamedBuilder<UrlEncodeFieldProcessor> {

    UrlEncodeFieldProcessor obj = new UrlEncodeFieldProcessor();

    @Override
    public UrlEncodeFieldProcessor.Builder named(String name) {
      getObj().setName(name);
      return this;
    }

    @Override
    protected UrlEncodeFieldProcessor getObj() {
      return obj;
    }

    public UrlEncodeFieldProcessor.Builder encodingField(String fieldName) {
      getObj().setFieldToEncode(fieldName);
      return this;
    }

    public UrlEncodeFieldProcessor.Builder withCharacterEncoding(String charset) {
      getObj().setCharset(charset);
      return this;
    }

    private void setObj(UrlEncodeFieldProcessor obj) {
      this.obj = obj;
    }

    public UrlEncodeFieldProcessor build() {
      UrlEncodeFieldProcessor object = getObj();
      setObj(new UrlEncodeFieldProcessor());
      return object;
    }
  }
}
