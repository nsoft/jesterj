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

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 8/11/16
 */
public class DropFieldProcessor implements DocumentProcessor {
  private String name;


  private String fieldToDrop;

  @Override
  public Document[] processDocument(Document document) {
    document.removeAll(fieldToDrop);
    return new Document[]{document};
  }

  @Override
  public String getName() {
    return this.name;
  }

  public String getFieldToDrop() {
    return fieldToDrop;
  }

  public static class Builder extends NamedBuilder<DropFieldProcessor> {

    DropFieldProcessor obj = new DropFieldProcessor();

    @Override
    public DropFieldProcessor.Builder named(String name) {
      getObj().name = name;
      return this;
    }

    @Override
    protected DropFieldProcessor getObj() {
      return obj;
    }

    public DropFieldProcessor.Builder dropping(String fieldToDrop) {
      getObj().fieldToDrop = fieldToDrop;
      return this;
    }


    private void setObj(DropFieldProcessor obj) {
      this.obj = obj;
    }

    public DropFieldProcessor build() {
      DropFieldProcessor object = getObj();
      setObj(new DropFieldProcessor());
      return object;
    }

  }
}
