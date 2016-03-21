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

import java.util.List;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 3/21/16
 */
public class FieldCopyProcessor implements DocumentProcessor {
  String from;
  String into;
  boolean retainOriginal = true;

  @Override
  public Document[] processDocument(Document document) {

    List<String> values = document.get(from);
    document.putAll(into, values);
    if (!retainOriginal) {
      document.removeAll(from);
    }
    return new Document[]{document};
  }

  public static class Builder {
    private FieldCopyProcessor obj = new FieldCopyProcessor();

    public Builder from(String from) {
      getObj().from = from;
      return this;
    }

    public Builder into(String into) {
      getObj().into = into;
      return this;
    }

    public Builder retainingOriginal(boolean retain) {
      getObj().retainOriginal = retain;
      return this;
    }

    protected FieldCopyProcessor getObj() {
      return obj;
    }

    private void setObj(FieldCopyProcessor obj) {
      this.obj = obj;
    }

    public FieldCopyProcessor build() {
      FieldCopyProcessor object = getObj();
      setObj(new FieldCopyProcessor());
      return object;
    }
  }
}
