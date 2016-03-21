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
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;

import java.util.List;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 3/21/16
 */
public class CopyField implements DocumentProcessor {
  private String from;
  private String into;
  private boolean retainOriginal = true;

  @Override
  public Document[] processDocument(Document document) {

    List<String> values = document.get(getFrom());
    document.putAll(getInto(), values);
    if (!isRetainOriginal()) {
      document.removeAll(getFrom());
    }
    return new Document[]{document};
  }

  @SimpleProperty
  public String getFrom() {
    return from;
  }

  @SimpleProperty
  public String getInto() {
    return into;
  }

  @SimpleProperty
  public boolean isRetainOriginal() {
    return retainOriginal;
  }

  public static class Builder {
    private CopyField obj = new CopyField();

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

    protected CopyField getObj() {
      return obj;
    }

    private void setObj(CopyField obj) {
      this.obj = obj;
    }

    public CopyField build() {
      CopyField object = getObj();
      setObj(new CopyField());
      return object;
    }
  }
}
