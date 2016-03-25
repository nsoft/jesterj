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

import java.util.concurrent.atomic.AtomicInteger;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 3/18/16
 */
public class DefaultWarningProcessor implements DocumentProcessor {
  private static final AtomicInteger count = new AtomicInteger(0);
  // leaving this non-final because logging is a critical aspect of the unit test for this
  // most other cases we ignore testing the logging.
  static Logger log = LogManager.getLogger();

  // todo this isn't going to work out... need to think harder here non-unique name possible with deserialization
  private String name = "default_processor" + count.getAndIncrement();
  

  @Override
  public Document[] processDocument(Document document) {
    log.warn("Default processor in use, this is usually not the intention");
    return new Document[]{document};
  }

  @Override
  public String getName() {
    return name;
  }

  public static class Builder {

    DefaultWarningProcessor obj = new DefaultWarningProcessor();

    protected DefaultWarningProcessor getObj() {
      return obj;
    }

    public Builder named(String name) {
      getObj().name = name;
      return this;
    }


    private void setObj(DefaultWarningProcessor obj) {
      this.obj = obj;
    }

    public DefaultWarningProcessor build() {
      DefaultWarningProcessor object = getObj();
      setObj(new DefaultWarningProcessor());
      return object;
    }

  }

}
