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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is the default processor if none is otherwise configured. One can configure it intentionally for unit tests
 * or to create a step that routes documents without changing them.
 */
public class NoOpProcessor implements DocumentProcessor {
  private static final AtomicInteger count = new AtomicInteger(0);
  // leaving this non-final because logging is a critical aspect of the unit test for this
  // most other cases we ignore testing the logging.
  static volatile Logger log = LogManager.getLogger();

  // todo this isn't going to work out... need to think harder here non-unique name possible with deserialization
  private String name = "default_processor" + count.getAndIncrement();

  private boolean warn = true;


  @Override
  public Document[] processDocument(Document document) {
    if (isWarn()) { // note: class initializers won't have run when easiermocks invokes this, so must use method call.
      log.warn("Default processor in use, this is usually not the intention");
    }
    return new Document[]{document};
  }
  public boolean isWarn() {
    return warn;
  }

  @Override
  public String getName() {
    return name;
  }

  public static class Builder extends NamedBuilder<NoOpProcessor> {

    NoOpProcessor obj = new NoOpProcessor();

    protected NoOpProcessor getObj() {
      return obj;
    }

    public Builder named(String name) {
      getObj().name = name;
      return this;
    }

    public Builder turnOffWarning() {
      getObj().warn = false;
      return this;
    }


    private void setObj(NoOpProcessor obj) {
      this.obj = obj;
    }

    public NoOpProcessor build() {
      NoOpProcessor object = getObj();
      setObj(new NoOpProcessor());
      return object;
    }

  }

}
