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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.Status;
import org.jesterj.ingest.model.impl.NamedBuilder;

import java.util.concurrent.atomic.AtomicInteger;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 3/18/16
 */

/**
 * A processor for testing failure scenarios.
 */
public class LogAndFail implements DocumentProcessor {

  // leaving this non-final because logging is a critical aspect of the unit test for this
  // most other cases we ignore testing the logging.
  static Logger log = LogManager.getLogger();
  private Level level;
  private String name;
  private RuntimeException exception;
  private int afterNumCalls;
  private AtomicInteger calls = new AtomicInteger(0);

  protected LogAndFail() {
  }

  @Override
  public Document[] processDocument(Document document) {
    if (getCalls().getAndIncrement() >= getAfterNumCalls()) {
      if (getException() != null) {
        log.log(getLevel(), getException());
        throw getException();
      } else {
        document.setStatus(Status.ERROR);
      }
    }
    log.log(getLevel(), document.toString());
    return new Document[]{document};
  }

  Level getLevel() {
    return level;
  }

  @Override
  public String getName() {
    return name;
  }

  public int getAfterNumCalls() {
    return afterNumCalls;
  }

  public RuntimeException getException() {
    return exception;
  }

  public AtomicInteger getCalls() {
    return calls;
  }

  public static class Builder extends NamedBuilder<LogAndFail> {

    LogAndFail obj = new LogAndFail();

    public Builder withLogLevel(Level level) {
      getObj().level = level;
      return this;
    }

    public Builder throwing(RuntimeException t) {
      getObj().exception = t;
      return this;
    }

    public Builder after(int calls) {
      getObj().afterNumCalls = calls;
      return this;
    }

    protected LogAndFail getObj() {
      return obj;
    }

    public Builder named(String name) {
      getObj().name = name;
      return this;
    }

    private void setObj(LogAndFail obj) {
      this.obj = obj;
    }

    public LogAndFail build() {
      LogAndFail object = getObj();
      setObj(new LogAndFail());
      return object;
    }

  }
}
