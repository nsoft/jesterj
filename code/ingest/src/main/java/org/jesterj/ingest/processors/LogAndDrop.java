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

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 3/18/16
 */

/**
 * Simply write to the log file and then drop the document on the floor
 */
public class LogAndDrop implements DocumentProcessor {

  // leaving this non-final because logging is a critical aspect of the unit test for this
  // most other cases we ignore testing the logging.
  static Logger log = LogManager.getLogger();
  private Level level;

  protected LogAndDrop() {
  }

  @Override
  public Document[] processDocument(Document document) {
    log.log(getLevel(), document.toString());
    document.setStatus(Status.DROPPED);
    return new Document[]{document};
  }

  Level getLevel() {
    return level;
  }
  
  public static class Builder {

    LogAndDrop obj = new LogAndDrop();

    public Builder withLogLevel(Level level) {
      getObj().level = level;
      return this;
    }

    protected LogAndDrop getObj() {
      return obj;
    }

    private void setObj(LogAndDrop obj) {
      this.obj = obj;
    }

    public LogAndDrop build() {
      LogAndDrop object = getObj();
      setObj(new LogAndDrop());
      return object;
    }

  }
}
