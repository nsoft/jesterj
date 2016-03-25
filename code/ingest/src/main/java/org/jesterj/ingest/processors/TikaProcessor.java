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
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.jesterj.ingest.model.Buildable;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.Status;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.AccessControlException;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 3/19/16
 */
public class TikaProcessor implements DocumentProcessor {

  private static final Logger log = LogManager.getLogger();
  private String name;

  @Override
  public Document[] processDocument(Document document) {
    byte[] rawData = document.getRawData();
    try {
      Tika tika = new Tika();
      Metadata metadata = new Metadata();
      try (ByteArrayInputStream bais = new ByteArrayInputStream(rawData)) {
        String textContent = tika.parseToString(bais, metadata);
        document.setRawData(textContent.getBytes(Charset.forName("UTF-8")));
        for (String name : metadata.names()) {
          document.put(name, metadata.get(name));
        }
      } catch (IOException | TikaException e) {
        log.warn("Tika processing failure!", e);
        // if tika can't parse it we certainly don't want random binary crap in the index
        document.setStatus(Status.DROPPED);
      }
    } catch (Throwable t) {
      boolean isAccessControl = t instanceof AccessControlException;
      boolean isSecurity = t instanceof SecurityException;
      if (!isAccessControl && !isSecurity) {
        throw t;
      } else {
        System.out.println("gotcha!");
      }
    }
    return new Document[]{document};
  }

  @Override
  public String getName() {
    return name;
  }

  public static class Builder implements Buildable<TikaProcessor> {

    TikaProcessor obj = new TikaProcessor();

    protected TikaProcessor getObj() {
      return obj;
    }

    public Builder named(String name) {
      getObj().name = name;
      return this;
    }

    private void setObj(TikaProcessor obj) {
      this.obj = obj;
    }

    public TikaProcessor build() {
      TikaProcessor object = getObj();
      setObj(new TikaProcessor());
      return object;
    }

  }

}
