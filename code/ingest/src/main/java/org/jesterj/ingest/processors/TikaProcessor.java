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
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.Status;
import org.jesterj.ingest.model.impl.NamedBuilder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.AccessControlException;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 3/19/16
 */
public class TikaProcessor implements DocumentProcessor {

  private static final Logger log = LogManager.getLogger();
  private String name;
  private String suffix;
  private int maxLength = -1; // process all text by default
  private TikaConfig tikaConfig = TikaConfig.getDefaultConfig();
  private boolean replaceRaw = true;
  private String destField = null;

  @Override
  public Document[]   processDocument(Document document) {
    try {
      byte[] rawData = document.getRawData();
      if (rawData == null) {
        log.debug("Skipping document without data in " + getName());
        return new Document[]{document};
      }
      Tika tika = new Tika(tikaConfig);
      tika.setMaxStringLength(document.getRawData().length);
      Metadata metadata = new Metadata();
      try (ByteArrayInputStream bais = new ByteArrayInputStream(rawData)) {
        String textContent = tika.parseToString(bais, metadata, maxLength);
        if (replaceRaw) {
          document.setRawData(textContent.getBytes(StandardCharsets.UTF_8));
        }
        if (destField != null) {
          document.put(destField,textContent);
        }
        for (String name : metadata.names()) {
          document.put(sanitize(name) + plusSuffix(), metadata.get(name));
        }
      } catch (IOException | TikaException e) {
        log.debug("Tika processing failure!", e);
        // if tika can't parse it we certainly don't want random binary crap in the index
        document.setStatus(Status.ERROR);
      }
    } catch (Throwable t) {
      boolean isAccessControl = t instanceof AccessControlException;
      boolean isSecurity = t instanceof SecurityException;
      if (!isAccessControl && !isSecurity) {
        throw t;
      }
    }
    return new Document[]{document};
  }

  private String plusSuffix() {
    return suffix == null ? "" : suffix;
  }

  private String sanitize(String dirty) {
    StringBuilder clean = new StringBuilder(dirty.length());
    for (char c : dirty.toCharArray()) {
      if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
        clean.append(c);
      } else {
        clean.append('_');
      }
    }
    return clean.toString();
  }


  @Override
  public String getName() {
    return name;
  }

  @SuppressWarnings("WeakerAccess")
  public static class Builder extends NamedBuilder<TikaProcessor> {

    TikaProcessor obj = new TikaProcessor();

    protected TikaProcessor getObj() {
      return obj;
    }

    public Builder named(String name) {
      getObj().name = name;
      return this;
    }

    /**
     * Add a suffix to all fields names output by tika.
     *
     * @param suffix the suffix to add
     * @return This builder for further configuration
     */
    public Builder appendingSuffix(String suffix) {
      getObj().suffix = suffix;
      return this;
    }

    /**
     * Convenience override for safety valve to guard against large documents. By
     * default this is set to -1 for no limit on the amount of data to process
     * with Tika.
     *
     * @param chars The limit
     * @return This builder for further configuration
     */
    public Builder truncatingTextTo(int chars) {
      getObj().maxLength = chars;
      return this;
    }

    /**
     * Speifiy if the results of tika's analysis should replace the raw document content or not.
     *
     * @param replaceRaw if true the original content for the document will be overwritten by tika's extracted output.
     * @return This builder for further configuration
     */
    public Builder replacingRawData(boolean replaceRaw) {
      getObj().replaceRaw = replaceRaw;
      return this;
    }

    /**
     * Send the results of tika's text extraction (text extracted but not metadata) into the supplied field.
     *
     * @param field the name of the field to containt the extracted text.
     * @return This builder for further configuration
     */
    public Builder intoField(String field) {
      getObj().destField = field;
      return this;
    }

    /**
     * Specify a tika configuration via an XML document you have loaded via filesystem/classpath or other method
     * of your choice.
     *
     * @param config The configuration
     * @return This builder for further config
     * @throws TikaException if Tika doesn't like your config
     * @throws IOException if Tika can't find something it needed?
     */
    public Builder configuredWith(org.w3c.dom.Document config) throws TikaException, IOException {
      getObj().tikaConfig = new TikaConfig(config);
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
