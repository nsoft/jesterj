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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.Status;
import org.jesterj.ingest.model.impl.NamedBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;

/**
 * Issue a GET request to fetch a document at a particular URL. Uses simple URL connection, for more
 * complicated scenarios such as authentication, a different processor will be required, probably more
 * complicated versions should be implemented with Apache HTTP Client.
 */
public class FetchUrl implements DocumentProcessor {
  private static final Logger log = LogManager.getLogger();

  private String linkField;
  private boolean failOnIOError;
  private String name;
  Cache<String, Long> visitedSiteCache = CacheBuilder.newBuilder().maximumSize(50000).build();
  private long throttleMs;
  private String errorField;
  private String httpStatusField;

  @Override
  public Document[] processDocument(Document document) {
    URL url = null;
    try {
      url = new URL(document.getFirstValue(linkField));
      String protocol = url.getProtocol();
      String server = url.getHost();
      Long lastAccess = visitedSiteCache.getIfPresent(server);
      long now = System.currentTimeMillis();
      if (lastAccess == null) {
        visitedSiteCache.put(server, now);
      } else {
        long elapsed = now - lastAccess;
        if (elapsed < throttleMs) {
          try {
            Thread.sleep(throttleMs - elapsed);
          } catch (InterruptedException e) {
            // ignore, not really important.
          }
        }
      }
      URLConnection conn = url.openConnection();
      conn.setConnectTimeout(5000);
      conn.setReadTimeout(5000);
      conn.connect();

      ByteArrayOutputStream baos = new ByteArrayOutputStream();

      if (protocol != null && ("http".equals(protocol) || "https".equals(protocol))) {
        HttpURLConnection httpConnection = (HttpURLConnection) conn;
        int responseCode = httpConnection.getResponseCode();
        if (httpStatusField != null) {
          document.put(httpStatusField, String.valueOf(responseCode));
        }
        if (responseCode >= 400) {
          String message = "HTTP server responded " + responseCode + " " + httpConnection.getResponseMessage();
          if (errorField != null) {
            document.put(errorField, message);
          }
          throw new IOException(message);
        }
      }
      IOUtils.copy(conn.getInputStream(), baos);
      document.setRawData(baos.toByteArray());
    } catch (IOException e) {
      if (failOnIOError) {
        if (errorField != null) {
          document.put(errorField, e.getMessage());
        }
        document.setStatus(Status.ERROR);
      } else {
        log.warn("Could not fetch " + url + " for " + document.getId(), e);
      }
    }
    return new Document[]{document};
  }

  @SimpleProperty
  public String getLinkField() {
    return linkField;
  }

  @Override
  public String getName() {
    return name;
  }

  public static class Builder extends NamedBuilder<FetchUrl> {

    FetchUrl obj = new FetchUrl();

    @Override
    public FetchUrl.Builder named(String name) {
      getObj().name = name;
      return this;
    }

    @Override
    protected FetchUrl getObj() {
      return obj;
    }

    public FetchUrl.Builder fromLinkIn(String from) {
      getObj().linkField = from;
      return this;
    }

    public FetchUrl.Builder reportErrorsIn(String errorField) {
      getObj().errorField = errorField;
      return this;
    }

    public FetchUrl.Builder reportHttpStatusIn(String statusField) {
      getObj().httpStatusField = statusField;
      return this;
    }

    public FetchUrl.Builder sameSiteAccessOncePer(long miliseconds) {
      getObj().throttleMs = miliseconds;
      return this;
    }

    public FetchUrl.Builder withSiteCachSize(long numSites) {
      getObj().visitedSiteCache = CacheBuilder.newBuilder().maximumSize(numSites).build();
      return this;
    }

    public FetchUrl.Builder failDocOnError(boolean fail) {
      getObj().failOnIOError = fail;
      return this;
    }

    private void setObj(FetchUrl obj) {
      this.obj = obj;
    }

    public FetchUrl build() {
      FetchUrl object = getObj();
      setObj(new FetchUrl());
      return object;
    }

  }
}
