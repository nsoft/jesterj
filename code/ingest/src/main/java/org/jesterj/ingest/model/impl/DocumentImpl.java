/*
 * Copyright 2013-2016 Needham Software LLC
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

package org.jesterj.ingest.model.impl;

import com.coremedia.iso.Hex;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ForwardingListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import net.jini.core.entry.Entry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.appender.AppenderLoggingException;
import org.jesterj.ingest.Main;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.model.Scanner;
import org.jesterj.ingest.model.Status;
import org.jesterj.ingest.model.Step;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 11/10/13
 */

/**
 * A container for the file data and associated metadata. MetaData for which the key and the value
 * are of type @link(java.lang.String} should be submitted as a field &amp; value to the index. Multiple
 * values for the same field are supported and addition order is maintained. The file data
 * will be discarded by default, and if it is to be indexed, it should be processed
 * and the text result added as a string value by a step in a plan that processes this item.
 *
 * @see ForwardingListMultimap
 */
public class DocumentImpl implements Document {

  // document id field.
  private final String idField;

  Logger log = LogManager.getLogger();

  private ArrayListMultimap<String, String> delegate = ArrayListMultimap.create();
  private byte[] rawData;
  private Status status = Status.PROCESSING;
  private String statusMessage = "";
  private Operation operation;
  private String sourceScannerName;

  public DocumentImpl(byte[] rawData, String id, Plan plan, Operation operation, Scanner source) {
    this.rawData = rawData;
    this.operation = operation;
    this.sourceScannerName = source.getName();
    this.idField = plan.getDocIdField();
    this.delegate.put(idField, id);

    if (this.rawData != null) {
      this.delegate.put(FIELD_FILE_SIZE, String.valueOf(this.rawData.length));
    }
  }

  /**
   * Copy constructor. Creates a deep copy of raw data, so may be memory intensive.
   *
   * @param doc The original document to be copied.
   */
  public DocumentImpl(Document doc) {
    this(doc, true);
  }

  /**
   * Create a copy of a document but do not copy the raw data or existing mappings. Useful in creating
   * child documents or documents calculated from other documents.
   *
   * @param doc  The document to copy
   * @param deep whether or not to copy the mappings and raw content or only the document info.
   */
  public DocumentImpl(Document doc, boolean deep) {
    if (deep) {
      byte[] duplicate = new byte[doc.getRawData().length];
      System.arraycopy(doc.getRawData(), 0, duplicate, 0, doc.getRawData().length);
      this.rawData = duplicate;
    } else {
      rawData = new byte[]{};
    }
    this.operation = doc.getOperation();
    if (deep) {
      this.delegate = ArrayListMultimap.create(doc.getDelegate());
    } else {
      this.delegate = ArrayListMultimap.create();
    }
    this.sourceScannerName = doc.getSourceScannerName();
    this.idField = doc.getIdField();
    this.status = doc.getStatus();
    this.statusMessage = doc.getStatusMessage();
  }

  @Override
  public Multiset<String> keys() {
    return delegate.keys();
  }

  @Override
  public boolean putAll(@Nullable java.lang.String key, Iterable<? extends String> values) {
    return delegate.putAll(key, values);
  }

  @Override
  public boolean put(@Nonnull java.lang.String key, @Nonnull java.lang.String value) {
    if (getIdField().equals(key)) {
      ArrayList<String> values = new ArrayList<>();
      values.add(value);
      List<String> prev = replaceValues(this.idField, values);
      return prev == null || prev.size() != 1 || !prev.get(0).equals(value);
    } else {
      return delegate.put(key, value);
    }
  }

  @Override
  public boolean putAll(Multimap<? extends String, ? extends String> multimap) {
    return delegate.putAll(multimap);
  }

  @Override
  public Set<String> keySet() {
    return delegate.keySet();
  }

  @Override
  public boolean containsEntry(@Nullable java.lang.Object key, @Nullable java.lang.Object value) {
    return delegate.containsEntry(key, value);
  }

  @Override
  public boolean remove(@Nullable java.lang.Object key, @Nullable java.lang.Object value) {
    return delegate.remove(key, value);
  }

  @Override
  public boolean containsValue(@Nullable java.lang.Object value) {
    return delegate.containsValue(value);
  }

  @Override
  public Collection<Map.Entry<String, String>> entries() {
    return delegate.entries();
  }

  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Override
  public void clear() {
    delegate.clear();
  }

  @Override
  public Map<String, Collection<String>> asMap() {
    return delegate.asMap();
  }

  @Override
  public List<String> replaceValues(@Nullable java.lang.String key, Iterable<? extends String> values) {
    return delegate.replaceValues(key, values);
  }

  @Override
  public Collection<String> values() {
    return delegate.values();
  }

  @Override
  public boolean containsKey(@Nullable java.lang.Object key) {
    return delegate.containsKey(key);
  }

  @Override
  public List<String> get(@Nullable java.lang.String key) {
    return delegate.get(key);
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public List<String> removeAll(@Nullable java.lang.Object key) {
    return delegate.removeAll(key);
  }

  @Override
  public byte[] getRawData() {
    return rawData;
  }

  @Override
  public void setRawData(byte[] rawData) {
    this.rawData = rawData;
  }

  @Override
  public Status getStatus() {
    return this.status;
  }

  @Override
  public void setStatus(Status status, String statusMessage) {
    this.statusMessage = statusMessage;
    setStatus(status);
  }

  @Override
  public void setStatus(Status status) {
    this.status = status;
    try {
      log.info(status.getMarker(), statusMessage);
    } catch (AppenderLoggingException e) {
      if (!Main.isShuttingDown()) {
        log.error("Could not contact our internal Cassandra!!!" + e);
      }
    }
  }

  @Override
  public String getStatusMessage() {
    return statusMessage;
  }

  @Override
  public void setStatusMessage(String message) {
    this.statusMessage = message;
  }

  @Override
  public Entry toEntry(Step next) {
    return new DocumentEntry(this, next);
  }

  @Override
  public ArrayListMultimap<String, String> getDelegate() {
    return delegate;
  }

  @Override
  public String getId() {
    return get(getIdField()).get(0);
  }

  @Override
  public String getHash() {
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      try {
        md.update(getDelegateString().getBytes("UTF-8"));
        if (getRawData() != null) {
          md.update(getRawData());
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
      return Hex.encodeHex(md.digest());
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  String getDelegateString() {
    return delegate.toString();
  }

  @Override
  public String getIdField() {
    return idField;
  }

  @Override
  public Operation getOperation() {

    return operation;
  }

  public String getSourceScannerName() {
    return sourceScannerName;
  }

  @Override
  public String getFirstValue(String fieldName) {
    List<String> values = get(fieldName);
    return values == null || values.size() == 0 ? null : values.get(0);
  }


  /**
   * A serializable form of an item that can be placed in a JavaSpace. The nextStepName is the property on which
   * steps query JavaSpaces to retrieve entries.
   */
  public static class DocumentEntry implements Entry {

    public ArrayListMultimap<String, String> contents;
    public String scannerName;
    public Status status;
    public String statusMessage;
    public RawData data;
    public String nextStepName;
    public String operation;

    DocumentEntry(Document document, Step destination) {
      this.scannerName = document.getSourceScannerName();
      this.contents = document.getDelegate();
      this.status = document.getStatus();
      this.statusMessage = document.getStatusMessage();
      this.data = new RawData();
      this.data.data = document.getRawData();
      this.nextStepName = destination.getName();
      this.operation = document.getOperation().toString();
    }
  }

  // may want to associate encoding or parsing related information in the future...
  public static class RawData {
    public byte[] data;
  }

  @Override
  public String toString() {
    return "DocumentImpl{" +
        "delegate=" + delegate +
        ", status=" + status +
        ", statusMessage='" + statusMessage + '\'' +
        ", operation=" + operation +
        ", sourceScannerName='" + sourceScannerName + '\'' +
        ", idField='" + idField + '\'' +
        '}';
  }
}
