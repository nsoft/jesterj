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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ForwardingListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import org.apache.commons.codec.binary.Hex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.appender.AppenderLoggingException;
import org.jesterj.ingest.Main;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.model.Scanner;
import org.jesterj.ingest.model.Status;
import org.jesterj.ingest.processors.DocumentLoggingContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

  public static final String CHILD_SEP = "⇛";
  // document id field.
  private final String idField;

  Logger log = LogManager.getLogger();

  private final ArrayListMultimap<String, String> delegate = ArrayListMultimap.create();
  private byte[] rawData;
  private Status status = Status.PROCESSING;
  private String statusMessage = "";
  private final Operation operation;
  private final String sourceScannerName;
  private final String parentId;
  private final String originalParentId;
  private volatile boolean statusChanged = true;
  private String docHash;
  private boolean forceReprocess;


  public DocumentImpl(byte[] rawData, String id, Plan plan, Operation operation, Scanner source) {
    this(rawData,id,plan.getDocIdField(), operation,source.getName(),null,id);
  }

  DocumentImpl(byte[] rawData, String id, String idField, Operation operation, String source,String parentId, String originalParentId) {
    this.rawData = rawData;
    this.operation = operation;
    this.sourceScannerName = source;
    this.idField = idField;
    this.parentId = parentId;
    this.originalParentId = originalParentId;
    this.delegate.put(idField, id);
    if (this.rawData != null) {
      this.delegate.put(DOC_RAW_SIZE, String.valueOf(this.rawData.length));
    }
  }

  /**
   * Make a child document. DeterministChild ID generation is critical for handling future
   *
   * @param rawData Any raw data for the child document
   * @param operation The operation implied for this child
   * @param childId A unique and deterministically generated identifier for the child
   * @return A properly configured child document, with an ID composed of the parentID and
   * the child id separated by a delimiter of '⇛' ('U+21DB')
   */
  @SuppressWarnings("unused")
  Document makeChild(byte[] rawData,  Operation operation, int childId) {
    String id = getId() + CHILD_SEP + childId;
    return new DocumentImpl(rawData, id, this.idField, operation, this.sourceScannerName,
        this.getId(),this.originalParentId);
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
  public  void setStatus(Status status) {
    this.statusChanged = true;
    this.status = status;
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
  public ArrayListMultimap<String, String> getDelegate() {
    return delegate;
  }

  @Override
  public String getId() {
    return get(getIdField()).get(0);
  }

  @Override
  public String getHash() {
    if (docHash != null) {
      return docHash;
    }
    try {
      MessageDigest md = MessageDigest.getInstance(getHashAlg());
      md.update(getDelegateString().getBytes(StandardCharsets.UTF_8));
      if (getRawData() != null) {
        md.update(getRawData());
      }
      docHash = new String(Hex.encodeHex(md.digest(), false));
      return docHash;
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

  @Override
  public String getParentId() {
    return parentId;
  }

  @Override
  public String getOrignalParentId() {
    return originalParentId;
  }

  @Override
  public String toString() {
    return "DocumentImpl{" +
        "id=" + getId() +
        ", delegate=" + delegate +
        ", status=" + status +
        ", statusMessage='" + statusMessage + '\'' +
        ", operation=" + operation +
        ", sourceScannerName='" + sourceScannerName + '\'' +
        ", idField='" + idField + '\'' +
        '}';
  }


  @Override
  public boolean isStatusChanged() {
    return statusChanged;
  }

  public void reportDocStatus(Status status, String message, Object... messageParams) {
    setStatus(status, message);
    statusChanged = false;
    try(DocumentLoggingContext dc = new DocumentLoggingContext(this)) {
      dc.run(() -> log.info(status.getMarker(), message, messageParams));
    } catch (AppenderLoggingException e) {
      if (Main.isNotShuttingDown()) {
        log.error("Could not contact our internal Cassandra!!!", e);
      }
    }
  }

  @Override
  public void setForceReprocess(boolean b) {
    this.forceReprocess = b;
  }

  @Override
  public boolean isForceReprocess() {
    return forceReprocess;
  }
}
