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

import com.google.common.collect.*;
import org.apache.commons.codec.binary.Hex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.appender.AppenderLoggingException;
import org.jesterj.ingest.Main;
import org.jesterj.ingest.logging.Markers;
import org.jesterj.ingest.model.Scanner;
import org.jesterj.ingest.model.*;
import org.jesterj.ingest.processors.DocumentLoggingContext;
import org.jesterj.ingest.utils.Cloner;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


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
  public static final Pattern DEFAULT_TO_STRING = Pattern.compile("([A-Za-z_.0-9]+=\\[[^=]*[0-9_a-z.]+\\.[0-9_A-Za-z.]+@[0-9A-F]+)]}?,");
  // document id field.
  private final String idField;

  private static final Logger log = LogManager.getLogger();

  // sync so we have no issues with happens before among steps... possibly over conservative, but was fighting
  // concurrency bugs, optimize it out later.
  private final ListMultimap<String, String> delegate = Multimaps.synchronizedListMultimap(ArrayListMultimap.create());
  private byte[] rawData;

  private final Operation operation;
  private final String sourceScannerName;
  private final String parentId;
  private final String originalParentId;
  private String docHash;
  private boolean forceReprocess;
  private final Map<String, DocDestinationStatus> statusChanges = new ConcurrentHashMap<>();
  private final Map<String, DocDestinationStatus> incompletePotentSteps = new ConcurrentHashMap<>();
  private final String origination;


  public DocumentImpl(byte[] rawData, String id, Plan plan, Operation operation, Scanner source, String origination) {
    this(rawData, id, plan.getDocIdField(), operation, source.getName(), null, id, origination);
  }

  DocumentImpl(byte[] rawData, String id, String idField, Operation operation, String source, String parentId, String originalParentId, String origination) {
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
    this.origination =origination;
  }

  public DocumentImpl(byte[] rawData, String id, Operation oper, DocumentImpl parent) {
    this.rawData =rawData;
    if (this.rawData != null) {
      this.delegate.put(DOC_RAW_SIZE, String.valueOf(this.rawData.length));
    }
    this.operation = oper;

    this.idField = parent.idField;
    this.delegate.put(idField,id);
    this.sourceScannerName = parent.sourceScannerName;
    this.parentId = parent.getId();
    this.originalParentId = parent.originalParentId;
    this.origination = parent.origination;
    Cloner<DocDestinationStatus> cloner = new Cloner<>();
    for (Map.Entry<String, DocDestinationStatus> step : parent.incompletePotentSteps.entrySet()) {
      try {
        this.incompletePotentSteps.put(step.getKey(), cloner.cloneObj(step.getValue()));
      } catch (IOException | ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }

  }

  /**
   * Make a child document. DeterministChild ID generation is critical for handling future
   *
   * @param rawData   Any raw data for the child document
   * @param operation The operation implied for this child
   * @param childId   A unique and deterministically generated identifier for the child
   * @return A properly configured child document, with an ID composed of the parentID and
   * the child id separated by a delimiter of '⇛' ('U+21DB')
   */
  @SuppressWarnings("unused")
  Document makeChild(byte[] rawData, Operation operation, int childId) {
    return new DocumentImpl(rawData, getId() + CHILD_SEP + childId, operation,this);
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
  public Status getStatus(String potentStepName) {
    DocDestinationStatus result = incompletePotentSteps.get(potentStepName);
    return result == null ? null : result.getStatus();
  }

  @Override
  public String getStatusMessage(String potentStep) {
    return incompletePotentSteps.get(potentStep).getMessage();
  }

  @Override
  public void setStatus(Status status, String potentStep, String statusMessage, Serializable... messageArgs) {
    if (!incompletePotentSteps.containsKey(potentStep)) {
      throw new UnsupportedOperationException("Do not add new downstream steps via setStatus");
    }
    @SuppressWarnings("RedundantCast")
    DocDestinationStatus docStat = new DocDestinationStatus(status, potentStep, statusMessage, (Serializable[]) messageArgs);
    incompletePotentSteps.put(potentStep, docStat);
    statusChanges.put(potentStep, docStat);
  }

  @Override
  public ListMultimap<String, String> getDelegate() {
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
      String delegateString = getDelegateString();
      // warn the user if they are making a simple error with potentially subtle consequences.
      Matcher m = DEFAULT_TO_STRING.matcher(delegateString);
      if (m.matches()) {
        log.warn("Detected possible default Object.toString() when calculating hash code for {}! " +
            "If allowed, this will lead to non-reproducable hash codes due to the inclusion of java memory " +
            "addresses that are non-deterministic. The normal fix is to implement toString() for the object, or" +
            "serialize the object in a deterministic fashion before adding it to the document when scanning." +
            "Offending match={}", getId(), m.group(1));
      }
      md.update(delegateString.getBytes(StandardCharsets.UTF_8));
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
        ", status=" + incompletePotentSteps +
        ", operation=" + operation +
        ", sourceScannerName='" + sourceScannerName + '\'' +
        ", idField='" + idField + '\'' +
        ", origin=" + origination +
        '}';
  }


  @Override
  public boolean isStatusChanged() {
    return statusChanges.size() != 0;
  }

  @Override
  public void reportDocStatus() {
    if (!isStatusChanged()) {
      return;
    }
    String message = statusChanges.values().stream().map(DocDestinationStatus::getMessage).collect(Collectors.joining("#,#"));
    Object[] params = statusChanges.values().stream().flatMap(d -> Arrays.stream(d.getMessageParams())).toArray();
    try (DocumentLoggingContext dc = new DocumentLoggingContext(this)) {
      dc.run(() -> log.info(Markers.FTI_MARKER, message, params));
    } catch (AppenderLoggingException e) {
      if (Main.isNotShuttingDown()) {
        log.error("Could not contact our internal Cassandra!!!", e);
      } else {
        log.info("Shutdown prevented update {} ==> {}", getId(), getStatusChanges());
      }
    }
    for (DocDestinationStatus value : statusChanges.values()) {
      if (value.getStatus() != Status.PROCESSING) {
        incompletePotentSteps.remove(value.getPotentStep());
      }
    }
    statusChanges.clear();
  }


  @Override
  public void setForceReprocess(boolean b) {
    this.forceReprocess = b;
  }

  @Override
  public boolean isForceReprocess() {
    return forceReprocess;
  }

  //
  // NOTE: many methods here, the point of which is that the document should not expose the incomplete steps
  // map such that it could be modified directly.
  //

  @Override
  public void setIncompletePotentSteps(Map<String, DocDestinationStatus> value) {
    this.incompletePotentSteps.clear();
    this.incompletePotentSteps.putAll(value);
    this.statusChanges.clear();
    this.statusChanges.putAll(value);
  }

  @Override
  public boolean alreadyHasIncompleteStepList() {
    return this.incompletePotentSteps.size() > 0;
  }

  @Override
  public boolean isIncompletePotentStep(String stepName) {
    return incompletePotentSteps.containsKey(stepName);
  }

  @Override
  public String listIncompletePotentSteps() {
    return String.join(",", incompletePotentSteps.keySet());
  }

  @Override
  public Map<String, DocDestinationStatus> getStatusChanges() {
    return statusChanges;
  }

  @Override
  public String[] getIncompletePotentSteps() {
    return incompletePotentSteps.keySet().toArray(String[]::new);
  }

  @Override
  public void setStatusAll(Status status, String message, Object... args) {
    for (String step : incompletePotentSteps.keySet()) {
      setStatus(status, step, message, args);
    }
  }

  @Override
  public void removeDownStreamPotentStep(Router router, Step step) {
    log.trace("Removing destination step {} from {} after processing with {}", step.getName(), getId(), router.getStep().getName());
    if (incompletePotentSteps.remove(step.getName()) == null) {
      throw new RuntimeException("Tried to remove non-existent destination step! Router:" + router.getClass().getSimpleName() + " Step:" + step.getName());
    }
  }

  @Override
  public String dumpStatus() {
    return String.valueOf(incompletePotentSteps);
  }

  @Override
  public String getOrigination() {
    return this.origination;
  }
}
