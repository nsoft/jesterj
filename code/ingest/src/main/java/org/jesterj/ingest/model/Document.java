/*
 * Copyright 2014-2016 Needham Software LLC
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

package org.jesterj.ingest.model;

import com.google.common.collect.ListMultimap;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The publicly usable methods on a document. This interface is not meant to have multiple implementations. It mostly
 * exists to document the methods that authors of processors should interact with. There are multiple places where this
 * will get cast to DocumentImpl, but doing so inside a processor may be dangerous.
 */
public interface Document extends  Serializable {

  /**
   * The 'file_size' field which holds the size of the original content for an input document as
   * the framework first pulled it in.
   */
  String DOC_RAW_SIZE = "doc_raw_size";


  Set<String> keySet();

  boolean containsEntry(@Nullable String key, @Nullable String value);

  boolean remove(@Nullable String key, @Nullable String value);

  boolean containsValue(@Nullable String value);

  Collection<Map.Entry<String, String>> entries();

  boolean isEmpty();

  Map<String, Collection<String>> asMap();

  List<String> replaceValues(@Nullable String key, Iterable<? extends String> values);

  Collection<String> values();

  boolean containsKey(@Nullable String key);

  List<String> get(@Nullable String key);

  int size();

  List<String> removeAll(@Nullable String key);

  boolean putAll(@Nullable String key, Iterable<? extends String> values);

  boolean put(String field, String value);

  /**
   * Get the raw bytes from which this item was constructed. This is usually only used by the first or
   * second step in the pipeline which converts the binary form into entries in this map.
   *
   * @return the actual bytes of the document.
   */
  byte[] getRawData();

  /**
   * Replace the raw bytes. This is only used when the originally indexed document is to be interpreted as
   * a pointer to the "real" document, or when the Item is first constructed.
   *
   * @param rawData the actual bytes of the document
   */
  void setRawData(byte[] rawData);


  /**
   * The current processing status of the document relative to a given destinagion.
   *
   * @param outputDestination A destination for which the status is to be reported.
   * @return An enumeration value indicating whether the item is processing, errored out or complete.
   * @throws java.lang.IllegalStateException if the plan has not been set.
   */
  Status getStatus(String outputDestination);


  /**
   * Get a message relating to the processing status. This will typically be used to print the name of
   * The last successful processor, or the error message onto the item.
   *
   * @param outputStep the output step for which we want to know the message.
   * @return A short message suitable for logging and debugging (not a stack trace)
   */
  String getStatusMessage(String outputStep);

  @SuppressWarnings("unused")
  ListMultimap<String, String> getDelegate();

  /**
   * Returns the identifier for this document. This should be identical to get(getIdField()).
   *
   * @return the id
   */
  String getId();

  /**
   * A hash based on the contents of the delegate and the raw data.
   *
   * @return a hex string md5 checksum
   */
  String getHash();

  default String getHashAlg() {
    return "MD5";
  }

  String getIdField();

  Operation getOperation();

  String getSourceScannerName();

  String getFirstValue(String fieldName);

  String getParentId();

  String getOrignalParentId();

  boolean isStatusChanged();

  void reportDocStatus();

  /**
   * Ensures that this document will be fed into the plan regardless of memory or hashing settings. Has no
   * effect after the document exits the scanner.
   *
   * @param b true if the document should ignore hashing and memory settings.
   */
  void setForceReprocess(boolean b);

  boolean isForceReprocess();

  void setIncompleteOutputDestinations(Map<String, DocDestinationStatus> value);

  boolean alreadyHasIncompleteStepList();

  boolean isPlanOutput(String stepName);
  String listIncompleteOutputSteps();

  /**
   * Get the statuses that will be altered if reportDocStatus is invoked.
   *
   * @return a map of status changes.
   */
  DocStatusChange getStatusChange();

  List<String> listChangingDestinations();

  String[] getIncompleteOutputDestinations();

  /**
   * Set a status for the current down stream steps. The status message may contain '{}' and additional
   * arguments which will be substituted in the same manner as log4j logging messages. Since document objects must
   * remain serializable, these arguments should typically be reduced to strings if they are not already serializable.
   * <p>&nbsp;</p>
   * <p><strong>WARNING: this method has no persistent effect until {@link Document#reportDocStatus()} is called. If
   * the system is killed (power cord, whatever) before reportStatus() is completed this status change will not be
   * retained when JesterJ restarts.</strong></p>
   *
   * @param status The status to set for the destination step
   * @param message The user readable message explaining the status change
   * @param args values to be substituted into the message
   */
  void setStatus(Status status, String message, Serializable... args);

  /**
   * Remove a downstream potent step. This should only be performed by routers and step infrastructure, hence the
   * router argument. If you found a way to call this from your processor, please report a bug in our issue tracker.
   *
   * @param routerBase The router for the step in which the removal takes place.
   * @param name the name of the step to remove
   */
  void removeDownStreamOutputStep(Router routerBase, String name);

  /**
   * Get a string representation of the current status information. For debugging only.
   *
   * @return status information in string form.
   */
  String dumpStatus();

  /**
   * Identify if the document originated from Fault tolerance or a scan.
   *
   * @return the source type for the document for debugging.
   */
  String getOrigination();

  /**
   * For use during routing, to remove destinations from document duplicates before distribution to
   * downstream steps. This is only necessary for routers that route to more than one step and therefore
   * must clone documents. The clones need to be adjusted so that they do not have statuses for destinations
   * not downstream from their immediate targets, without causing updates to the removed destinations (which might
   * still be serviced by another clone). The returned destinations need to be aggregated and inspected to
   * subsequently determine if the routing is in effect dropping some destinations For example a router that
   * sends 2 copies to 2 out of 3 possible down stream steps has to only issue DROPPED status updates for the
   * 3rd step that doesn't get a clone of the original document.
   *
   * @param outputDestinationNames the correct destinations at which this clone will be targeted
   */
  void removeAllOtherDestinationsQuietly(Set<String> outputDestinationNames);

  /**
   * There are points in batch processing where the infrastructure wants to build bidirectional maps to relate
   * {code Document}s to their destination specific representation, and this can only be achieved if the destination
   * specific representation is unique. In such cases (and in any other case where {code Document} uniqueness is
   * important this method may be employed.
   *
   * @param fieldName the name of the field in which to store the nonce
   * @return the nonce value that was added.
   */
  String addNonce(String fieldName);

  enum Operation implements Serializable {
    NEW,
    UPDATE, // Note: most cases want NEW not update since search indexes usually overwrite rather than update
    DELETE
  }
}
