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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;

public interface Document extends ListMultimap<String, String>, Serializable {

  /**
   * The 'file_size' field which holds the size of the original content for an input document as
   * the framework first pulled it in.
   */
  String DOC_RAW_SIZE = "doc_raw_size";

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
   * The current processing status of the item. Each {@link DocumentProcessor}
   * is responsible for releasing the item with a correct status.
   *
   * @return An enumeration value indicating whether the item is processing, errored out or complete.
   * @throws java.lang.IllegalStateException if the plan has not been set.
   */
  Status getStatus();

  /**
   * Set the status
   *
   * @param status The new status.
   * @see #getStatus()
   */
  void setStatus(Status status);

  void setStatus(Status status, String statusMessage);

  /**
   * Get a message relating to the processing status. This will typically be used to print the name of
   * The last successful processor, or the error message onto the item.
   *
   * @return A short message suitable for logging and debugging (not a stack trace)
   */
  String getStatusMessage();

  /**
   * Set the status message.
   *
   * @param message the status message.
   * @see #getStatus()
   */
  void setStatusMessage(String message);

  @SuppressWarnings("unused")
  ArrayListMultimap<String, String> getDelegate();

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

    @NotNull
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

  void reportDocStatus(Status status, String message, Object... messageParams);

    void setForceReprocess(boolean b);

  boolean isForceReprocess();

  enum Operation implements Serializable {
    NEW,
    UPDATE, // Note: most cases want NEW not update since search indexes usually overwrite rather than update
    DELETE
  }
}
