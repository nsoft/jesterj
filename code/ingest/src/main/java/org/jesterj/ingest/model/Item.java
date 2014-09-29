/*
 * Copyright 2014 Needham Software LLC
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

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 9/28/14
 */

import com.google.common.collect.ListMultimap;

public interface Item extends ListMultimap<String, String> {

  /**
   * Get the raw bytes from which this item was constructed. This is usually only used by the first or
   * second step in the pipeline which converts the binary form into entries in this map.
   *
   * @return
   */
  byte[] getRawData();

  /**
   * Replace the raw bytes. This is only used when the originally indexed document is to be interpreted as
   * a pointer to the "real" document, or when the Item is first constructed.
   *
   * @param rawData
   */
  void setRawData(byte[] rawData);

  /**
   * A reference to the scanner that originally generated this item. This is generally used for reporting
   * the status of an item as it moves down the pipeline.
   *
   * @return
   */
  public Scanner getSource();

  /**
   * The current processing status of the item. Each {@link org.jesterj.ingest.model.ItemProcessor}
   * is responsible for releasing the item with a correct status.
   *
   * @return An enumeration value indicating whether the item is processing, errored out or complete.
   */
  public Status getStatus();

  /**
   * Set the status
   *
   * @see #getStatus()
   * @param status The new status.
   */
  public void setStatus(Status status);

  /**
   * Get a message relating to the processing status. This will typically be used to print the name of
   * The last successful processor, or the error message onto the item.
   *
   * @return A short message suitable for logging and debugging (not a stack trace)
   */
  public String getStatusMessage();

  /**
   * Set the status message.
   *
   * @see #getStatus()
   * @param message the status message.
   */
  public void setStatusMessage(String message);

}
