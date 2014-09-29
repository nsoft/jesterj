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

package org.solrsystem.ingest.model;/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 9/28/14
 */

import com.google.common.collect.ListMultimap;

public interface Item extends ListMultimap<String, String> {

  byte[] getRawData();

  void setRawData(byte[] rawData);

  /**
   * This value is intended to be set as the item is added to the queue. It should be set by
   * the add method of each step, with an incrementing serial number that allows the queue
   * to efficiently split it's contents for parallel processing with a modulo function.
   *
   * @return a serial integer set by the last <code>Step</code> to which this item was added.
   */
  public int getQueueEntryNumber();

  public void setQueueEntryNumber(int number);

  public Scanner getSource();

  public Status getStatus();

  public void setStatus(Status status);

}
