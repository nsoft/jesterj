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

public interface ItemProcessor {

  /**
   * Mutate, validate or transmit an item (to a search index). Implementations must not throw any
   * {@link java.lang.Throwable} that is not a JVM {@link java.lang.Error} All item processors are responsible
   * for setting a reasonable status and status message via {@link Item#setStatus(Status)} and
   * {@link Item#setStatusMessage(String)}. The item processor has no need to add the item to the next
   * step in the plan as this will be handled by the Step's infrastructure based on the status of the plan.
   *
   * @param item the item to process
   * @return true if processing succeeded, false otherwise.
   */
  public boolean processItem(Item item);

}
