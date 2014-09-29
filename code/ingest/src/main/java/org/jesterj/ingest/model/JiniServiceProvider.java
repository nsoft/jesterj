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

package org.jesterj.ingest.model;/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 9/28/14
 */

public interface JiniServiceProvider {
  /**
   * Instructs the step to start advertising it's JINI services. Many implementations of this
   * method will simply call {@link #acceptJiniRequests()}, but may alternately throw
   * an <code>IllegalStateException</code> in special cases where configuration or resource
   * acquisition must complete before requests can be accepted.
   *
   * @throws IllegalStateException at the implementations discretion.
   */
  void advertise();

  /**
   * Instructs the object to stop advertising it's JINI services
   */
  void stopAdvertising();

  void acceptJiniRequests();

  void denyJiniRequests();

  /**
   * Get the readiness of this object for jini service connections
   *
   * @return
   */
  boolean readyForJiniRequests();
}
