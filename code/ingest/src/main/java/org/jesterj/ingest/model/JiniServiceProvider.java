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

/**
 * Interface for coordinating Jini services. {@link org.jesterj.ingest.model.Plan} objects will must
 * iterate their items when advertising and jini services are being turned on and off so that all
 * steps in the plan are enabled and disabled as a unit when the plan is enabled or disabled.
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
   * Instructs the object to stop advertising it's JINI services.
   */
  void stopAdvertising();

  /**
   * Start up the Jini unicast discovery server, and respond to service requests.
   */
  void acceptJiniRequests();

  /**
   * Shut down the Jini unicast discovery server, and stop responding to service requests.
   * {@link #stopAdvertising()} must be called before this method, otherwise an exception
   * will be thrown.
   *
   * @throws java.lang.IllegalStateException if services are still being advertised.
   */
  void denyJiniRequests();

  /**
   * Get the readiness of this object for jini service connections
   *
   * @return
   */
  boolean readyForJiniRequests();
}
