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

package org.jesterj.ingest.jini.service;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 9/28/14
 */

import org.jesterj.ingest.model.Plan;

public interface IngestService {
  /**
   * Return a descriptor sufficient to connect to a JavasSpace. If no JavaSpace exists one must be created
   * or an error thrown. Implementations may return a new space each time, or the same space each time at
   * their own discretion.
   * <p>
   * returns an object with sufficient information to identify and connect to a javaspace that may be used
   * for posting serialized items
   */
  void /*JavaSpace Descriptor Foo*/ supplySpace();

  /**
   * Install the supplied plan into this ingestion node. Installation of a plan removes the existing plan
   * from the node. The installed plan will be in the deactivated state until {@link #activate(Plan)} is
   * called.
   *
   * @param plan the plan to install.
   */
  void installPlan(Plan plan);


  void activate(Plan plan);

  void deactivate(Plan plan);
}
