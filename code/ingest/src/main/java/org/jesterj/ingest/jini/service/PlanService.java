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

package org.jesterj.ingest.jini.service;/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 9/28/14
 */

public interface PlanService {

  /**
   * Simply returns an array of descriptors sufficient to fetch the services related to all
   * steps in the plan. If this is a helper node, this will include steps that will never run
   * because they are before or after the first and last helper steps. Future versions may
   * support branching and merging plans, and the ordering of returned steps will be defined
   * at that time.
   *
   * return An array of step service descriptors in the order that the steps execute.
   */
  void /* Step Service Descriptor Foo[] */ getAllSteps();

  /**
   * Fetches an array of descriptors sufficient to fetch the services that relate to all
   * steps in the plan that will execute.
   */
  void /* Step Service Descriptor Foo[] */ getExecutableSteps();

  /**
   * Sends a message to the first executable node to begin processing. If this node is a
   * scanner step (i.e. file system watcher, or web crawler) with a schedule, the schedule
   * will initiate
   */
  void activate();

}
