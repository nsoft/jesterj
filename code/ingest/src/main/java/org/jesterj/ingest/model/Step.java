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

import java.util.concurrent.BlockingQueue;

/**
 * A <code>Step</code> is a {@link BlockingQueue} with a capacity equal to it's concurrency level.
 * When the step is full, attempts to add an {@link Document} will
 * throw IllegalStateException, and the calling code MUST check for and handle this condition
 * gracefully. If this is a primary node with no helpers steps should typically use
 * {@link #offer(Object)} or {@link #offer(Object, long, java.util.concurrent.TimeUnit)} instead of
 * add. However if there is an output {@link net.jini.space.JavaSpace} set then the typical
 * behavior would be to try an add, and if false is returned, serialize the item to the JavaSpace
 * so that helper nodes may process the overflow. For the last executable step of a helper node,
 * the step must never add or offer to the getNext step, but always place the <code>Item</code> in the
 * <code>JavaSpace</code>
 */
public interface Step extends Active, JiniServiceProvider, BlockingQueue<Document>, Runnable {

  /**
   * Set the number of items to process concurrently.
   *
   * @return the batch size.
   */
  int getBatchSize();

  /**
   * Get the getNext step in the plan for the given document
   *
   * @return the getNext step
   */
  Step getNext(Document d);

  /**
   * Get all the steps to which a document might travel
   *
   * @experimental
   * @return downstream steps
   */
  Step[] getSubsequentSteps();

  /**
   * Get the plan instance to which this step belongs.
   *
   * @return the plan (not a man, not a canal, not panama)
   */
  Plan getPlan();

  /**
   * Determine if this step is the last step in a helper node. Implementations that need to overide the
   * default behavior are almost inconceivable.
   *
   * @return true if this is the last step in a helper node, false otherwise.
   */
  default boolean isFinalHelper() {
    Plan plan = getPlan();
    if (plan == null) {
      return false; // if we aren't part of a plan yet, we aren't part of a helper node either.
    }
    Step[] executableSteps = plan.getExecutableSteps();
    return plan.isHelping() && executableSteps[executableSteps.length - 1] == this;
  }

  /**
   * A name for this step to distinguish it from other steps in the UI. This value is generally supplied
   * by the plan author. Every step in a plan must have a unique name.
   *
   * @return The user supplied name for this step
   */
  String getStepName();

  void sendToNext(Document doc);

}
