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

import net.jini.space.JavaSpace;

import java.util.concurrent.BlockingQueue;

/**
 * A <code>Step</code> is a {@link BlockingQueue} with a capacity equal to it's concurrency level.
 * When the step is full, attempts to add an {@link org.jesterj.ingest.model.Item} will
 * throw IllegalStateException, and the calling code MUST check for and handle this condition
 * gracefully. If this is a primary node with no helpers steps should typically use
 * {@link #offer(Object)} or {@link #offer(Object, long, java.util.concurrent.TimeUnit)} instead of
 * add. However if there is an output {@link net.jini.space.JavaSpace} set then the typical
 * behavior would be to try an add, and if false is returned, serialize the item to the JavaSpace
 * so that helper nodes may process the overflow. For the last executable step of a helper node,
 * the step must never add or offer to the next step, but always place the <code>Item</code> in the
 * <code>JavaSpace</code>
 */
public interface Step extends Active, JiniServiceProvider, BlockingQueue<Item> {

  /**
   * Set the number of items to process concurrently.
   *
   * @return the batch size.
   */
  public int getBatchSize();

  /**
   * Get the next step in the plan
   *
   * @return the next step
   */
  public Step next();

  /**
   * Provide a JavaSpace that should be polled for items suitable for processing.
   *
   * @param space a space to poll for new items.
   */
  public void setInputJavaSpace(JavaSpace space);

  /**
   * Provide a {@link net.jini.space.JavaSpace} to which resulting Items should be serialized.
   * Generally items will only be placed in the output space if the next step is full, and they
   * cannot be processed locally. The exception to this is when the step is part of a plan that
   * returns true for {@link Plan#isHelping()} and this step is the last step returned by
   * {@link Plan#getExecutableSteps()}. In that case, the item will always be added to the output
   * JavaSpace
   *
   * @param space a space into which items may be placed for processing by other nodes.
   */
  public void setOutputJavaSpace(JavaSpace space);

  /**
   * Get the plan instance to which this step belongs.
   *
   * @return the plan (not a man, not a canal, not panama)
   */
  public Plan getPlan();

  /**
   * Determine if this step is the last step in a helper node. Implementations that need to overide the
   * default behavior are almost inconceivable.
   *
   * @return true if this is the last step in a helper node, false otherwise.
   */
  default public boolean isFinalHelper() {
    Plan plan = getPlan();
    if (plan == null) {
      return false; // if we aren't part of a plan yet, we aren't part of a helper node either.
    }
    Step[] executableSteps = plan.getExecutableSteps();
    return plan.isHelping() && executableSteps[executableSteps.length -1] == this;
  }

  /**
   * A name for this step to distinguish it from other steps in the UI. This value is generally supplied
   * by the plan author.
   *
   * @return The user supplied name for this step
   */
  public String getName();
}
