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

import net.jini.space.JavaSpace;

import java.util.concurrent.BlockingQueue;

/**
 * A <code>Step</code> is a {@link BlockingQueue} with a capacity equal to it's concurrency level.
 * When the step is full, attempts to add an {@link org.solrsystem.ingest.model.Item} will
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

  public int getBatchSize();

  public Step next();

  /**
   * Provide a JavaSpace that should be polled for items suitable for processing.
   *
   * @param space
   */
  public void setInputJavaSpace(JavaSpace space);

  /**
   * Provide a JavaSpace to which resulting Items should be serialized. Note that
   *
   * @param space
   */
  public void setOutputJavaSpace(JavaSpace space);


}
