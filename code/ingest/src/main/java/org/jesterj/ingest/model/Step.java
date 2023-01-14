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

import org.jesterj.ingest.model.impl.StepImpl;

import java.util.LinkedHashMap;
import java.util.concurrent.BlockingQueue;

/**
 * A <code>Step</code> is a {@link BlockingQueue} with a capacity equal to it's concurrency level.
 * When the step is full, attempts to add an {@link Document} will
 * throw IllegalStateException, and the calling code MUST check for and handle this condition
 * gracefully. If this is a primary node with no helpers steps should typically use
 * {@link #offer(Object)} or {@link #offer(Object, long, java.util.concurrent.TimeUnit)} instead of
 * add. However, if there is an output {@link net.jini.space.JavaSpace} set then the typical
 * behavior would be to try an add, and if false is returned, serialize the item to the JavaSpace
 * so that helper nodes may process the overflow. For the last executable step of a helper node,
 * the step must never add or offer to the getNext step, but always place the <code>Item</code> in the
 * <code>JavaSpace</code>
 */
public interface Step extends Active, JiniServiceProvider, BlockingQueue<Document>, Runnable, DeferredBuilding, Configurable {

  /**
   * Set the number of items to process concurrently.
   *
   * @return the batch size.
   */
  int getBatchSize();

  /**
   * Get the next step in the plan for the given document
   *
   * @return the getNext step
   * @param d the document for which a next step should be determined.
   */
  NextSteps getNextSteps(Document d);



  /**
   * Get the plan instance to which this step belongs.
   *
   * @return the plan (not a man, not a canal, not panama)
   */
  Plan getPlan();

  /**
   * After processing is complete, send it on to any subsequent steps if appropriate. This
   * method may inspect the document status and if the document is not dropped, errored,
   * etc. and there are multiple possible destination steps it should invoke the router to
   * determine the appropriate destinations and conduct the submission of the results to the
   * indicated steps.
   *
   * @param doc The document for which processing is complete.
   */
  void sendToNext(Document doc);

  /**
   * Identify the downstream steps that must only be executed once per document.
   *
   * @return The steps downstream from this one that are neither safe nor idempotent.
   */
  Step[] getDownstreamPotentSteps();


  /**
   * The steps that are reachable from this step.
   *
   * @return A map of steps keyed by their names.
   */
  LinkedHashMap<String, Step> getNextSteps();

  /**
   * Determine if any upstream steps are still active. A true result implies that
   * documents may yet be recieved for processing, and it is not safe to shut down
   * the processing thread for this step.
   *
   * @return true if any immediately prior steps are still active
   */
  boolean isActivePriorSteps();

  /**
   * Register a step as a predecessor of this step (one that might send documents to
   * this step).
   *
   * @param obj The step to register as a potential upstream source of documents.
   */
  void addPredecessor(StepImpl obj);
}
