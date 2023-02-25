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

import org.jesterj.ingest.model.impl.StepImpl;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

/**
 * A <code>Step</code> is a {@link BlockingQueue} with a capacity equal to its concurrency level.
 * When the step is full, attempts to add an {@link Document} will
 * throw IllegalStateException, and the calling code MUST check for and handle this condition
 * gracefully.
 */
public interface Step extends Active, BlockingQueue<Document>, Runnable, DeferredBuilding, Configurable {

  String JJ_PLAN_VERSION = "JJ_PLAN_VERSION";
  String JJ_PLAN_NAME = "JJ_PLAN_NAME";

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

  Set<String> getOutputDestinationNames();

  /**
   * Identify the downstream steps that must only be executed once per document.
   *
   * @return The steps downstream from this one that are neither safe nor idempotent.
   */
  Set<Step> getDownstreamOutputSteps();


  boolean isOutputStep();

  /**
   * The steps that are reachable from this step.
   *
   * @return A map of steps keyed by their names.
   */
  LinkedHashMap<String, Step> getNextSteps();

  /**
   * Determine if any upstream steps are still active. A true result implies that
   * documents may yet be received for processing, and it is not safe to shut down
   * the processing thread for this step.
   *
   * @return true if any immediately prior steps are still active
   */
  boolean isActivePriorSteps();

  // visible for testing
  List<Step> getPriorSteps();

  /**
   * Register a step as a predecessor of this step (one that might send documents to
   * this step).
   *
   * @param obj The step to register as a potential upstream source of documents.
   */
  void addPredecessor(StepImpl obj);

  Router getRouter();
}
