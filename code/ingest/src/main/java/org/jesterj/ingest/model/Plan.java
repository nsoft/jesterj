/*
 * Copyright 2014-2016 Needham Software LLC
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

import guru.nidi.graphviz.engine.Format;
import guru.nidi.graphviz.engine.Renderer;
import org.jesterj.ingest.config.Transient;

public interface Plan extends Active, Configurable {

  /**
   * Return every {@link Step} in the plan regardless of whether it will be executing when the plan is activated
   *
   * @return All steps
   */
  Step[] getSteps();

  /**
   * Get the subset of steps that will execute when the plan is activated. This should be a continuous set of steps
   * that can be traversed in total via {@link org.jesterj.ingest.model.Step#getNextSteps(Document)}.
   *
   * @return an Array of steps in the plan that will be executed
   */
  @Transient
  Step[] getExecutableSteps();

  /**
   * This is the field that is to be used as a docId
   *
   * @return usually "id" but some legacy indexes may need to use something else
   */
  String getDocIdField();


  /**
   * Locate the step in the plan that has the given name. All steps in the plan must have a
   * unique name.
   *
   * @param stepName the name of the step to find
   * @return the step if found or null if the name does not match any steps.
   */
  Step findStep(String stepName);

  /**
   * Produce an image visualization of this plan
   *
   * @param format the format that should be rendered
   * @return An image visualizing the plan as a directed graph.
   */
  Renderer visualize(Format format);

  /**
   * A (usually serially incremented) version identifier for a plan. Changing the version of the plan indicates that
   * the processing has changed substantially, and that prior indexing no longer is considered to have produced
   * the desired output. The practical implication of changing the plan version is that all documents will become
   * eligible for re-indexing.
   *
   * @return a numeric identifier which changes when prior indexing is invalidated and there is a
   * need for re-indexing. Monotonically increasing positive numbers are recommended but not absolutely required.
   */
  int getVersion();

}
