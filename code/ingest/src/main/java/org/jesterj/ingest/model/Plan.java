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
public interface Plan extends JiniServiceProvider, Active {

  /**
   * Return every {@link Step} in the plan regardless of whether or not it will be executing when the plan is activated
   *
   * @return All steps
   */
  public Step[] getAllSteps();

  /**
   * Get the subset of steps that will execute when the plan is activated. This should be a continuous set of steps
   * that can be traversed in total via {@link org.jesterj.ingest.model.Step#next()}.
   *
   * @return
   */
  public Step[] getExecutableSteps();

  /**
   * Denote the start point for this node's execution of the plan. Calling this method on anything other
   * than the first step in the plan makes this a "helper" node.
   *
   * @param step
   */
  public void setFirstExecutableStep(Step step);


  /**
   * Denote the last step for this node's execution of the plan.
   *
   * @param step
   */
  public void setLastExecutableStep(Step step);

  /**
   * Is this plan installed as a helper, or as a primary. Implementations that need something other than
   * the default logic are almost inconceivable.
   *
   * @return true if the first step is not the first executible step, or the last step is not the
   *         last executable step.
   */
  default public boolean isHelping() {
    Step[] all = getAllSteps();
    Step[] exec = getExecutableSteps();
    return (all[0] != exec[0] || all[all.length - 1] != exec[exec.length - 1]);
  }

}
