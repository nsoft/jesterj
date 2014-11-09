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

package org.jesterj.ingest.model.impl;

import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.model.Step;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 10/9/14
 */
public class PlanImpl implements Plan {
  @Override
  public Step[] getAllSteps() {
    return new Step[0];
  }

  @Override
  public Step[] getExecutableSteps() {
    return new Step[0];
  }

  @Override
  public void setFirstExecutableStep(Step step) {

  }

  @Override
  public void setLastExecutableStep(Step step) {

  }

  @Override
  public String getDocIdField() {
    return null;
  }

  @Override
  public Step findStep(String stepName) {
    if (stepName == null) {
      return null;
    }
    for (int i = 0; i < this.getAllSteps().length; i++) {
      Step step = this.getAllSteps()[i];
      if (stepName.equals(step.getName())) {
        return step;
      }
    }
    return null;
  }

  @Override
  public void activate() {

  }

  @Override
  public void deactivate() {

  }

  @Override
  public boolean isActive() {
    return false;
  }

  @Override
  public void advertise() {

  }

  @Override
  public void stopAdvertising() {

  }

  @Override
  public void acceptJiniRequests() {

  }

  @Override
  public void denyJiniRequests() {

  }

  @Override
  public boolean readyForJiniRequests() {
    return false;
  }
}
