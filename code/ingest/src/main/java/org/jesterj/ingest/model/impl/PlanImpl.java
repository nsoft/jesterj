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

package org.jesterj.ingest.model.impl;

import com.google.common.collect.ArrayListMultimap;
import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.model.Step;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 10/9/14
 */
public class PlanImpl implements Plan {

  private LinkedHashMap<String, Step> steps;
  private String idField;

  protected PlanImpl() {
  }

  @Override
  public Step[] getAllSteps() {
    return steps.values().toArray(new Step[steps.values().size()]);
  }

  @Override
  public Step[] getExecutableSteps() {
    // for now... 
    return steps.values().toArray(new Step[steps.values().size()]);
  }


  @Override
  public String getDocIdField() {
    return idField;
  }

  @Override
  public Step findStep(String stepName) {
    if (stepName == null) {
      return null;
    }
    for (int i = 0; i < this.getAllSteps().length; i++) {
      Step step = this.getAllSteps()[i];
      if (stepName.equals(step.getStepName())) {
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


  public static class Builder {

    PlanImpl obj = new PlanImpl();
    /**
     * The steps already built
     */
    LinkedHashMap<String, Step> steps = new LinkedHashMap<>();
    /**
     * The steps yet to be built
     */
    LinkedHashMap<String, StepImpl.Builder> builders = new LinkedHashMap<>();

    /**
     * A list of steps waiting on successors to build. Any time a successor appears in this
     * list we have a cycle and we should throw a CyclicGraphException.
     */
    List<StepImpl.Builder> pendingBuilders = new ArrayList<>();

    /**
     * The predecessor steps, the key is the name of the step and the value is the predecessor
     * a key with multiple values implies a node that coalesces two or mor paths in the DAG.
     * two keys with the same value implies a node that is a fork in the dag. Scanners will not have
     * predecessors and therefore will not appear as keys in this map.
     */
    ArrayListMultimap<String, String> predecessors = ArrayListMultimap.create();

    /**
     * Add a step to the plan. Will automatically append the supplied step to the specified predecessor step.
     * If a step is added twice with different predecessors this represents a node that coalesces two paths in the DAG.
     * If more than one step exists for the same predecessor this represents a fork in the DAG. Steps never
     * know from whence a document was handed to them.
     *
     * @param predecessors the steps that this step should follow. If null, step must build a scanner. The step must have a
     *                     step name that is unique.
     * @param step         the step to add, must not be null
     */
    public Builder addStep(String[] predecessors, StepImpl.Builder step) {
      if ((predecessors == null || predecessors.length == 0) && !(step instanceof ScannerImpl.Builder)) {
        throw new IllegalArgumentException("Only scanners can have no predecessor");
      }
      if (builders.get(step.getStepName()) != null) {
        throw new IllegalArgumentException("Cannot add the same step twice. A step named " +
            step.getStepName() + " has already been added.");
      }
      builders.put(step.getStepName(), step);
      if (predecessors != null) {
        for (String predecessor : predecessors) {
          this.predecessors.put(step.getStepName(), predecessor);
        }
      }
      return this;
    }

    List<StepImpl.Builder> findScanners() {
      List<StepImpl.Builder> scanners = new ArrayList<>();
      for (String stepName : builders.keySet()) {
        if (!predecessors.keySet().contains(stepName)) {
          scanners.add(builders.get(stepName));
        }
      }
      return scanners;
    }

    public Plan build() {
      List<StepImpl.Builder> scanners = findScanners();
      for (StepImpl.Builder scanner : scanners) {
        buildStep(scanner);
      }
      getObj().steps = this.steps;
      return getObj();
    }

    private void buildStep(StepImpl.Builder builder) {
      if (pendingBuilders.contains(builder)) {
        throw new CyclicGraphException("Step " + builder.getStepName() + " is referenced by one of it's descendants");
      }
      Set<String> successors = predecessors.keySet().stream()
          .filter(stepName -> predecessors.get(stepName).contains(builder.getStepName())).collect(Collectors.toSet());

      List<String> unbuiltSuccessors = successors.stream()
          .filter(stepName -> !steps.keySet().contains(stepName)).collect(Collectors.toList());

      if (unbuiltSuccessors.size() > 0) {
        pendingBuilders.add(builder);
        for (String unbuiltSuccessor : unbuiltSuccessors) {
          buildStep(builders.remove(unbuiltSuccessor));
        }
      }

      for (String successor : successors) {
        builder.addNextStep(steps.get(successor));
      }
      steps.put(builder.getStepName(), builder.build());
    }

    protected PlanImpl getObj() {
      return obj;
    }

    public Builder withIdField(String id) {
      getObj().idField = id;
      return this;
    }
  }
}
