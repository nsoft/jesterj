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
import guru.nidi.graphviz.attribute.Color;
import guru.nidi.graphviz.attribute.Style;
import guru.nidi.graphviz.engine.Format;
import guru.nidi.graphviz.engine.Graphviz;
import guru.nidi.graphviz.engine.Renderer;
import guru.nidi.graphviz.model.Factory;
import guru.nidi.graphviz.model.Graph;
import guru.nidi.graphviz.model.Node;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.Main;
import org.jesterj.ingest.config.Transient;
import org.jesterj.ingest.model.Configurable;
import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.model.Scanner;
import org.jesterj.ingest.model.Step;

import java.util.*;
import java.util.stream.Collectors;

import static guru.nidi.graphviz.model.Factory.graph;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 10/9/14
 */
public class PlanImpl implements Plan {
  private static final Logger log = LogManager.getLogger();
  private LinkedHashMap<String, Step> stepsMap;
  private String idField;
  private boolean active = false;
  private String name;
  private int planVersion;

  protected PlanImpl() {
  }

  @Override
  public Step[] getSteps() {
    return getStepsMap().values().toArray(new Step[0]);
  }

  @Override
  public Step[] getExecutableSteps() {
    // for now...
    return getStepsMap().values().toArray(new Step[0]);
  }


  @Override
  public String getDocIdField() {
    return getIdField();
  }

  @Override
  public Step findStep(String stepName) {
    if (stepName == null) {
      return null;
    }
    for (int i = 0; i < this.getSteps().length; i++) {
      Step step = this.getSteps()[i];
      if (stepName.equals(step.getName())) {
        return step;
      }
    }
    return null;
  }

  @Override
  public Renderer visualize(Format format) {
    Map<String, Node> nodes = new LinkedHashMap<>();
    List<Step> scanners = new ArrayList<>();
    for (Step step : getSteps()) {
      if (step instanceof Scanner) {
        scanners.add(step);
      }
    }
    List<String> linkedSteps = new ArrayList<>();
    for (Step step : scanners) {
      linkUp(nodes, linkedSteps, (StepImpl) step); // build nodes recursively
    }
    Graph g = graph("visualize").directed();
    for (Step scanner : scanners) {
      String label = scanner.getName();
      if (scanner.getRouter() != null) {
        label += "\n(" +scanner.getRouter().getName()+ ")";
      }
      Node node = nodes.get(label);
      g = g.with(node);
    }
    Graphviz tmp = Graphviz.fromGraph(g);
    return tmp.render(format);
  }

  @Override
  public int getVersion() {
    return planVersion;
  }

  private void linkUp(Map<String, Node> nodes, List<String> knownSteps, StepImpl step) {
    LinkedHashMap<String, Step> nextSteps = step.getNextSteps();
    String label = getLabel(step);
    Node node = nodes.computeIfAbsent(label, Factory::node);
    if (step instanceof Scanner) {
      node = node.with(Color.BLUE, Style.lineWidth(3));
      nodes.replace(label, node);
    }
    if (step.getProcessor().isIdempotent()) {
      node = node.with(Color.PLUM, Style.lineWidth(3));
      nodes.replace(label, node);
    }
    if (step.getNextSteps().isEmpty() || step.getProcessor().isPotent()) {
      node = node.with(Color.RED, Style.lineWidth(3));
      nodes.replace(label, node);
    }

    knownSteps.add(label);
    if (nextSteps.size() == 0) {
      return;
    }
    for (Step subsequentStep : nextSteps.values()) {
      String subsequentLabel = getLabel(subsequentStep);
      if (!knownSteps.contains(subsequentLabel)) {
        // new node, need to recurse
        linkUp(nodes, knownSteps, (StepImpl) subsequentStep);  // yuck, but I don't really want to expose next steps in interface either
      }
      Node nextNode = nodes.get(subsequentLabel);
      node = node.link(nextNode);
      // link returns an immutable copy of the node we just created, so we need
      // to throw out the original and keep the copy
      nodes.put(label, node);
    }
  }

  private static String getLabel(Step step) {
    String label = step.getName();
    if (step.getRouter() != null) {
      label = String.format("%s\n(%s)",label, step.getRouter().getName());
    }
    return label;
  }

  @Override
  public synchronized void activate() {
    String planName = getName();
    log.info("Activating plan '{}'", planName);
    register();
    this.setActive(true);
    getStepsMap().values().parallelStream().forEach(Step::activate);
    log.info("Activation of plan {} complete", planName);
  }

  void register() {
    Main.registerPlan(this);
  }

  @Override
  public synchronized void deactivate() {
    if (!isActive()) {
      return;
    }
    log.info("Deactivating plan '{}'", getName());
    // Need to ensure that we never have a live thread feeding documents to a step that is already stopped.
    // since this will cause potential deadlock if the queue fills up and the step is blocked on a put()
    // while holding it's own monitor... so complications below for thread safety. Below we refer to
    // any connected sets of steps from the upstream side that do not have multiple inputs as a "level"
    // We process deactivations level by level.

    Set<Step> nextLevel = getStepsMap().values().stream().filter(s -> s instanceof Scanner).collect(Collectors.toCollection(LinkedHashSet::new));
    Set<Step> currentLevel;
    Set<Step> finalNextLevel = nextLevel;
    log.info("Scanners found: {}", () -> finalNextLevel.stream().map(Configurable::getName).collect(Collectors.toSet()));
    do {
      currentLevel = nextLevel;
      nextLevel = new LinkedHashSet<>();
      for (Step step : currentLevel) {
        if (step.isActive()) {
          step.deactivate();
        }
        for (Step nextStep : step.getNextSteps().values()) {
          deactivateStep(nextStep, nextLevel);
        }
      }
    } while (!nextLevel.isEmpty());
    this.setActive(false);
    Main.deregisterPlan(this);
  }

  private void deactivateStep(Step step, Set<Step> nextLevel) {
    if (step.isActivePriorSteps()) {
      nextLevel.add(step);
      return;
    }
    if (step.isActive()) {
      step.deactivate();
    }
    for (Step nextStep : step.getNextSteps().values()) {
      deactivateStep(nextStep, nextLevel);
    }

  }

  @Transient
  @Override
  public synchronized boolean isActive() {
    return active;
  }

  @Override
  public String getName() {
    return name;
  }

  LinkedHashMap<String, Step> getStepsMap() {
    return stepsMap;
  }

  void setStepsMap(LinkedHashMap<String, Step> stepsMap) {
    this.stepsMap = stepsMap;
  }

  String getIdField() {
    return idField;
  }

  void setIdField(String idField) {
    this.idField = idField;
  }

  void setActive(boolean active) {
    this.active = active;
  }

  void setName(String name) {
    this.name = name;
  }


  public static class Builder extends NamedBuilder<Plan> {

    PlanImpl obj = new PlanImpl();
    /**
     * The stepsMap already built
     */
    LinkedHashMap<String, Step> steps = new LinkedHashMap<>();
    /**
     * The stepsMap yet to be built
     */
    LinkedHashMap<String, StepImpl.Builder> builders = new LinkedHashMap<>();

    /**
     * A list of stepsMap waiting on successors to build. Any time a successor appears in this
     * list we have a cycle, and we should throw a CyclicGraphException.
     */
    List<StepImpl.Builder> pendingBuilders = new ArrayList<>();

    /**
     * The predecessor stepsMap, the key is the name of the step and the value is the predecessor
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
     * @param step         the step to add, must not be null
     * @param predecessors the stepsMap that this step should follow. If null, step must build a scanner. The step must have a
     *                     step name that is unique.
     * @return this builder object for chaining
     */
    public Builder addStep(StepImpl.Builder step, String... predecessors) {
      if (!step.isValid()) {
        throw new RuntimeException("Invalid configuration for step " + step.getStepName());
      }
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
          if (!builders.containsKey(predecessor)) {
            throw new IllegalArgumentException("Unknown Step as predecessor:" + predecessor);
          }
          this.predecessors.put(step.getStepName(), predecessor);
        }
      }
      return this;
    }


    List<StepImpl.Builder> findScanners() {
      return builders.keySet().stream().filter(stepName ->
              !predecessors.keySet().contains(stepName))
          .map(stepName -> builders.get(stepName))
          .collect(Collectors.toList());
    }

    public Plan build() {
      if (!isValid()) {
        throw new RuntimeException("Invalid configuration, cannot build plan named " + getObj().getName());
      }
      List<StepImpl.Builder> scanners = findScanners();
      scanners.forEach(this::buildStep);
      PlanImpl obj = getObj();
      this.obj = new PlanImpl();
      obj.setStepsMap(this.steps);
      for (Step step : steps.values()) {
        ((StepImpl) step).setPlan(obj); // get with the plan...
      }
      return obj;
    }

    private void buildStep(StepImpl.Builder builder) {
      if (pendingBuilders.contains(builder)) {
        throw new CyclicGraphException("Step " + builder.getStepName() + " is referenced by one of it's descendants");
      }
      Set<String> successors = predecessors.keySet().stream()
          .filter(stepName -> predecessors.get(stepName).contains(builder.getStepName())).collect(Collectors.toSet());

      List<String> unbuiltSuccessors = successors.stream()
          .filter(stepName -> !steps.containsKey(stepName)).collect(Collectors.toList());

      if (unbuiltSuccessors.size() > 0) {
        pendingBuilders.add(builder);
        for (String unbuiltSuccessor : unbuiltSuccessors) {
          buildStep(builders.remove(unbuiltSuccessor));
        }
      }

      for (String successor : successors) {
        builder.addNextStep(steps.get(successor));
      }
      StepImpl step = builder.build();

      String stepName = step.getName();
      steps.put(stepName, step);
    }

    public Builder named(String name) {
      getObj().setName(name);
      return this;
    }

    @SuppressWarnings("unused")
    public Builder withVersion(int version) {
      getObj().planVersion = version;
      return this;
    }

    protected PlanImpl getObj() {
      return obj;
    }

    public Builder withIdField(String id) {
      getObj().setIdField(id);
      return this;
    }
  }

}
