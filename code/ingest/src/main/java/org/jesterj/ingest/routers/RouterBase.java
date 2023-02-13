package org.jesterj.ingest.routers;

import org.apache.logging.log4j.ThreadContext;
import org.jesterj.ingest.model.*;
import org.jesterj.ingest.model.impl.NamedBuilder;

import java.util.*;
import java.util.stream.Collectors;

public abstract class RouterBase implements Router {

  Step step;
  String name;

  @Override
  public Step getStep() {
    return step;
  }

  /**
   * Sets dropped status for any destinations not reachable from any of the supplied steps.
   *
   * @param doc the document for which statuses need to be updated
   * @param dest the steps that the document *will* be routed to.
   */
  void updateExcludedDestinations(Document doc, Step... dest) {
    List<Step> stepsExcluded = new ArrayList<>(Arrays.asList(getStep().getDownstreamPotentSteps()));
    Set<Step> stillDownStream = new HashSet<>();
    if (dest != null) {
      for (Step s : dest) {
        stillDownStream.addAll(Arrays.asList(s.getDownstreamPotentSteps()));
      }
      for (Step ps : stillDownStream) {
        stepsExcluded.remove(ps);
      }
    }
    String saved = ThreadContext.get(Step.JJ_DOWNSTREAM_POTENT_STEPS);
    try {
      String dsPotentStepsToUpdate = stepsExcluded.stream()
          .map(Configurable::getName)
          .collect(Collectors.joining(","));
      ThreadContext.put(Step.JJ_DOWNSTREAM_POTENT_STEPS, dsPotentStepsToUpdate);
      doc.reportDocStatus(Status.DROPPED, dest == null, "Document routed down path not leading to this destination by {}", getName());
    } finally {
      ThreadContext.put(Step.JJ_DOWNSTREAM_POTENT_STEPS, saved);
    }
  }

  public abstract static class Builder<T extends RouterBase> extends NamedBuilder<RouterBase> {
    private T obj;

    public RouterBase.Builder<T> named(String name) {
      getObj().name = name;
      return this;
    }

    public RouterBase.Builder<T> forStep(Step step) {
      getObj().step = step;
      return this;
    }

    protected T getObj() {
      return obj;
    }

    public T build() {
      obj = null;
      return null; // abstract class
    }
  }

}
