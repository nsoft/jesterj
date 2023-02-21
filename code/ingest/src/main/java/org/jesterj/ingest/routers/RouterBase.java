package org.jesterj.ingest.routers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.*;
import org.jesterj.ingest.model.impl.NamedBuilder;

import java.util.*;

public abstract class RouterBase implements Router {

  @SuppressWarnings("unused")
  private static final Logger log = LogManager.getLogger();
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
    // find everywhere we might have went
    List<Step> stepsExcluded = new ArrayList<>(Arrays.asList(getStep().geOutputSteps()));
    Set<Step> stillDownStream = new HashSet<>();
    if (dest != null) {
      // find everywhere we are still going
      for (Step s : dest) {
        Step[] downstreamOutputSteps = s.geOutputSteps();
        List<Step> c = Arrays.asList(downstreamOutputSteps);
        stillDownStream.addAll(c);
      }
    }
    // remove places we are still going to
    for (Step ps : stillDownStream) {
      stepsExcluded.remove(ps);
    }

    // Now stepsExcluded should only include places that were possible that have become impossibe

    // now remove any step to which the document was not targeted (possibly because it has partially completed
    // and is now re-running due to FTI
    stepsExcluded.removeIf((s) -> !doc.isPlanOutput(s.getName()));

    // Now if anything remains in stepsExcluded, then it was a valid target that has become invalid due to the
    // router's routing decision

    for (Step step : stepsExcluded) {
      // drop anything that is not the current step.
      doc.setStatus(Status.DROPPED,  step.getName(),"Document routed down path not leading to {} by {}", step.getName(), getName());
      doc.removeDownStreamOutputStep(this,step);
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
