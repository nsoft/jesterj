package org.jesterj.ingest.routers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.*;
import org.jesterj.ingest.model.impl.DocumentImpl;
import org.jesterj.ingest.model.impl.NamedBuilder;
import org.jesterj.ingest.model.impl.StepImpl;

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
   * @param destsSelected the steps that the document *will* be routed to.
   */
  public void updateExcludedDestinations(Document doc, Step... destsSelected) {
    // find everywhere we might have gone
    List<String> destinationsExcluded = new ArrayList<>(getStep().getOutputDestinationNames());
    Set<String> stillDownStream = new HashSet<>();
    if (destsSelected != null) {
      // find everywhere we are still going
      for (Step s : destsSelected) {
        Set<String> downstreamOutputSteps = s.getOutputDestinationNames();
        stillDownStream.addAll(downstreamOutputSteps);
      }
    }
    // remove places we are still going to
    for (String ps : stillDownStream) {
      destinationsExcluded.remove(ps);
    }

    // Now stepsExcluded should only include places that were possible that have become impossibe

    // now remove any step to which the document was not targeted (possibly because it has partially completed
    // and is now re-running due to FTI
    destinationsExcluded.removeIf((s) -> !doc.isPlanOutput(s));

    // don't remove the current step if it's potent or idempotent because we still need to record it as indexed
    destinationsExcluded.removeIf((s) -> {
      DocumentProcessor processor = ((StepImpl) getStep()).getProcessor();
      return (processor.isIdempotent()|| processor.isPotent()) && s.contains(getStep().getName());
    });

    // Now if anything remains in stepsExcluded, then it was a valid target that has become invalid due to the
    // router's routing decision

    if (destinationsExcluded.isEmpty()) {
      return;
    }
    ((DocumentImpl)doc).setStatusForDestinations(Status.DROPPED,  destinationsExcluded,"Document routed down path not leading to {} by {}", destinationsExcluded.toString(), getName());
    doc.reportDocStatus();
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
