package org.jesterj.ingest.routers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.NextSteps;
import org.jesterj.ingest.model.Step;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A router that selects among the next steps in a round-robin fashion. Note that in some cases where
 * a document is being reprocessed and some of the next steps are not suitable for the document's listed
 * destination, this implementation will skip ahead to the next eligible step. Thus, if absolute long term
 * fairness is desired a different router implementation should be selected (or written).
 */
@SuppressWarnings("unused")
public class RoundRobinRouter extends RouterBase {
  private static final Logger log = LogManager.getLogger();
  AtomicInteger nextTarget = new AtomicInteger(0);


  @Override
  public boolean isDeterministic() {
    return false;
  }

  @Override
  public boolean isConstantNumberOfOutputDocs() {
    return true;
  }

  @Override
  public int getNumberOfOutputCopies() {
    return 1;
  }

  @Override
  public NextSteps route(Document doc) {
    Step selected;
    int tries = 0;
    int stepsAvail = getStep().getNextSteps().size();
    LinkedHashMap<String, Step> eligibleNextSteps = getStep().getEligibleNextSteps(doc);
    do {
      selected = selectNextStep();
      tries++;
    } while(tries <= stepsAvail && !eligibleNextSteps.containsValue(selected));
    if (tries > stepsAvail) {
      log.error("Unable to find eligible next step for {}", doc.getId());
      throw new RuntimeException("Invalid router implementation, or bug. No next step selected by " + getName() + " for " + doc);
    }
    updateExcludedDestinations(doc,selected);
    return new NextSteps(doc, selected);
  }

  private Step selectNextStep() {
    int stepIndex = nextTarget.getAndIncrement() % getStep().getNextSteps().size();
    Collection<Step> values = getStep().getNextSteps().values();
    Iterator<Step> iter = values.iterator();
    Step selected = iter.next();
    for (int i = stepIndex ; i > 0; i--) {
       selected = iter.next();
    }
    return selected;
  }

  @Override
  public String getName() {
    return name;
  }

  public static class Builder extends RouterBase.Builder<RoundRobinRouter> {
    private RoundRobinRouter obj = new RoundRobinRouter();

    public Builder named(String name) {
      getObj().name = name;
      return this;
    }

    protected RoundRobinRouter getObj() {
      return obj;
    }

    private void setObj(RoundRobinRouter obj) {
      this.obj = obj;
    }

    public RoundRobinRouter build() {
      if (getObj().name == null) {
        throw new IllegalStateException("Name of router must nto be null");
      }
      RoundRobinRouter object = getObj();
      setObj(new RoundRobinRouter());
      return object;
    }
  }
}
