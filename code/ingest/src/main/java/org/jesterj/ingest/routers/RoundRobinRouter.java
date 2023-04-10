package org.jesterj.ingest.routers;

import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.NextSteps;
import org.jesterj.ingest.model.Step;
import org.jesterj.ingest.model.impl.NamedBuilder;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("unused")
public class RoundRobinRouter extends RouterBase {
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
    int stepIndex = nextTarget.getAndIncrement() % getStep().getNextSteps().size();
    Collection<Step> values = getStep().getNextSteps().values();
    Iterator<Step> iter = values.iterator();
    Step selected = iter.next();
    for (int i = stepIndex ; i > 0; i--) {
       selected = iter.next();
    }
    updateExcludedDestinations(doc,selected);
    return new NextSteps(doc, selected);
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
      RoundRobinRouter object = getObj();
      setObj(new RoundRobinRouter());
      return object;
    }
  }
}
