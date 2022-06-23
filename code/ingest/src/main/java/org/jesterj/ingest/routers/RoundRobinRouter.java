package org.jesterj.ingest.routers;

import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.NextSteps;
import org.jesterj.ingest.model.Router;
import org.jesterj.ingest.model.Step;
import org.jesterj.ingest.model.impl.NamedBuilder;

import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinRouter implements Router {
  AtomicInteger nextTarget = new AtomicInteger(0);
  private Step[] availableSteps;
  private String name;

  @Override
  public NextSteps route(Document doc, LinkedHashMap<String, Step> nextSteps) {
    if (availableSteps == null) {
      availableSteps = nextSteps.values().toArray(new Step[0]);
    }
    int stepIndex = nextTarget.getAndIncrement() % availableSteps.length;
    return new NextSteps(doc, availableSteps[stepIndex]);
  }

  @Override
  public String getName() {
    return name;
  }

  public static class Builder extends NamedBuilder<RoundRobinRouter> {
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
