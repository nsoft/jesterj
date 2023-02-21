package org.jesterj.ingest.processors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.impl.NamedBuilder;

public class PauseEveryFiveTestProcessor implements DocumentProcessor {
  private static final Logger log = LogManager.getLogger();
  private int count = 0;
  private String name;
  private long millis;

  @Override
  public String getName() {
    return name;
  }

  public void setMillis (int millis) {
    this.millis = millis;
  }

  @Override
  public Document[] processDocument(Document document) {
    count++; // first is #1
    if (count % 5 == 1 && count > 1) {
      try {
        log.warn("sleeping {} starting {}", Thread.currentThread().getName(), this.getName());
        Thread.sleep(millis);
        log.warn("sleeping done {}", this.getName());
      } catch (InterruptedException e) {
        log.warn("sleeping interrupted {}", this.getName());

        count = 0;
        return new Document[0];
      }
    }
    log.trace("FOO: {} sending {} with {}", getName(), document.getId(), Thread.currentThread().getName());
    return new Document[]{document};
  }

  public static class Builder extends NamedBuilder<PauseEveryFiveTestProcessor> {

    PauseEveryFiveTestProcessor obj = new PauseEveryFiveTestProcessor();

    @Override
    public PauseEveryFiveTestProcessor.Builder named(String name) {
      getObj().name = name;
      return this;
    }

    public PauseEveryFiveTestProcessor.Builder pausingFor(long millis) {
      getObj().millis = millis;
      return this;
    }

    @Override
    protected PauseEveryFiveTestProcessor getObj() {
      return obj;
    }

    private void setObj(PauseEveryFiveTestProcessor obj) {
      this.obj = obj;
    }

    public PauseEveryFiveTestProcessor build() {
      PauseEveryFiveTestProcessor object = getObj();
      setObj(new PauseEveryFiveTestProcessor());
      return object;
    }

  }
}
