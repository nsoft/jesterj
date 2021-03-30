package org.jesterj.ingest.processors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.impl.NamedBuilder;

public class PauseEveryFiveTestProcessor implements DocumentProcessor {
  private static final Logger log = LogManager.getLogger();
  int count = 0;
  private String name;

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Document[] processDocument(Document document) {
    count++;
    if (count % 5 == 1 && count > 1) {
      try {
        Thread.sleep(60000);
      } catch (InterruptedException e) {
        count = 0;
        return new Document[0];
      }
    }
    log.info(this.getClass().getSimpleName() + " saw " + document.getId());
    return new Document[]{document};
  }

  public static class Builder extends NamedBuilder<PauseEveryFiveTestProcessor> {

    PauseEveryFiveTestProcessor obj = new PauseEveryFiveTestProcessor();

    @Override
    public PauseEveryFiveTestProcessor.Builder named(String name) {
      getObj().name = name;
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
