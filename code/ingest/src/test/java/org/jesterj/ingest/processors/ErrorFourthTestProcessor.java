package org.jesterj.ingest.processors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.Status;
import org.jesterj.ingest.model.impl.NamedBuilder;

public class ErrorFourthTestProcessor implements DocumentProcessor {
  private static final Logger log = LogManager.getLogger();
  int count = 0;
  private String name;
  @SuppressWarnings("unused")
  private String[] lastErrorId;
  private boolean shouldError = true;

  @Override
  public String getName() {
    return name;
  }

  public void setShouldError(boolean shouldError) {
    this.shouldError = shouldError;
  }

  @Override
  public Document[] processDocument(Document document) {
    count++;
    if (count % 5 == 4 && shouldError) {
      document.setStatus(Status.ERROR,"Unit Test 4th doc drop");
      log.info("Erroring {}",document.getId());
      lastErrorId[0] = document.getId();
    }
    log.info(getName() + " saw " + document.getId());
    return new Document[]{document};
  }


  public static class Builder extends NamedBuilder<ErrorFourthTestProcessor> {

    ErrorFourthTestProcessor obj = new ErrorFourthTestProcessor();

    @Override
    public ErrorFourthTestProcessor.Builder named(String name) {
      getObj().name = name;
      return this;
    }

    public ErrorFourthTestProcessor.Builder withErrorReporter(String[] reporter) {
      if (reporter.length != 1) {
        throw new IllegalArgumentException("only one error can be held, please pass array of length 1");
      }
      getObj().lastErrorId = reporter;
      return this;
    }

    public ErrorFourthTestProcessor.Builder erroringFromStart(boolean doErrors) {
      getObj().shouldError = doErrors;
      return this;
    }

    @Override
    protected ErrorFourthTestProcessor getObj() {
      return obj;
    }

    private void setObj(ErrorFourthTestProcessor obj) {
      this.obj = obj;
    }

    public ErrorFourthTestProcessor build() {
      ErrorFourthTestProcessor object = getObj();
      setObj(new ErrorFourthTestProcessor());
      return object;
    }

  }
}
