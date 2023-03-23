package org.jesterj.ingest.processors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.impl.NamedBuilder;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class generalizes ErrorFourthTestProcessor to provide any repeating pattern of errors and no-errors
 */
public class ErrorPatternTestProcessor implements DocumentProcessor {
  private static final Logger log = LogManager.getLogger();
  AtomicInteger count = new AtomicInteger(0);
  private String name;
  @SuppressWarnings("unused")
  private String[] lastErrorId;
  private boolean shouldError = true;

  private List<Boolean> errorPattern;

  @Override
  public String getName() {
    return name;
  }

  @SuppressWarnings("unused")
  public void setShouldError(boolean shouldError) {
    this.shouldError = shouldError;
  }

  @Override
  public Document[] processDocument(Document document) {
    if (shouldError) {
      boolean isErrorDoc  = errorPattern.get(count.getAndIncrement() % errorPattern.size());
      if (isErrorDoc) {
        if (lastErrorId != null) {
          lastErrorId[0] = document.getId();
        }
        throw new RuntimeException("document #" + this.count.get() + " Intentional error by " + getName() + " for " + document.getId());
      }
    }
    log.info(getName() + " saw " + document.getId());
    return new Document[]{document};
  }


  public static class Builder extends NamedBuilder<ErrorPatternTestProcessor> {

    ErrorPatternTestProcessor obj = new ErrorPatternTestProcessor();

    @Override
    public ErrorPatternTestProcessor.Builder named(String name) {
      getObj().name = name;
      return this;
    }

    public ErrorPatternTestProcessor.Builder withErrorReporter(String[] reporter) {
      if (reporter.length != 1) {
        throw new IllegalArgumentException("only one error can be held, please pass array of length 1");
      }
      getObj().lastErrorId = reporter;
      return this;
    }

    public ErrorPatternTestProcessor.Builder erroringFromStart(boolean doErrors) {
      getObj().shouldError = doErrors;
      return this;
    }

    public ErrorPatternTestProcessor.Builder withErrorPattern(List<Boolean> pat) {
      getObj().errorPattern = pat;
      return this;
    }

    @Override
    protected ErrorPatternTestProcessor getObj() {
      return obj;
    }

    private void setObj(ErrorPatternTestProcessor obj) {
      this.obj = obj;
    }

    public ErrorPatternTestProcessor build() {
      ErrorPatternTestProcessor object = getObj();
      setObj(new ErrorPatternTestProcessor());
      return object;
    }

  }
}
