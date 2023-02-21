package org.jesterj.ingest.processors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.impl.NamedBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Processor for tests to use as a output step (destination)
 */
public class DocumentCounter implements DocumentProcessor {

  private static final Logger log = LogManager.getLogger();


  private final Map<String, DocCounted> scannedDocs = new HashMap<>();
  private String name;
  private boolean block;

  public DocumentCounter() {

  }
  @Override
  public String getName() {
    return name;
  }

  @Override
  public Document[] processDocument(Document document) {
    while(block) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        // ignore, would be shutting down.
      }
    }
    getScannedDocs().computeIfAbsent(document.getId(), (id) -> new DocCounted(document)).timesSeen.incrementAndGet();

    log.info("Recording {}", document.getId());
    return new Document[]{document};
  }

  @Override
  public boolean isPotent() {
    return true;
  }

  @Override
  public boolean isSafe() {
    return false;
  }

  public Map<String,DocCounted> getScannedDocs() {
    return scannedDocs;
  }

  public void setBlock(boolean block) {
    this.block = block;
  }

  public static class Builder extends NamedBuilder<DocumentCounter> {

    DocumentCounter obj = new DocumentCounter();

    @Override
    public Builder named(String name) {
      getObj().name = name;
      return this;
    }
    @Override
    public DocumentCounter build() {
      DocumentCounter result = getObj();
      obj = new DocumentCounter();
      return result;
    }

    @Override
    protected DocumentCounter getObj() {
      return obj;
    }
  }



  public static class DocCounted {
    public final Document document;
    public final AtomicInteger timesSeen = new AtomicInteger(0);

    public DocCounted(Document document) {
      this.document = document;
    }
  }
}
