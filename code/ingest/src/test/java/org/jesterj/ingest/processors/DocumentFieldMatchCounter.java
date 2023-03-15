package org.jesterj.ingest.processors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.*;
import org.jesterj.ingest.model.impl.NamedBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Processor for tests to use as a output step (destination)
 */
public class DocumentFieldMatchCounter implements DocumentProcessor {

  private static final Logger log = LogManager.getLogger();


  private final Map<String,Map<String, DocCounted>> scannedDocsByValue = new HashMap<>();
  private String name;

  private String fieldName;

  private boolean block;

  public DocumentFieldMatchCounter() {

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
    String firstValue = document.getFirstValue(fieldName);
    Map<String, DocCounted> valueMap = getScannedDocsByValue().computeIfAbsent(firstValue, (fValue) -> new HashMap<>());
    valueMap.computeIfAbsent(document.getId(), (id) -> new DocCounted(document)).timesSeen.incrementAndGet();
    log.info("Recording {} for value {}", document.getId(), firstValue);
    document.setStatus(Status.INDEXED,"Document counted for value of {}", firstValue);
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

  public Map<String, Map<String, DocCounted>> getScannedDocsByValue() {
    return scannedDocsByValue;
  }

  public void setBlock(boolean block) {
    this.block = block;
  }

  public static class Builder extends NamedBuilder<DocumentFieldMatchCounter> {

    DocumentFieldMatchCounter obj = new DocumentFieldMatchCounter();

    @Override
    public Builder named(String name) {
      getObj().name = name;
      return this;
    }
    @Override
    public DocumentFieldMatchCounter build() {
      DocumentFieldMatchCounter result = getObj();
      obj = new DocumentFieldMatchCounter();
      return result;
    }

    @Override
    protected DocumentFieldMatchCounter getObj() {
      return obj;
    }

    public ConfiguredBuildable<? extends DocumentProcessor> matchesForField(String field) {
       getObj().fieldName = field;
       return this;
    }
  }



  public static class DocCounted {
    public final Document document;
    public final AtomicInteger timesSeen = new AtomicInteger(0);

    public DocCounted(Document document) {
      this.document = document;
    }

    @Override
    public String toString() {
      return "DocCounted{" +
          "document=" + document +
          ", timesSeen=" + timesSeen +
          '}';
    }
  }
}
