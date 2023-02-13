package org.jesterj.ingest.processors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.ConfiguredBuildable;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.impl.NamedBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Processor for tests to use as a potent step (destination)
 */
public class DocumentFieldMatchCounter implements DocumentProcessor {

  private static final Logger log = LogManager.getLogger();


  private final Map<String,Map<String, DocCounted>> scannedDocs = new HashMap<>();
  private String name;

  private String fieldName;

  public DocumentFieldMatchCounter() {

  }
  @Override
  public String getName() {
    return name;
  }

  @Override
  public Document[] processDocument(Document document) {
    Map<String, DocCounted> valueMap = getScannedDocs().computeIfAbsent(document.getFirstValue(fieldName), (fValue) -> new HashMap<>());
    valueMap.computeIfAbsent(document.getId(), (id) -> new DocCounted(document)).timesSeen.incrementAndGet();
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

  public Map<String, Map<String, DocCounted>> getScannedDocs() {
    return scannedDocs;
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
  }
}
