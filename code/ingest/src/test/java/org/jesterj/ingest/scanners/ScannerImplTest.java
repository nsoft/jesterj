package org.jesterj.ingest.scanners;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.impl.NamedBuilder;

import java.util.HashMap;

public class ScannerImplTest {
  private static final Logger log = LogManager.getLogger();

  protected NamedBuilder<DocumentProcessor> getScannedDocRecorder(HashMap<String, Document> scannedDocs) {
    return new NamedBuilder<>() {

      @Override
      public NamedBuilder<DocumentProcessor> named(String name) {
        return null;
      }

      @Override
      public DocumentProcessor build() {
        return new DocumentProcessor() {
          @Override
          public String getName() {
            return null;
          }

          @Override
          public Document[] processDocument(Document document) {
            scannedDocs.put(document.getId(), document);
            log.info("Recording {}", document.getId());
            return new Document[] {document};
          }
        };
      }
    };
  }
}
