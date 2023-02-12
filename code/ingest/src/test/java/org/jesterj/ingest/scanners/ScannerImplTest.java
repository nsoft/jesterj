package org.jesterj.ingest.scanners;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.model.Step;
import org.jesterj.ingest.model.impl.NamedBuilder;
import org.jesterj.ingest.model.impl.StepImpl;
import org.jesterj.ingest.processors.ErrorFourthTestProcessor;
import org.jesterj.ingest.processors.PauseEveryFiveTestProcessor;

import java.util.HashMap;

public class ScannerImplTest {
  @SuppressWarnings("unused")
  private static final Logger log = LogManager.getLogger();

  @SuppressWarnings("SameParameterValue")
  protected static void startErrors(Plan plan1, String errorStep) {
    Step error4 = plan1.findStep(errorStep);
    StepImpl error41 = (StepImpl) error4;
    ErrorFourthTestProcessor processor = (ErrorFourthTestProcessor) error41.getProcessor();
    processor.setShouldError(true);
  }

  @SuppressWarnings("SameParameterValue")
  protected static void stopErrors(Plan plan1, String errorStep) {
    Step error4 = plan1.findStep(errorStep);
    ((ErrorFourthTestProcessor)((StepImpl)error4).getProcessor()).setShouldError(false);
  }

  @SuppressWarnings("SameParameterValue")
  protected static void shortPauses(Plan plan1, String pauseStep) {
    //noinspection SpellCheckingInspection
    Step pauser = plan1.findStep(pauseStep);
    ((PauseEveryFiveTestProcessor)((StepImpl)pauser).getProcessor()).setMillis(5);
  }

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
            return "RECORDER";
          }

          @Override
          public Document[] processDocument(Document document) {
            scannedDocs.put(document.getId(), document);
            log.info("Recording {}", document.getId());
            return new Document[] {document};
          }

          @Override
          public boolean isPotent() {
            return true;
          }

          @Override
          public boolean isSafe() {
            return false;
          }

        };
      }
    };
  }
}
