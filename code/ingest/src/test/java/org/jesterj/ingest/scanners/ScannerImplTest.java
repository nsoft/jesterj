package org.jesterj.ingest.scanners;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.model.Step;
import org.jesterj.ingest.model.impl.StepImpl;
import org.jesterj.ingest.processors.DocumentCounter;
import org.jesterj.ingest.processors.DocumentFieldMatchCounter;
import org.jesterj.ingest.processors.ErrorFourthTestProcessor;
import org.jesterj.ingest.processors.PauseEveryFiveTestProcessor;

import java.util.Map;

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
  @SuppressWarnings({"SameParameterValue", "unused"})
  protected static void longPauses(Plan plan1, String pauseStep, int millis) {
    //noinspection SpellCheckingInspection
    Step pauser = plan1.findStep(pauseStep);
    ((PauseEveryFiveTestProcessor)((StepImpl)pauser).getProcessor()).setMillis(millis);
  }

  protected static void blockCounters(Plan plan, boolean block) {
    for (Step step : plan.getSteps()) {
      StepImpl si = (StepImpl) step;
      if (si.getProcessor() instanceof DocumentCounter) {
        ((DocumentCounter)si.getProcessor()).setBlock(block);
      }
      if (si.getProcessor() instanceof DocumentFieldMatchCounter) {
        ((DocumentFieldMatchCounter)si.getProcessor()).setBlock(block);
      }
    }
  }

  static int getDocCount(Plan plan, String stepName) {
   return ScannerImplTest.getScannedDocs(plan, stepName).size();
 }

  static Map<String, DocumentCounter.DocCounted> getScannedDocs(Plan plan, String stepName) {
   StepImpl test2 = (StepImpl) plan.findStep(stepName);
   return ((DocumentCounter) test2.getProcessor()).getScannedDocs();
 }

  protected DocumentCounter.Builder getScannedDocRecorder(String name) {
    return new DocumentCounter.Builder().named(name);
  }

}
