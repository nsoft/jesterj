package org.jesterj.ingest.scanners;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.model.Step;
import org.jesterj.ingest.model.impl.StepImpl;
import org.jesterj.ingest.processors.DocumentCounter;
import org.jesterj.ingest.processors.DocumentFieldMatchCounter;
import org.jesterj.ingest.processors.ErrorFourthTestProcessor;
import org.jesterj.ingest.processors.PauseEveryFiveTestProcessor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

public class ScannerImplTest {
  @SuppressWarnings("unused")
  private static final Logger log = LogManager.getLogger();
  public static final Random RANDOM = new Random();

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
    DocumentCounter counter = findCounter(plan, stepName);
    return counter.getScannedDocs();
 }

  protected static int sizeForCounter(Plan plan, String name) {
    DocumentCounter counter = findCounter(plan, name);
    return counter.getScannedDocs().size();
  }

  public static DocumentCounter findCounter(Plan plan, String name) {
    Step counterStep = plan.findStep(name);
    StepImpl csImpl = (StepImpl) counterStep;
    DocumentProcessor processor = csImpl.getProcessor();
    return (DocumentCounter) processor;
  }

  public static File getUniqueTempDir() {

    Path base;
    try {
      base = Files.createTempDirectory("jj_");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    //
    // Copied from org.apache.lucene.tests.util.TestRuleTemporaryFilesCleanup.createTempDir
    //
    // Used under the terms of Apache Software License 2.0 ( http://www.apache.org/licenses/LICENSE-2.0)
    //
    // changed from the original to replace referenced Constants with hard coded values and to
    // use an additional random number in the file name
    //
    int attempt = 0;
    Path f;
    boolean success = false;
    do {
      if (attempt++ >= 50) {
        throw new RuntimeException(
            "Failed to get a temporary name too many times, check your temp directory and consider manually cleaning it: "
                + base.toAbsolutePath());
      }
      f = base.resolve( "jj_"+ Math.abs(RANDOM.nextLong()) + String.format(Locale.ENGLISH, "_%03d", attempt));
      try {
        Files.createDirectory(f);
        success = true;
      } catch (
          @SuppressWarnings("unused")
          IOException ignore) {
      }
    } while (!success);
    //
    // End ASF code
    //

    return f.toFile();
  }

  protected DocumentCounter.Builder getScannedDocRecorder(String name) {
    return new DocumentCounter.Builder().named(name);
  }

}
