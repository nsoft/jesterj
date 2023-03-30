package org.jesterj.ingest.scanners;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.opencsv.CSVWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.model.Step;
import org.jesterj.ingest.model.impl.StepImpl;
import org.jesterj.ingest.persistence.CassandraSupport;
import org.jesterj.ingest.processors.DocumentCounter;
import org.jesterj.ingest.processors.DocumentFieldMatchCounter;
import org.jesterj.ingest.processors.ErrorFourthTestProcessor;
import org.jesterj.ingest.processors.PauseEveryFiveTestProcessor;
import org.jesterj.ingest.utils.ColDefs;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

import static org.jesterj.ingest.utils.ColDefs.notStupid;

public class ScannerImplTest {
  @SuppressWarnings("unused")
  private static final Logger log = LogManager.getLogger();
  public static final Random RANDOM = new Random();
  public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss").withZone(ZoneId.of("UTC"));

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

  protected static int getDocCount(Plan plan, String stepName) {
   return ScannerImplTest.getScannedDocs(plan, stepName).size();
 }

  protected static Map<String, DocumentCounter.DocCounted> getScannedDocs(Plan plan, String stepName) {
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

  public static DocumentFieldMatchCounter findFieldMatchCounter(Plan plan, String name) {
    Step counterStep = plan.findStep(name);
    StepImpl csImpl = (StepImpl) counterStep;
    DocumentProcessor processor = csImpl.getProcessor();
    return (DocumentFieldMatchCounter) processor;
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

  protected static void showTable(CassandraSupport support, String tableName) {
    ResultSet showMe = support.getSession().execute("select * from " + tableName);
    int rowNum = 1;
    for (Row row : showMe) {
      System.out.println(rowNum++ + ") " + row.getFormattedContents());
    }
  }

  protected DocumentCounter.Builder getScannedDocRecorder(String name) {
    return new DocumentCounter.Builder().named(name);
  }

    protected void dumpTables(CassandraSupport support) {
      ResultSet tables = support.getSession().execute("SELECT table_name, keyspace_name from system_schema.tables");
      Instant startInstant = Instant.now();
      for (Row table : tables) {
        String keyspaceName = table.getString("keyspace_name");
        if (keyspaceName == null || !keyspaceName.startsWith("jj_")) {
          continue;
        }
        String tableName = table.getString("table_name");
        String keyspaceAndTable = keyspaceName + "." + tableName;
        ResultSet showMe = support.getSession().execute("select * from " + keyspaceAndTable);
        String stepName = "none";
        int rowNum = 1;
        StringWriter stringWriter = new StringWriter();
        try (CSVWriter writer = new CSVWriter(stringWriter)) {
          ColDefs cds = null;
          for (Row row : showMe) {
            if (rowNum++ == 1) {
              cds = notStupid(row.getColumnDefinitions());
              writer.writeNext(cds.getNames());
            }
            if (rowNum == 2) {
              try {
                stepName = row.getString("outputstepname") + "_";
              } catch (IllegalArgumentException e) {
                stepName = "hashes_";
              }
            }
            writer.writeNext(cds.getValues(row));
          }
          System.out.println(new File(".").getCanonicalFile());
          String dateDir = DATE_TIME_FORMATTER.format(startInstant);
          File file = new File("./build/tableDumps/" + dateDir + "/"+ getClass().getSimpleName() +
              "/" + stepName + "_" + keyspaceAndTable + ".csv");
          //noinspection ResultOfMethodCallIgnored
          file.getParentFile().mkdirs();
          Files.writeString(file.toPath(), stringWriter.toString());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
}
