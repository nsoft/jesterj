package org.jesterj.example.shakespeare;

import org.apache.logging.log4j.Level;
import org.jesterj.ingest.JavaPlanConfig;
import org.jesterj.ingest.PlanProvider;
import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.model.impl.PlanImpl;
import org.jesterj.ingest.model.impl.StepImpl;
import org.jesterj.ingest.processors.*;
import org.jesterj.ingest.routers.DuplicateToAll;
import org.jesterj.ingest.scanners.SimpleFileWatchScanner;

import java.io.File;

@JavaPlanConfig
public class FailWithExceptionConfig implements PlanProvider {

  private static final String SHAKESPEARE = "Shakespeare_scanner";

  public Plan getPlan() {
    PlanImpl.Builder planBuilder = new PlanImpl.Builder();
    SimpleFileWatchScanner.Builder scanner = new SimpleFileWatchScanner.Builder();
    StepImpl.Builder logAndFail = new StepImpl.Builder();

    File testDocs = new File("data");

    scanner
        .named(SHAKESPEARE)
        .withRoot(testDocs)
        .scanFreqMS(100);
    logAndFail
        .named("logAndFailStep")
        .withProcessor(new LogAndFail.Builder()
            .named("logAndFailProcessor")
            .withLogLevel(Level.ERROR)
            .after(5)
            .throwing(new RuntimeException("BOOM")));


    planBuilder
        .named("myPlan")
        .withIdField("id")
        .addStep(scanner)
        .addStep(logAndFail, SHAKESPEARE)
    ;
    return planBuilder.build();

  }

}
