package org.jesterj.example.shakespeare;

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
public class ShakespeareConfig implements PlanProvider {

  private static final String ACCESSED = "format_accessed_date";
  private static final String CREATED = "format_created_date";
  private static final String MODIFIED = "format_modified_date";
  private static final String SIZE_TO_INT = "size_to_int_step";
  private static final String TIKA = "tika_step";
  private static final String SHAKESPEARE = "Shakespeare_scanner";

  public Plan getPlan() {
    PlanImpl.Builder planBuilder = new PlanImpl.Builder();
    SimpleFileWatchScanner.Builder scanner = new SimpleFileWatchScanner.Builder();
    StepImpl.Builder formatCreated = new StepImpl.Builder();
    StepImpl.Builder formatModified = new StepImpl.Builder();
    StepImpl.Builder formatAccessed = new StepImpl.Builder();
    StepImpl.Builder renameFileszieToInteger = new StepImpl.Builder();
    StepImpl.Builder tikaBuilder = new StepImpl.Builder();
    StepImpl.Builder sendToSolrBuilder = new StepImpl.Builder();

    File testDocs = new File("data");

    scanner
        .named(SHAKESPEARE)
        .withRoot(testDocs)
        .scanFreqMS(100);
    formatCreated
        .named(CREATED)
        .withProcessor(
            new SimpleDateTimeReformatter.Builder()
                .named("format_created")
                .from("created")
                .into("created_dt")
        );
    formatModified
        .named(MODIFIED)
        .withProcessor(
            new SimpleDateTimeReformatter.Builder()
                .named("format_modified")
                .from("modified")
                .into("modified_dt")
        );
    formatAccessed
        .named(ACCESSED)
        .withProcessor(
            new SimpleDateTimeReformatter.Builder()
                .named("format_accessed")
                .from("accessed")
                .into("accessed_dt")
        );

    renameFileszieToInteger
        .named(SIZE_TO_INT)
        .withProcessor(
            new CopyField.Builder()
                .named("copy_size_to_int")
                .from("file_size")
                .into("file_size_i")
                .retainingOriginal(false)
        );
    tikaBuilder
        .named(TIKA)
        .routingBy(new DuplicateToAll.Builder()
            .named("duplicator"))
        .withProcessor(new TikaProcessor.Builder()
            .named("tika")
        );
    sendToSolrBuilder
        .named("solr_sender")
        .withProcessor(
            new SendToSolrCloudProcessor.Builder()
                .withZookeeper("localhost:9983")
                .usingCollection("jjtest")
                .placingTextContentIn("_text_")
                .withDocFieldsIn(".fields")
        );

    planBuilder
        .named("myPlan")
        .withIdField("id")
        .addStep(scanner)
        .addStep(formatCreated, SHAKESPEARE)
        .addStep(formatModified, CREATED)
        .addStep(formatAccessed, MODIFIED)
        .addStep(renameFileszieToInteger, ACCESSED)
        .addStep(tikaBuilder, SIZE_TO_INT);
    planBuilder.addStep(sendToSolrBuilder, TIKA);
    return planBuilder.build();

  }

}
