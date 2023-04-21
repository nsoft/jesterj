package org.jseterj.example.wikidocs;

import org.jesterj.ingest.JavaPlanConfig;
import org.jesterj.ingest.PlanProvider;
import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.model.impl.PlanImpl;
import org.jesterj.ingest.model.impl.StepImpl;
import org.jesterj.ingest.processors.CopyField;
import org.jesterj.ingest.processors.SendToSolrCloudProcessor;
import org.jesterj.ingest.processors.SimpleDateTimeReformatter;
import org.jesterj.ingest.routers.RoundRobinRouter;
import org.jesterj.ingest.scanners.SimpleFileScanner;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings("unused")
@JavaPlanConfig
public class WikidocsConfig implements PlanProvider {

  // First declare some constants for legibility and convenience (none required)
  private static final String ACCESSED = "format_accessed_date";
  private static final String CREATED = "format_created_date";
  private static final String MODIFIED = "format_modified_date";
  private static final String SIZE_TO_INT = "size_to_int_step";
  private static final String WIKIDOCS_SCANNER = "Wikidocs_scanner";

  public static final String WIKI_DOC_PARSER = "WikidocParser";
  private final AtomicLong idSerial = new AtomicLong(0);

  public Plan getPlan() {

    // Second, create all your builder objects. I find these to just be clutter when
    // reading the configuration portions so I just do them all up front at the top.
    // Of course you are free to declare these along with configs if you prefer.
    PlanImpl.Builder planBuilder = new PlanImpl.Builder();
    SimpleFileScanner.Builder scanner = new SimpleFileScanner.Builder();
    StepImpl.Builder formatCreated = new StepImpl.Builder();
    StepImpl.Builder formatModified = new StepImpl.Builder();
    StepImpl.Builder formatAccessed = new StepImpl.Builder();
    StepImpl.Builder renameFileSizeToInteger = new StepImpl.Builder();
    StepImpl.Builder wikidocParser = new StepImpl.Builder();
    StepImpl.Builder sendToSolrBuilder = new StepImpl.Builder();


    // This shold point to the data directory created on first run of https://github.com/mikemccand/luceneutil
    // and the .txt.lzma file should be unzipped
    File testDocs = new File("/home/gus/projects/apache/solr/perf/data");

    //
    // This is the first important part. Each step needs to be configured. Order is NOT important, but I find it
    // very helpful to write this in the approximate order that documents will flow through the steps.
    //

    // Our initial source of documents, note the use of testDocs defined above to keep this legible
    scanner
        .named(WIKIDOCS_SCANNER) // everything should be given a unique name composed of alphanumerics and underscores only.
        .withRoot(testDocs)
        .acceptOnly(pathname -> pathname.getName().endsWith(".txt"))
        .docPerLineIfMatches(pathname -> true)
        .rememberScannedIds(true)
        .detectChangesViaHashing(true)
        .scanFreqMS(1000);

    // format the several dates produced by the scanner (to the default ISO output, solr wants)
    formatCreated
        .named(CREATED)
        .batchSize(1000)
        .withProcessor(
            new SimpleDateTimeReformatter.Builder()
                .named("format_created")
                .from("created")
                .into("created_dt")
        );
    formatModified
        .named(MODIFIED)
        .batchSize(1000)
        .withProcessor(
            new SimpleDateTimeReformatter.Builder()
                .named("format_modified")
                .from("modified")
                .into("modified_dt")
        );
    formatAccessed
        .named(ACCESSED)
        .batchSize(1000)
        .withProcessor(
            new SimpleDateTimeReformatter.Builder()
                .named("format_accessed")
                .from("accessed")
                .into("accessed_dt")
        );

    // the _default schema used by this example interprest fields ending in _i as integers, so fix the field name
    renameFileSizeToInteger
        .named(SIZE_TO_INT)
        .batchSize(1000)
        .withProcessor(
            new CopyField.Builder()
                .named("copy_size_to_int")
                .from("file_size")
                .into("file_size_l")
                .retainingOriginal(false)
        );

    // This lets Tika process the documents we scanned. This is more for example than functionality since the documents
    // are already plain text. A more sophisticated solution would add a custom processor here to identify speakers
    // based on the formatting and could make it possible to search for matches in lines spoken by Rozencrantz or
    // Guildenstern or whatever. The sky is the limit if you can parse it and get the information into the document
    // such that solr can understand it.
    wikidocParser
        .named(WIKI_DOC_PARSER)
        .batchSize(1000)

//      If you wanted to send the result to more than one step (e.g. to two different collections) you would add a
//      Router with something like the commented code below.

        .routingBy((new RoundRobinRouter.Builder()).named("RoundRobinToSolr"))

        .withProcessor(new ParseWikidoc.Builder()
            .named("WikidocParserProcessor")
        );



      sendToSolrBuilder
          .named("solr_sender")
          .batchSize(1000)
          .withProcessor(
              new SendToSolrCloudProcessor.Builder()
                  .named("solr_processor")
                  .withZookeeper("localhost:2181")
                  .zkChroot("/solr__home_gus_projects_gus-asf_solr_testing_wikidoc2")
                  .usingCollection("solr-perf")
                  .transformIdsWith(id -> idSerial.incrementAndGet()) // Note: ONLY for parity with lucene-util. Normally a terrible idea with real data
                  .sendingBatchesOf(5000)
                  .sendingPartialBatchesAfterMs(20_000)

          );



    //
    // Important part #2: Building the Directed Acyclic Graph
    //

    // This example is linear, but see hints below for quick how to on branching and joining document flows
    planBuilder
        .named("myPlan")
        .withIdField("id")
        .addStep(scanner)

        // arguments: Builder for step, NAME of any predecessor steps. THIS is why all steps must be
        // named and all names must be unique. You will get an error if you have duplicate names.
        .addStep(formatCreated, WIKIDOCS_SCANNER)
        .addStep(formatModified, CREATED)
        .addStep(formatAccessed, MODIFIED)
        .addStep(renameFileSizeToInteger, ACCESSED)
        .addStep(wikidocParser, SIZE_TO_INT);
    planBuilder.addStep(sendToSolrBuilder, WIKI_DOC_PARSER);

    return planBuilder.build();

  }

  // Branching Hint:
  //
  // 1) add a router to the step before the branch point
  // 2) List the step with the router as the precedessor to more than one step.
  //
  // For example, you might configure 3 destinations like this:
  //
  //        .addStep(tikaBuilder, SIZE_TO_INT)
  //        .addStep(sendToSolrBuilderFoo, TIKA);
  //        .addStep(sendToSolrBuilderBar, TIKA);
  //        .addStep(sendToSolrBuilderBaz, TIKA);

  // Joining Hint:
  //
  // Simply list more than one predecessor, addStep accepts a variable number of step names.
  //
  //        .addStep(niftyProcessing, FILE_SCANNER, JDBC_SCANNER, SOME_OTHER_STEP_DOESNT_NEED_TO_BE_A_SCANNER);

}
