JesterJ
=======

A new highly flexible, highly scaleable document ingestion system. 

See the [web site](http://www.jesterj.org) for more info

# Status

## Working:
 * Embedded Cassandra server
 * Log4j can log to embedded cassandra server for regular logs and FTI status reports
 * Options to specify regular logs go to file also, or only to file (and locations of the file) -> log4j config
 * Actual web site, deploy as Github site.
 * Official API/process for user written steps. (just implement DocumentProcessor)
 * Simple filesystem scanner
 * Copy Field processor
 * Date Reformat processor
 * Tika processor to extract content
 * Solr sender.
 * [Execute a Plan](https://github.com/nsoft/jesterj/blob/master/code/ingest/README.md) that scans a filesystem, and indexes the documents in solr!

## TODO for 0.1:
 * Options to specify dirs for cassandra
 * Add Field Processor
 * Field value Number Format Processor
 * 40% test coverage (jacoco)
 * Fix up filesizes and provide slightly better metadata for demo execution
 
Release 0.1 is intended to be the smallest functional unit. Plans and steps will need to be assembled 
in code etc and only run locally, only single node supported. Documents indexed will have fields for mod-time, 
file name and file size.

## TODO for 0.2
 * Serialized format for a plan/steps.
 * Cassandra stored hashcode based file scanner
 * Xpath extractor
 * JsonPath extractor
 * Database table scanner
 * Cassandra based FTI
 * Source DB based FTI
 * Solr FTI plugin jar to mark documents searchable on commit
 * Execute a plan with data that causes an error, properly adjust statuses in cassandra FTI
 * Support for external Cassandra if desired.
 * 60% test coverage 
 * Index a database and a filesystem simultaneously into solr
 

Release 0.2 is intended to be the minimum usable single node system.  
 
## TODO for 0.3
 * JINI Registrar 
 * Register Node Service on JINI Registrar
 * Display nodes visible in control web app.
 * JINI Service to accept serialized format
 * Ability to build a plan in web-app.
 * 80% test coverage (maintain going forward)
 * Availability on maven central.
 * Build and run the 0.2 scenario via the control web-app.
 
Release 0.3 is intended to be similar to 0.2 but with a very basic web control UI. At this point it should be
possible to install the war file, start a node, 

## TODO for 1.0
 * secure connections among nodes and with the web app. (credential provider)
 * Ensure nodes namespace their cassandra data dirs to avoid disasters if more than one node run per user account
 * Cassandra cluster formation 
 * pass Documents among nodes using Java Spaces
 * Support for adding helper nodes that scale a step or several steps horizontally.
 * Make the control UI pretty.

Release 1.0 is intended to be the first release to spread work across nodes. 

# What is FTI?

FTI stands for Fault Tolerant Indexing. For our purposes this means that once a scanner is pointed at a document
source, it is guaranteed to eventually do one of the following things with every qualifying document:

 * Process the document and send it to solr. 
 * Log an error explaining why the document processing failed.
 
It will do this no matter how many nodes fail, or how many times Solr is rebooted  
