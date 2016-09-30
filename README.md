JesterJ [![License](https://img.shields.io/badge/license-Apache%202.0-B70E23.svg?style=plastic)](http://www.opensource.org/licenses/Apache-2.0) [![Build Status](https://travis-ci.org/nsoft/jesterj.svg?branch=master)](https://travis-ci.org/nsoft/jesterj)
=======

A new highly flexible, highly scaleable document ingestion system. 

See the [web site](http://www.jesterj.org) and the [documentation](https://github.com/nsoft/jesterj/wiki/Documentation) for more info

# Status

Current release version: 0.1.

## Features:

In this release we have the following features

 * Embedded Cassandra server
 * Cassandra config and data location configurable, defaults to ~/.jj/cassandra
 * Initial support for fault tolerance via logging statuses to the embedded cassandra server (WIP)
 * Log4j appender to write to Cassandra where desired
 * Initial API/process for user written steps. (see [documentation](https://github.com/nsoft/jesterj/wiki/Documentation))
 * 40% test coverage (jacoco)
 * Simple filesystem scanner
 * Copy Field processor
 * Date Reformat processor
 * Human Readable File Size processor 
 * Tika processor to extract content
 * Solr sender to send documents to solr in batches.
 * Runnable example to [execute a plan](https://github.com/nsoft/jesterj/blob/master/code/ingest/README.md) that scans a filesystem, and indexes the documents in solr.

Release 0.1 is intended to be the smallest functional unit. Plans and steps will need to be assembled 
in code etc and only run locally, only single node supported. Documents indexed will have fields for mod-time, 
file name and file size.

## Progress for 0.2
 * JDBC scanner
 * Node and Transport style senders for Elastic
 * Ability to load Java based config from a jar file - experimental. 
 * More processors: Fetch URL, Regex Replace Value, Delete Field, Parse Field as Template, URL Encode Field
 
The Java config feature is experimental. I wanted to use what I had built for a project but the lack of externalized configuration was a blocker. It was a quick fix but it's turnning out to be quite pleasant to work with. The down side is I'm not sure how it would carry forward to later stages of the project so it might still go away. Feedback welcome.

## TODO for 0.2
 * Serialized format for a plan/steps.
 * Cassandra stored hashcode based file scanner
 * Xpath extractor
 * JsonPath extractor
 * Cassandra based FTI
 * Source DB based FTI
 * Solr FTI plugin jar to mark documents searchable on commit
 * Execute a plan with data that causes an error, properly adjust statuses in cassandra FTI
 * Support for external Cassandra if desired.
 * 60% [test coverage](https://codecov.io/gh/nsoft/jesterj) 
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
