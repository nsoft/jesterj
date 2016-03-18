# Status

## Working:
 * Embedded Cassandra server
 * Log4j can log to embedded cassandra server for regular logs and FTI status reports
 
## TODO for 0.1:
 * Options to specify dirs for cassandra
 * (done, log4j config) Options to specify regular logs go to file also, or only to file (and locations of the file)
 * Simple filesystem scanner
 * Split field values step
 * Solr sender.
 * Execute a Plan that scans a filesystem, fixes up file sizes, and indexes the documents in solr!
 
Release 0.1 is intended to be the smallest functional unit. Plans and steps will need to be assembled 
in code etc and only run locally, only single node supported.

## TODO for 0.2
 * ensure nodes namespace their cassandra data dirs
 * JINI Registrar 
 * Register Node Service on JINI Registrar
 * Serialized format for a plan/steps.
 * JINI Service to accept serialized format
 * Do run the 0.1 via the control web-app.
 * Support for external Cassandra if desired.
 * Contributions from others welcome for 0.2 and beyond.
 

Release 0.2 is intended to be the minimum usable system.  
 
## TODO for 0.3
 * Solr FTI plugin jar
 * Execute a plan with data that causes an error, properly adjust statuses in cassandra FTI
 * Cassandra cluster formation (Success here solidifies Cassandra as a persistence solution)
 * Official API/process for user written steps.
 * Support for helper nodes that scale a step or several steps horizontally.
 * Ability to design a plan in web-app.
 * 80% test coverage
 * Actual web site, deploy as Github site.
 * Availability on maven central.
 
Release 0.3 is intended to be the first production release for use by folks who dare to tread the bleeding edge.
  
# Running

To run the ingest node use the following command line. 

java -jar build/libs/ingest-node.jar 

This will print usage info. This jar contains all dependencies, and thus can be copied to any machine and run
without any additional setup. It will create &lt;user_home_dir&gt;/.jj and place cassandra related files there.

Some of the startup spam can be reduced by adding -Done-jar.silent=true

https://sourceforge.net/p/one-jar/bugs/69/

You can omit the onejar property if you don't mind some additional spam.

# What is FTI?

FTI stands for Fault Tolerant Indexing. For our purposes this means that once a scanner is pointed at a document
source, it is guaranteed to eventually do one of the following things with every qualifying document:

 * Process the document and send it to solr. 
 * Log an error explaining why the document processing failed.
 
It will do this no matter how many nodes fail, or how many times Solr is rebooted