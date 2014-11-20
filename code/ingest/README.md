# Status

## Working:
 * Ebedded Cassandra server
 * Log4j can log to embedded cassandra server for regular logs and FTI status reports
 
## TODO for 0.1:
 * MANY things... but notably..
 * Options to specify dirs for cassandra
 * Options to specify regular logs go to file also, or only to file (and locations of the file)
 * JINI Registrar 
 * Register Node Service on JINI Registrar
 * Serialized format for a plan/steps.
 * JINI Service to accept serialized format
 * Simple DB scanner
 * Split field values step
 * Solr sender.
 * Execute a Plan that scans a DB, splits a comma separated list to multi-values and sends it to solr!
 
Release 0.1 is intended to be the smallest functional unit. 

## TODO for 0.2
 * Solr FTI plugin jar
 * Execute a plan with data that causes an error, properly adjust statuses in cassandra FTI
 * Do the above via the control web-app.
 * Installation process.
 * Support for external Cassandra if desired.
 * Contributions from others welcome for 0.2 and beyond.
 

Release 0.2 is intended to be the minimum usable system.  
 
## TODO for 0.3
 * Cassandra cluster formation (Success here solidifies Cassandra as a persistence solution)
 * Official API/process for user written steps.
 * Support for helper nodes that scale a step or several steps horizontally.
 * Ability to design a plan in web-app.
 * 80% test coverage
 * Actual web site, deploy as github site.
 * Availbility on maven central.
 
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
 
It will do this no matter how many nodes fail, or how many times Solr 