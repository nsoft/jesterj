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
 * Execute a Plan that scans a DB, splits a comma separated list to multi-values and sends it to solr!.
 * Execute a plan with data that causes an error, properly adjust statuses in cassandra FTI
 * Do the above via the control web-app.
 
# Running

To run the ingest node use the following command line. 

java -jar build/libs/ingest-node.jar 

This will print usage info. This jar contains all dependencies, and thus can be copied to any machine and run
without any additional setup. It will create &lt;user_home_dir&gt;/.jj and place cassandra related files there.

Some of the startup spam can be reduced by adding -Done-jar.silent=true

https://sourceforge.net/p/one-jar/bugs/69/

You can omit the onejar property if you don't mind some additional spam.