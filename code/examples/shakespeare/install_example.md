The following is a log of an example of installing and running the shakespeare example on and ec2 server. 

```

       __|  __|_  )
       _|  (     /   Amazon Linux AMI
      ___|\___|___|

https://aws.amazon.com/amazon-linux-ami/2017.03-release-notes/
[ec2-user@ip-172-30-3-55 ~]$ cd /opt
[ec2-user@ip-172-30-3-55 opt]$ cd /opt
[ec2-user@ip-172-30-3-55 opt]$ sudo mkdir jesterj
[ec2-user@ip-172-30-3-55 opt]$ sudo chown ec2-user jesterj
[ec2-user@ip-172-30-3-55 opt]$ ls -al
total 16
drwxr-xr-x  4 root     root 4096 Apr 22 14:17 .
dr-xr-xr-x 25 root     root 4096 Apr 22 14:17 ..
drwxr-xr-x  5 root     root 4096 Apr 17 07:58 aws
drwxr-xr-x  2 ec2-user root 4096 Apr 22 14:17 jesterj
[ec2-user@ip-172-30-3-55 opt]$ sudo !!
sudo wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u121-b13/e9e7ea248e2c4826b92b3f075a80e441/jdk-8u121-linux-x64.tar.gz"
--2017-04-22 19:25:19--  http://download.oracle.com/otn-pub/java/jdk/8u121-b13/e9e7ea248e2c4826b92b3f075a80e441/jdk-8u121-linux-x64.tar.gz?AuthParam=1492889239_e6808a6901f1fb9921f22e15587551bc
Connecting to download.oracle.com (download.oracle.com)|104.96.220.152|:80... connected.
HTTP request sent, awaiting response... 200 OK
Length: 183246769 (175M) [application/x-gzip]
Saving to: ‘jdk-8u121-linux-x64.tar.gz’

jdk-8u121-linux-x64.tar.gz                                                                 100%[======================================================================================================================================================================================================================================>] 174.76M  37.1MB/s    in 4.9s    

2017-04-22 19:25:24 (35.8 MB/s) - ‘jdk-8u121-linux-x64.tar.gz’ saved [183246769/183246769]
[ec2-user@ip-172-30-3-55 opt]$ sudo tar xzvf jdk-8u121-linux-x64.tar.gz 
jdk1.8.0_121/
jdk1.8.0_121/THIRDPARTYLICENSEREADME-JAVAFX.txt
jdk1.8.0_121/THIRDPARTYLICENSEREADME.txt
jdk1.8.0_121/lib/
jdk1.8.0_121/lib/jexec
jdk1.8.0_121/lib/javafx-mx.jar
jdk1.8.0_121/lib/packager.jar
...

-----------------------------------------------------------------

Patricks-MBP:~ gus$ cd projects/jesterj/code/jesterj/code/ingest/
Patricks-MBP:ingest gus$ ./gradlew clean build
:clean
:compileJava
:processResources
:classes
:jar
:startScripts
:distTar
:distZip
:assemble
:compileTestJava
:processTestResources
:testClasses
:test
:check
:build

BUILD SUCCESSFUL

Total time: 19.658 secs
Patricks-MBP:ingest gus$ ./gradlew onejar
:clean
:compileJava
Note: Some input files use unchecked or unsafe operations.
Note: Recompile with -Xlint:unchecked for details.
:processResources
:classes
:jar
:oneJar
Manifest.writeTo(Writer) has been deprecated and is scheduled to be removed in Gradle 4.0. Please use Manifest.writeTo(Object) instead.

BUILD SUCCESSFUL

Total time: 6.425 secs
Patricks-MBP:ingest gus$ scp -i ~/.ssh/JJ_org.pem build/libs/ingest-node.jar ec2-user@34.192.252.251:/opt/jesterj
ingest-node.jar  
Patricks-MBP:ingest gus$ pushd ../examples/shakespeare/
Patricks-MBP:shakespeare gus$ ./gradlew clean build
:clean
:compileJava
:processResources
:classes
:jar
:assemble
:compileTestJava UP-TO-DATE
:processTestResources UP-TO-DATE
:testClasses UP-TO-DATE
:test UP-TO-DATE
:check UP-TO-DATE
:build

BUILD SUCCESSFUL

Total time: 1.709 secs
Patricks-MBP:shakespeare gus$ 
Patricks-MBP:shakespeare gus$ scp -i ~/.ssh/JJ_org.pem build/libs/example-shakespeare-0.2-SNAPSHOT.jar ec2-user@34.192.252.251:/opt/jesterj
example-shakespeare-0.2-SNAPSHOT.jar                                                                                             100% 2083KB   2.0MB/s   00:01  
Patricks-MBP:shakespeare gus$ scp -r -i ~/.ssh/JJ_org.pem src/main/resources/data ec2-user@34.192.252.251:/opt/jesterj
allswellthatendswell                                                                                                             100%  132KB 362.8KB/s   00:00    
asyoulikeit                                                                                                                      100%  122KB 658.8KB/s   00:00    
comedyoferrors                                                                                                                   100%   87KB 614.6KB/s   00:00    
cymbeline                                                                                                                        100%  161KB 980.4KB/s   00:00    
loveslabourslost                                                                                                                 100%  127KB 402.9KB/s   00:00    
measureforemeasure                                                                                                               100%  127KB 600.8KB/s   00:00    
merchantofvenice                                                                                                                 100%  120KB   1.0MB/s   00:00    
merrywivesofwindsor                                                                                                              100%  128KB   1.1MB/s   00:00    
midsummersnightsdream                                                                                                            100%   94KB 983.1KB/s   00:00    
muchadoaboutnothing                                                                                                              100%  121KB   1.0MB/s   00:00    
periclesprinceoftyre                                                                                                             100%  109KB   1.0MB/s   00:00    
tamingoftheshrew                                                                                                                 100%  121KB   1.0MB/s   00:00    
tempest                                                                                                                          100%   97KB   1.3MB/s   00:00    
troilusandcressida                                                                                                               100%  155KB   1.4MB/s   00:00    
twelfthnight                                                                                                                     100%  114KB   1.4MB/s   00:00    
twogentlemenofverona                                                                                                             100%  100KB   1.3MB/s   00:00    
winterstale                                                                                                                      100%  142KB   1.4MB/s   00:00    
glossary                                                                                                                         100%   58KB   1.1MB/s   00:00    
1kinghenryiv                                                                                                                     100%  142KB   1.4MB/s   00:00    
1kinghenryvi                                                                                                                     100%  131KB   1.4MB/s   00:00    
2kinghenryiv                                                                                                                     100%  154KB   1.4MB/s   00:00    
2kinghenryvi                                                                                                                     100%  149KB   1.5MB/s   00:00    
3kinghenryvi                                                                                                                     100%  145KB   1.4MB/s   00:00    
kinghenryv                                                                                                                       100%  152KB   1.5MB/s   00:00    
kinghenryviii                                                                                                                    100%  145KB   1.5MB/s   00:00    
kingjohn                                                                                                                         100%  120KB   1.4MB/s   00:00    
kingrichardii                                                                                                                    100%  132KB   1.4MB/s   00:00    
kingrichardiii                                                                                                                   100%  176KB   1.5MB/s   00:00    
loverscomplaint                                                                                                                  100%   14KB 404.3KB/s   00:00    
rapeoflucrece                                                                                                                    100%   83KB   1.2MB/s   00:00    
sonnets                                                                                                                          100%   93KB   1.3MB/s   00:00    
various                                                                                                                          100%   19KB 538.3KB/s   00:00    
venusandadonis                                                                                                                   100%   53KB   1.1MB/s   00:00    
README                                                                                                                           100%  971    37.3KB/s   00:00    
antonyandcleopatra                                                                                                               100%  155KB   1.5MB/s   00:00    
coriolanus                                                                                                                       100%  164KB   1.5MB/s   00:00    
hamlet                                                                                                                           100%  178KB   1.5MB/s   00:00    
juliuscaesar                                                                                                                     100%  115KB   1.4MB/s   00:00    
kinglear                                                                                                                         100%  154KB   1.5MB/s   00:00    
macbeth                                                                                                                          100%  103KB   1.3MB/s   00:00    
othello                                                                                                                          100%  153KB   1.5MB/s   00:00    
romeoandjuliet                                                                                                                   100%  141KB   1.5MB/s   00:00    
timonofathens                                                                                                                    100%  111KB   1.4MB/s   00:00    
titusandronicus                                                                                                                  100%  121KB   1.4MB/s   00:00
   
--------------------------------------------------------------

[ec2-user@ip-172-30-3-55 jesterj]$ /opt/jdk1.8.0_121/bin/java -Djj.javaConfig=example-shakespeare-0.2-SNAPSHOT.jar -jar ingest-node.jar foo bar
logs will be written to: /home/ec2-user/.jj/logs

Received arguments:
   --cassandra-home:null
   <id>:foo
   <secret>:bar
Booting internal cassandra
2017-04-22T19:38:53,604 INFO Thread-0 com.datastax.driver.core.policies.DCAwareRoundRobinPolicy Using data-center name 'datacenter1' for DCAwareRoundRobinPolicy (if this is incorrect, please provide the correct datacenter name with DCAwareRoundRobinPolicy constructor)
2017-04-22T19:38:53,605 INFO Cassandra Java Driver worker-0 com.datastax.driver.core.Cluster New Cassandra host /0:0:0:0:0:0:0:1%lo:9042 added
Cassandra booted
2017-04-22T19:38:53,887 INFO Thread-0 org.jesterj.ingest.Main Looking for configuration class in example-shakespeare-0.2-SNAPSHOT.jar
2017-04-22T19:38:54,676 INFO Thread-0 org.reflections.Reflections Reflections took 778 ms to scan 12 urls, producing 1328 keys and 7654 values 
2017-04-22T19:38:54,679 INFO Thread-0 org.jesterj.ingest.Main Found the following @JavaPlanConfig classes (first in list will be used):[class org.jesterj.example.shakespeare.ShakespeareConfig]
2017-04-22T19:38:55,581 WARN Cassandra Java Driver worker-1 com.datastax.driver.core.Cluster Re-preparing already prepared query SELECT status, md5hash from jj_logging.fault_tolerant where docid = ? and scanner = ? ALLOW FILTERING. Please note that preparing the same query more than once is generally an anti-pattern and will likely affect performance. Consider preparing the statement only once.
2017-04-22T19:38:55,582 WARN Cassandra Java Driver worker-0 com.datastax.driver.core.Cluster Re-preparing already prepared query SELECT docid, scanner FROM jj_logging.fault_tolerant WHERE status = 'PROCESSING' and scanner = ? ALLOW FILTERING. Please note that preparing the same query more than once is generally an anti-pattern and will likely affect performance. Consider preparing the statement only once.
2017-04-22T19:38:55,584 WARN Cassandra Java Driver worker-1 com.datastax.driver.core.Cluster Re-preparing already prepared query SELECT docid, scanner FROM jj_logging.fault_tolerant WHERE status = 'ERROR' and scanner = ? ALLOW FILTERING. Please note that preparing the same query more than once is generally an anti-pattern and will likely affect performance. Consider preparing the statement only once.
2017-04-22T19:38:55,585 WARN Cassandra Java Driver worker-0 com.datastax.driver.core.Cluster Re-preparing already prepared query SELECT docid, scanner FROM jj_logging.fault_tolerant WHERE status = 'BATCHED' and scanner = ? ALLOW FILTERING. Please note that preparing the same query more than once is generally an anti-pattern and will likely affect performance. Consider preparing the statement only once.
2017-04-22T19:38:55,588 WARN Cassandra Java Driver worker-1 com.datastax.driver.core.Cluster Re-preparing already prepared query UPDATE jj_logging.fault_tolerant SET status = 'DIRTY'   where docid = ? and scanner = ? . Please note that preparing the same query more than once is generally an anti-pattern and will likely affect performance. Consider preparing the statement only once.
2017-04-22T19:38:55,589 WARN Cassandra Java Driver worker-0 com.datastax.driver.core.Cluster Re-preparing already prepared query UPDATE jj_logging.fault_tolerant set md5hash = ?  where docid = ? and scanner = ? . Please note that preparing the same query more than once is generally an anti-pattern and will likely affect performance. Consider preparing the statement only once.
2017-04-22T19:38:55,589 INFO Thread-0 org.jesterj.ingest.Main Activating Plan: myPlan
2017-04-22T19:38:55,590 INFO Thread-0 org.jesterj.ingest.model.impl.StepImpl Starting solr sender 
2017-04-22T19:38:55,590 INFO Thread-0 org.jesterj.ingest.model.impl.StepImpl Starting new thread for solr sender 
2017-04-22T19:38:55,593 INFO Thread-0 org.jesterj.ingest.model.impl.StepImpl Starting tika_step 
2017-04-22T19:38:55,593 INFO Thread-0 org.jesterj.ingest.model.impl.StepImpl Starting new thread for tika_step 
2017-04-22T19:38:55,594 INFO Thread-0 org.jesterj.ingest.model.impl.StepImpl Starting size_to_int_step 
2017-04-22T19:38:55,594 INFO Thread-0 org.jesterj.ingest.model.impl.StepImpl Starting new thread for size_to_int_step 
2017-04-22T19:38:55,594 INFO Thread-0 org.jesterj.ingest.model.impl.StepImpl Starting format_accessed_date 
2017-04-22T19:38:55,594 INFO Thread-0 org.jesterj.ingest.model.impl.StepImpl Starting new thread for format_accessed_date 
2017-04-22T19:38:55,596 INFO Thread-0 org.jesterj.ingest.model.impl.StepImpl Starting format_modified_date 
2017-04-22T19:38:55,596 INFO Thread-0 org.jesterj.ingest.model.impl.StepImpl Starting new thread for format_modified_date 
2017-04-22T19:38:55,596 INFO Thread-0 org.jesterj.ingest.model.impl.StepImpl Starting format_created_date 
2017-04-22T19:38:55,596 INFO Thread-0 org.jesterj.ingest.model.impl.StepImpl Starting new thread for format_created_date 
2017-04-22T19:38:55,596 INFO Thread-0 org.jesterj.ingest.model.impl.StepImpl Starting Shakespeare_scanner 
2017-04-22T19:38:55,596 INFO Thread-0 org.jesterj.ingest.model.impl.StepImpl Starting new thread for Shakespeare_scanner 
.2017-04-22T19:38:55,609 INFO pool-15-thread-1 org.jesterj.ingest.model.impl.DocumentImpl 
2017-04-22T19:38:55,620 INFO pool-15-thread-1 org.jesterj.ingest.model.impl.StepImpl Shakespeare_scanner finished processing file:///opt/jesterj/data/comedies/comedyoferrors
2017-04-22T19:38:55,625 INFO pool-15-thread-1 org.jesterj.ingest.model.impl.DocumentImpl 
2017-04-22T19:38:55,628 INFO pool-15-thread-1 org.jesterj.ingest.model.impl.StepImpl Shakespeare_scanner finished processing file:///opt/jesterj/data/comedies/midsummersnightsdream

```