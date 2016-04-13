
# Running

To run the ingest node use the following command line. 

java -jar build/libs/ingest-node.jar 

This will print usage info. This jar contains all dependencies, and thus can be copied to any machine and run
without any additional setup. It will create &lt;user_home_dir&gt;/.jj and place cassandra related files there.

# Watch It Go

If you set up a solr cloud with zookeeper at 9983 on localhost and a collection named jjtest that is configured 
similarly to the dynamic schema example that comes with solr, the following command will index the complete works 
of William Shakespeare into your index:

    java -Djj.example=solr -jar ingest-node.jar foo bar
 
If you prefer elastic set up elastic on port localhost:9300 and use the command
 
    java -Djj.example=elastic -jar ingest-node.jar foo bar
 
And if you prefer overkill set both up and use:

     java -Djj.example=both -jar ingest-node.jar foo bar

The code that sets this up and runs it starts at 
[Line 162 of Main.java](https://github.com/nsoft/jesterj/blob/master/code/ingest/src/main/java/org/jesterj/ingest/Main.java#L162)
