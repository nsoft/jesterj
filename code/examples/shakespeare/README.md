To see JesterJ in action, you can run this example. (Java 11 required)

**PLEASE DO use this project to bootstrap your own!**

## Setup Solr
1. Set up a local Solr Cloud node with zookeeper on 9983  (or edit ShakespeareConfig.java to match your ports[^1])
1. Create a collection named jjtest using the _default configset

## Set up JDK 11 for JesterJ
- For the commands below to work you will need to `export JAVA_HOME=~/gus/tools/java/zulu11.50.19-ca-jdk11.0.12-linux_x64/` (or similar)

## Build or Download JesterJ

### Download: 
- https://github.com/nsoft/jesterj/releases

### Build:
1. Clone https://github.com/nsoft/jesterj.git
1. In /jesterj/code/ingest run `./gradlew  publishToMavenLocal` (to publish 1.0-SNAPSHOT required by this example)
1. In /jesterj/code/example/shakespeare run `./gradlew build`

## Run JesterJ
1. In /jesterj/code/example/shakespeare run `$JAVA_HOME/bin/java -jar ../../ingest/build/libs/jesterj-ingest-1.0-SNAPSHOT-node.jar build/libs/example-shakespeare-1.0-SNAPSHOT.jar shakespeare mysecret`

Once JesterJ goes throug it's startup, your search engine(s) should have indexed shakespeare's plays within a minute or so.
Note that the example is set up for batches of 20 and partial batch sending every 20 seconds so for a brief moment you may see 40 documents, but in 20 seconds or less from that you should see all 44 documents in your index

NOTE: if you are using unmodified _default config as suggested, you won't see anything until you send an explicit commit to solr.

```
http://localhost:8983/solr/jjtest/update?commit=true
http://localhost:8983/solr/jjtest/select?q=%22poor%20yorick%22
```
Should yield
```
{
  "responseHeader":{
    "zkConnected":true,
    "status":0,
    "QTime":24,
    "params":{
      "q":"\"poor yorick\"",
      "_forwardedCount":"1",
      "_":"1614052438408"}},
  "response":{"numFound":1,"start":0,"numFoundExact":true,"docs":[
      {
        "doc_raw_size":[182567],
        ".fields":["doc_raw_size",
          "created_dt",
          "Content_Encoding",
          "created",
          "accessed_dt",
          "X_Parsed_By",
          "accessed",
          "file_size_i",
          "Content_Type",
          "modified_dt",
          "modified",
          "id",
          "csv_delimiter"],
        "created_dt":"2018-02-04T16:41:50.111Z",
        "Content_Encoding":["ISO-8859-1"],
        "created":[1517762510111],
        "accessed_dt":"2021-02-23T01:45:26.665Z",
        "X_Parsed_By":["org.apache.tika.parser.DefaultParser"],
        "accessed":[1614044726665],
        "file_size_i":182567,
        "Content_Type":["text/tsv; charset=ISO-8859-1; delimiter=tab"],
        "modified_dt":"2018-02-04T16:41:50.111Z",
        "modified":[1517762510111],
        "id":"file:///home/gus/projects/jesterj/code/jesterj/code/examples/shakespeare/data/tragedies/hamlet",
        "csv_delimiter":["tab"],
        "_version_":1692456843219042304}]
  }}
```


If you edit or add files the changes will be re-indexed automatically. Happy Searching!

[^1]: Since I tend to build solr frequently I use solr's [https://github.com/apache/solr/blob/main/dev-tools/scripts/cloud.sh](cloud.sh) for testing I typically modify it to have these lines:
    ````
                .withZookeeper("localhost:2181")
                .zkChroot("/solr__home_gus_projects_apache_solr_testing_2023-02-02")
    ````
