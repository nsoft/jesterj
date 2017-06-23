Under construction...

https://github.com/nsoft/jesterj/issues/73

1. Set up either Solr cloud with zookeeper on 9983 or elastic on port 9300 (or both, or edit ShakespearConfig.java to match your ports)
1. in /jesterj/code/ingest run `./gradlew oneJar`
1. in this directory run `./gradelw build`
1. in this directory run `java -jar -Djj.javaConfig=build/libs/example-shakespeare-0.2-SNAPSHOT.jar ../../ingest/build/libs/ingest-node.jar foo bar`
1. Observe issue #59 (unless I've fixed it!)
1. For a second time, run `java -jar -Djj.javaConfig=build/libs/example-shakespeare-0.2-SNAPSHOT.jar ../../ingest/build/libs/ingest-node.jar foo bar`

Your search engine(s) should now have indexed shakespeare's plays.  If you edit or add files the changes will be re-indexed. Happy Searching!
