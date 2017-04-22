An example config used to test logging of exceptions

1. Set up either Solr cloud with zookeeper on 9983 or elastic on port 9300 (or both, or edit ShakespearConfig.java to match your ports)
1. in /jesterj/code/injest run `./gradlew oneJar`
1. in this directory run `./gradelw build`
1. in this directory run `java -jar -Djj.javaConfig=build/libs/exception-0.2-SNAPSHOT.jar ../../ingest/build/libs/ingest-node.jar foo bar`
1. Observe issue #59 (unless I've fixed it!)
1. For a second time, run `java -jar -Djj.javaConfig=build/libs/exeption-0.2-SNAPSHOT.jar ../../ingest/build/libs/ingest-node.jar foo bar`

You should see an exeption thrown after the 5th document 