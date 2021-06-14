# Ingest

This portion of jesterj defines a software package that can be run to effect the transport of documents into a search engine. This software is entirely back end with no UI other than log file output. (A UI will be created in subseqent releases). The primary things that set the current release of Jesterj appart from tradditional ETL and many other solutions for getting documents into search engines are:

1. It is built for search. Transformations are performed on ket to list of value datastructures, not rows (Documents), gone are the days of repeatedly splitting and joining delimited lists when manipulating data for multivalue fields! 
1. Zero infrastructure config for simple to moderate projects. No database to setup or connect, no hadoop or spark cluster to build out, no need to push configs or state into zookeeper. All you need is the data source, the search engine, a Plan for processing the data and this jar file.
1. Built in fault tolerance out of the box
1. Ability to handle branched and joined paths

# Running

1. Checkout and build the head revision from the repository to produce a "node" jar (much better than last release now)
2. java -jar jesterj-node-1.0-beta1.jar 

This will print usage info. This jar contains all dependencies, and thus can be copied to any machine and run
without any additional setup. It will create &lt;user_home_dir&gt;/.jj and place logs and files needed for it's embedded cassandra database there. These directories can be relocated via symlinks if desired after they have been created.

However without an implementation of a PlanProvider, we can't do much useful, so next you'll want to check out our [Documentation](https://github.com/nsoft/jesterj/wiki/Documentation)

