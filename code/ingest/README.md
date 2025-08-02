# Ingest

This portion of jesterj defines a software package that can be run to effect the transport of documents into a search engine.
This software is entirely back end with no UI other than log file output. (A UI will be created under /control in subseqent releases).
The primary things that set the current release of Jesterj appart from tradditional ETL and many other solutions for getting documents into search engines are:

1. It is built for search. Transformations are performed on ket to list of value datastructures, not rows (Documents), gone are the days of repeatedly splitting and joining delimited lists when manipulating data for multivalue fields!
1. Zero infrastructure config for simple to moderate projects. No database to setup or connect, no hadoop or spark cluster to build out, no need to push configs or state into zookeeper. All you need is the data source, the search engine, a Plan for processing the data and this jar file.
1. Ability to handle branched and joined paths (Full DAG processing)
1. Built in fault tolerance out of the box, even for complex processing workflow

# Running

1. Checkout and build the head revision from the repository to produce a "node" jar (much better than last release now) The command to build a jar is `./gradlew packageUnoJar`
2. java -jar jesterj-node-1.0-beta2.jar

This will print usage info. This jar contains all dependencies, and thus can be copied to any machine and run
without any additional setup. It will create &lt;user_home_dir&gt;/.jj and place logs and files needed for it's embedded cassandra database there. These directories can be relocated via symlinks if desired after they have been created.

However without an implementation of a PlanProvider, we can't do much useful, so next you'll want to check out our [Documentation](https://github.com/nsoft/jesterj/wiki/Documentation)

# Developing

If you wish to customize JesterJ you are of course generally free to do so, keeping in mind the requirements of our license.
We would be happy to hear about, and help with customizations.
Please use the github discussions feature to let us know what you are doing.
We are also very happy to consider pull requests if you think your customizations might be generally useful or fix bugs.

## Building

We use gradle wrapper for building this section of JesterJ as noted above, here are some tips on tasks and their intended usage

* **quickTest** - ~3 min - runs everything but the fault tolerance integration tests, does include some integration tests. _Intended for use during normal development_
* **test** - ~15 min - runs all unit tests, including the much longer fault tolerance tests takes 15 minutes. _Intended for use in initial exploration of library upgrades, or development of fault tolerance features_
* **check** - ~16 min - depends on test, also runs license checks **_RUN THIS BEFORE PUSH/PR_**
* **packageUnoJar** - ~18 min - depends on check, creates artifacts, including packaging the executable node jar (using [uno-jar](https://github.com/nsoft/uno-jar)). _Intended for use when creating an executable distribution_
* **jacocoTestReport** - ~17min - depends on check, and is run by continuous integration to publish code coverage metrics to CodeCov. IDE based code coverage is recommended for dat to day development.
* **build** - ~20 min - depends on check, creates artifacts, and also verifies that there are no dead links in the license documentation files (makes outgoing http requests and checks the returned status codes). _Intended for use before checking in library/dependency upgrades and before publishing releases_

# System Requirements
 - Posix Operating system (Linux, BSD, Mac OS X, Windows is **not** supported)
 - Java 11 installed (Tested: )
 - Minimum 4 core (8 thread) cpu recommended

