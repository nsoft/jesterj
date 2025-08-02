# Ingest

This portion of jesterj builds a software package that can be run to effect the transport of documents into a search engine.
This software is entirely back end with no UI other than log file output. (A UI will be created under /control in subseqent releases).

# Running

1. Download the release "node" jar (the one that ends in -node.jar) or checkout and build the desired revision from the repository. The command to build a jar is `./gradlew packageUnoJar`
2. Create a plan configuration (see [example](../examples) directories, start with the shakespeare example if you are new) and package it into its own jar
2. `java -jar jesterj-ingest-node-1.0.0.jar`

This will print usage info.
This jar contains all dependencies, and thus can be copied to any machine and run directly without any additional setup.
It will create &lt;user_home_dir&gt;/.jj and place logs and files needed for it's embedded cassandra database there. These directories can be relocated via symlinks if desired after they have been created.

However, to actually cause any interaction with your local documents, you will need to create a PlanProvider defining the inputs, the outputs and any transformations along the way. You'll want to check out our [Documentation](https://github.com/nsoft/jesterj/wiki/Documentation) to learn how to do that

# Developing

If you wish to customize JesterJ you are, of course, generally free to do so, keeping in mind the requirements of our license.
We would be happy to hear about, and help with customizations.
Please use the github discussions feature to let us know what you are doing.
We are also very happy to consider pull requests if you think your customizations might be generally useful or fix bugs.

## Building

We use gradle wrapper for building this section of JesterJ as noted above. Here are some tips on tasks and their intended usage:

* **quickTest** - ~3 min - runs everything but the fault tolerance integration tests, does include some integration tests. _Intended for use during normal development_
* **test** - ~15 min - runs all unit tests, including the much longer fault tolerance tests. _Intended for use in initial exploration of library upgrades, or development of fault tolerance features_
* **check** - ~16 min - depends on test, also runs license checks **_RUN THIS BEFORE PUSH/PR_**
* **packageUnoJar** - ~18 min - depends on check, creates artifacts, including packaging the executable node jar (using [uno-jar](https://github.com/nsoft/uno-jar)). _Intended for use when creating an executable distribution_
* **jacocoTestReport** - ~17min - depends on check, and is run by continuous integration to publish code coverage metrics to CodeCov. IDE based code coverage is recommended for day to day development.
* **build** - ~20 min - depends on check, creates artifacts, and also verifies that there are no dead links in the license documentation files (makes outgoing http requests and checks the returned status codes). _Intended for use before checking in library/dependency upgrades and before publishing releases_

Please use GitHub's fork and pull request features for submitting enhancements and bugfixes.

# System Requirements
 - Posix Operating system (Linux, BSD, Mac OS X, Windows is **not** supported)
 - Java 11 installed (Tested: )
 - Minimum 4 core (8 thread) cpu recommended

