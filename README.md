JesterJ
=======
A highly flexible, scalable, fault-tolerant document ingestion system designed for search.

[![License](https://img.shields.io/badge/license-Apache%202.0-B70E23.svg?style=plastic)](http://www.opensource.org/licenses/Apache-2.0)
[![Build Status](https://github.com/nsoft/jesterj/actions/workflows/gradle.yml/badge.svg)](https://github.com/nsoft/jesterj/actions)

Builds are run on infrastructure kindly donated by [<img align="top" src="https://crave.io/wp-content/uploads/2022/09/Crave_logo_black_bg-e1663023213710.png" alt="" width="100px" height="26px">](https://crave.io/)

## The problem
Frequently, search projects start by feeding a few documents manually to a search engine, often via the "just for testing" built-in processing features of Solr such as [SolrCell](https://solr.apache.org/guide/6_6/uploading-data-with-solr-cell-using-apache-tika.html) or [post.jar](https://solr.apache.org/guide/6_6/post-tool.html#simpleposttool).
These features are documented and included to help users get a feel for what they can do with Solr with minimal painful setup.

This is good, and that's how it should be for first explorations. **Unfortunately, it's also a potential trap.**
Large-scale ingestion of documents for search is non-trivial, and many projects outgrow these simple tools and have to throw away their early exploratory work. Nobody likes setting aside valuable work, and it's natural to resist, but the longer one clings to an insufficient tool, the bigger, more difficult, and more expensive the migration is.


Common problems are:

- It works "ok" for a small test corpus and then becomes unstable on a larger production corpus.
- The code written to feed into such interfaces often needs to be repeated for several types of documents or various document formats. It can easily lead to duplication and cut-and-paste copying of common functionality.
- No way to recover if indexing fails partway through reindexing.
- If failure is related to the size of a growing corpus, failures become increasingly common, and the system eventually cannot be reindexed or upgraded at all.

## JesterJ's solution

JesterJ makes it super easy to start with a robust, full-featured indexing infrastructure, so that you don't have to re-invent the wheel, and you don't have to throw away your early work.

The key aspects for achieving this are simplicity, robustness, flexibility, and scalability:

- A variety of re-usable processing components are provided _(flexibility)_
- Scanners (active connectors) for database and filesystem data sources _(simplicity)_
- Custom processors only require a 4-method interface _(simplicity)_
- Specialized classloading allows any version of a library in your custom code _(flexibility, simplicity)_
- Simplified [startup](): `java -jar jesterj.jar <id> <secret>` _(simplicity)_
- Built in embedded Cassandra for performant persistent storage _(simplicity)_
- Optional auto-detection of changes to documents _(flexibility, simplicity)_
- Automatic fault-tolerant restart skipping previously seen documents _(robustness, scalability)_
- Multi-threaded processing to leverage modern machines with large numbers of cores. _(scalability)_
- Explicit and direct control of threading. Easy to ensure more threads working on heavy steps _(scalability)_
- Single system handling multiple data sources _(flexability, scalability, simplicity)_
- Pre-baked batching of documents for efficient transmission to the search engine _(scalability, simplicity)_
- DAG capable processing model, and graphical visualization _(flexibility, simplicity)_

DAG-structured processing is a key feature that is not provided by other tools.
Most other tools require a linear pipeline structure, which can become limiting. As time passes, features and enhancements often add complexity.
Multiple data sources are also a common dimension for growth. With other systems, you wind up deploying a system per data source.
JesterJ is designed to handle complex indexing scenarios.
Consider the following hypothetical indexing workflow, where the system has evolved from a simple linear ingestion into a single index:
- Data format changed from, effectively creating a new data source (old data may need reindexing)
- An external system needed to know that the document was received
- Product features required a faster, optimized line-item-only search index
- New features were added to the product that required block-join indexing, but old features couldn't be migrated, so a new index was required.
In other tools, this will mean six indexing processes (two sources times three sinks), all of which need to send messages, none of which are coordinated if one fails. In JesterJ, it is all one coherent system:
![Complex Processing](https://raw.githubusercontent.com/nsoft/jesterj/79ed481c7c0b98469e3e41c96b92170837a26130/code/examples/routing/complex-routing.png)

JesterJ handles such scenarios with a single centralized processing plan, and there is no need to deploy new indexing infrastructure. Furthermore, JesterJ will ensure that if the system is unplugged partway through indexing, you won't get a second message about an order received for everything it processed previously (fault tolerance). The default mode for JesterJ is to ensure delivery for steps that are not marked safe or idempotent at most once. Safe steps do not have external effects, and idempotent steps may be repeated en route to the final processing end point.

See the [website](http://www.jesterj.org) and the [documentation](https://github.com/nsoft/jesterj/wiki/Documentation) for more info

# Getting Started

Please see the [documentation in the wiki](https://github.com/nsoft/jesterj/wiki/Documentation)

# Project Status

**Current release**: 1.0. This is the best version to use, and should be fully functional.

NOTE: The current release targets any design and load a single machine can service.
JesterJ is explicitly designed to take advantage of machines with many processors in future releases.
You can design your plan with duplicates of your slowest step to alleviate bottlenecks. In future versions, the load will be spread across machines. Currently, each duplicate implies an additional thread working on that step.

# JDK versions

Presently, only JDK 11 has been tested regularly.
Unit tests have passed on JDK 17, but the initial system startup and custom class loading are the most JDK-sensitive parts, so we welcome feedback on experiences with more recent JDK versions.
Any Distribution of JDK 11 should work.
Support for Java 17 and future LTS versions is among our highest priorities for future releases.
Building with the latest uno-jar version may be sufficient, but this is not yet certified. https://github.com/nsoft/uno-jar/issues/37
# Discord Server

Discuss features, ask questions, etc, on Discord. https://discord.gg/RmdTYvpXr9

## Features:

In this release, we have the following features.

* Ability to visualize the structure of your plan (.dot or .png format: [example from unit tests here](https://tinyurl.com/22k7tu74) )
* Simple filesystem scanner for locally mounted drives (replacement for post.jar)
* JDBC scanner (replacement for Data Import Handler!)
* Scanners can remember what documents they've seen (or not, boolean flag)
* Scanners can recognize updated content (or not, boolean flag)
* Send to Solr processor with tunable batch sizes
* Tika processor to extract content from Word/PDF/XML/HTML, etc (Replacement for SolrCell!)
* Stax extract processor for dissecting XML documents directly.
* Copy field processor to rename source fields to the desired index field
* Regexp replace processor to edit field content, or drop fields that don't match
* Split field processor to split delimited values for multi-value fields
* Drop field processor to get rid of annoying excess fields.
* Field template processor for composing field content using a Velocity template
* URL encode processor to encode the value of a field and make it safe for use in URLs
* Fetch URL processor for acquiring or enhancing content by contacting other systems
* Log and drop processor for when you identify an invalid document* Date Reformat processor, because dates, formatting... always. (*sigh*)* Human Readable File Size processor
* Solr sender to send documents to Solr in batches.
* Pre-Analyze processor to move Solr analysis workload out of Solr (just give it your schema.xml!)
* Embedded Cassandra server (no need to install Cassandra yourself!)
* Cassandra config and data location configurable, defaults to `~/.jj/cassandra`
* Support for fault tolerance, writing status change events to the embedded Cassandra server
* Initial API/process for user-written document processors. (see [documentation](https://github.com/nsoft/jesterj/wiki/Documentation))
* 70% test coverage (jacoco)
* Simple, single Java file to configure everything, non-Java programmers need only follow a simple example (for use cases not requiring custom code)
* If you DO need custom code, that code can be packaged as an [uno-jar](https://github.com/nsoft/uno-jar) to provide all required dependencies and escape from any library versions that JesterJ uses! You only have to deal with your OWN jar hell, not ours! Of course, you can also rely on whatever we already provide. The classloaders for custom code prefer your uno-jar and then default to whatever JesterJ has available on its classpath.
* Runnable example to [execute a plan](https://github.com/nsoft/jesterj/blob/master/code/ingest/README.md) that scans a filesystem, and indexes the documents in Solr.

Release 1.0 is intended to be usable for small to medium-sized projects (tens of millions of documents or low hundreds of millions of documents with some patience).

## Road Map

The best guess at any time of what will be in future releases is given by the milestones filters [on our issues page](https://github.com/nsoft/jesterj/issues)
