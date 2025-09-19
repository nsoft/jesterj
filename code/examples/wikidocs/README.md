# Wikidocs Example

This example injests a linefile based on wikipedia that is one of the data sources for the lucene nightly benchmarks. There are over 33 million lines which become documents of approximately 1k, and this is set up to have multiple send to solr processors, so this will be somewhat more useful for looking at performance.

## Work in progress!
This may have issues or break things, no guarantees yet.
Note that at present the schema in /src/main/solr is not yet working, but the ingest runs against the standard solr _default just fine.