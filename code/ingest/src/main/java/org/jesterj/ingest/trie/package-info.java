/**
 * Trie implementation copied from Apache Commons Collections 4 under the terms of ASL 2.0 License.
 * Sadly this had to be lifted rather than extended due to the strange decision to
 * make AbstractPatriciaTrie package private, with no means of specifying an alternate
 * KeyAnalyzer class (aside from re-implementing all of AbstractPatriciaTrie on top of
 * AbstractBitwiseTrie which doesn't look like fun).
 */
package org.jesterj.ingest.trie;