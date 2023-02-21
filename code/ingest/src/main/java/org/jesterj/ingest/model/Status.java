/*
 * Copyright 2014-2016 Needham Software LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jesterj.ingest.model;/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 9/29/14
 */

import java.io.Serializable;

/**
 * The conceptual states available for indexed resources.
 */
public enum Status implements Serializable {

  /**
   * Resource must be re-indexed. Scanners will always reprocess documents that have this status set. This status
   * would typically be set by an external application or administrator interacting with cassandra directly, to
   * handle special cases such as a document that was declared dead due repeated errors after a misconfiguration
   * of JesterJ or the plan. Once the misconfiguration is fixed, and affected documents have been identified it is
   * best to write a FORCE event to the appropriate table in the keyspace for that scanner in that version of the plan
   * JesterJ never writes force events itself, they only come from external sources.
   */
  FORCE,

  /**
   * Resource needs reprocessing, Semantically similar to Force but indicates a decision made in the Plan, or by
   * JesterJ itself.
   */
  RESTART,

  /**
   * Resource may require re-indexing. Scanners will look for this state and then decide whether to create
   * a document for processing based on memory and hashing settings.
   */
  DIRTY,


  /**
   * A scanner has picked up resource, and deemed it worthy of processing document is in-flight. Further processing
   * should continue until another status is set.
   */
  PROCESSING,

  /**
   * Something went wrong, human being must intervene and evaluate. Further processing should be avoided, and stateful
   * Scanners should avoid creating new documents for the resource.
   */
  ERROR ,

  /**
   * The document is being held for communication to an entity in a batch. Commonly used when send to outside systems
   * that are more efficient receiving batches (e.g. Solr).
   */
  BATCHED,

  /**
   * The document has been accepted by the destination (usually a search index), but may not be searchable until
   * the next commit.
   */
  INDEXED,

  /**
   * The resource is visible to users in the search index. This state is optional, and requires an index that
   * can report the status of committed documents. Most indexes don't do this without customization code.
   */
  SEARCHABLE ,

  /**
   * This document was intentionally skipped by the pipeline. Further processing should be avoided.
   */
  DROPPED ,

  /**
   * Terminal state for resources that generate documents that will never succeed and cannot be processed.
   */
  DEAD

}
