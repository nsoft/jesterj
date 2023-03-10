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

import org.jesterj.ingest.model.impl.DocumentImpl;

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
  FORCE {
    @Override
    public boolean isStepSpecific() {
      return false;
    }
  },

  /**
   * Resource needs reprocessing, Semantically similar to Force but indicates a decision made in the Plan, or by
   * JesterJ itself.
   */
  RESTART {
    @Override
    public boolean isStepSpecific() {
      return false;
    }
  },

  /**
   * Resource may require re-indexing. Scanners will look for this state and then decide whether to create
   * a document for processing based on memory and hashing settings.
   */
  DIRTY {
    @Override
    public boolean isStepSpecific() {
      return false;
    }
  },


  /**
   * A scanner has picked up resource, and deemed it worthy of processing document is in-flight. Further processing
   * should continue until another status is set. **WARNING** this status should not be set on an existing document
   * by any processor in the plan. Doing should never be necessary and if done after another step specific status
   * was set could erase the step specific status before it gets persisted. This will be the initial status for
   * child documents however and thus can be set on freshly created child documents.
   */
  PROCESSING {
    @Override
    public boolean isStepSpecific() {
      return false;
    }
  },

  /**
   * Something went wrong executing a step, this document may be retried until retry limit is reached. This is
   * appropriate when there was an issue contacting an external service or upstream steps might provide different
   * enhancements to the document on the next step somehow.
   */
  ERROR {
    @Override
    public boolean isStepSpecific() {
      return false;
    }
  },


  /**
   * The document is being held for communication to an entity in a batch. Commonly used when send to outside systems
   * that are more efficient receiving batches (e.g. Solr).
   */
  BATCHED {
    @Override
    public boolean isStepSpecific() {
      return true;
    }
  },

  /**
   * The document has been accepted by the destination (usually a search index), but may not be searchable until
   * the next commit.
   */
  INDEXED {
    @Override
    public boolean isStepSpecific() {
      return true;
    }
  },

  /**
   * This document was intentionally skipped by the pipeline. Further processing should be avoided.
   */
  DROPPED {
    @Override
    public boolean isStepSpecific() {
      return false;
    }
  },

  /**
   * Terminal state for resources that generate documents that will never succeed and cannot be processed.  A human
   * must intervene and evaluate before this can be processed again.
   */
  DEAD {
    @Override
    public boolean isStepSpecific() {
      return false;
    }
  },

  /**
   * Something went wrong with the document. A human must intervene and evaluate before this can be processed again.
   * Further processing should be avoided, and stateful Scanners should avoid creating new documents for the resource.
   * This status is similar to DEAD, but no retries were attempted. Examples of where this status is appropriate
   * include XML parsing failure or JSON parsing failure when parsing the original document bytes.
   */
  ERROR_DOC {
    @Override
    public boolean isStepSpecific() {
      return false;
    }
  };

  /**
   * Can this status be set with respect to an outputDestination, or should it affect all destinations remaining on
   * the document. Statuses for which this return true will still be set for all destinations if this status is
   * set in a step with a processor that returns true for {@link DocumentProcessor#isSafe()}. Typically, the only
   * status for which that would happen is ERROR. For example if the step is calling out to a service and adding
   * the returned information to the document, that has no outside effects and is thus "safe" but the document
   * information content may be unacceptable for further processing. Also, marking a document indexed from a potent
   * processor that has downstream steps should only mark the destinations relating to the current step and not any
   * downstream steps.
   *
   * @return true if this status should be set for a particular destination step that corresponds to the present
   * potent or idempotent processor rather than all destinations.
   *
   * @see DocumentImpl#getStatusChange()
   */
  public abstract boolean isStepSpecific();

}
