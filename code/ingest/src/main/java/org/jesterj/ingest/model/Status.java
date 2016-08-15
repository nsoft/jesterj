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

import org.apache.logging.log4j.Marker;

import java.io.Serializable;

import static org.jesterj.ingest.logging.Markers.SET_DEAD;
import static org.jesterj.ingest.logging.Markers.SET_DIRTY;
import static org.jesterj.ingest.logging.Markers.SET_DROPPED;
import static org.jesterj.ingest.logging.Markers.SET_ERROR;
import static org.jesterj.ingest.logging.Markers.SET_INDEXED;
import static org.jesterj.ingest.logging.Markers.SET_PROCESSING;
import static org.jesterj.ingest.logging.Markers.SET_READY;
import static org.jesterj.ingest.logging.Markers.SET_SEARCHABLE;

/**
 * The conceptual states available for indexed resources.
 */
public enum Status implements Serializable {
  /**
   * Resource requires re-indexing. Scanners will look for this state when deciding whether to create
   * an document for processing.
   */
  DIRTY {
    @Override
    public Marker getMarker() {
      return SET_DIRTY;
    }
  },

  /**
   * A scanner has picked up resource, and document is in-flight and processing should continue.
   */
  PROCESSING {
    @Override
    public Marker getMarker() {
      return SET_PROCESSING;
    }
  },

  /**
   * This document was intentionally skipped by the pipeline. Further processing should be avoided.
   */
  DROPPED {
    @Override
    public Marker getMarker() {
      return SET_DROPPED;
    }
  },

  /**
   * Something went wrong, human being must intervene and evaluate. Further processing should be avoided, and stateful
   * Scanners should avoid creating new documents for the resource.
   */
  ERROR {
    @Override
    public Marker getMarker() {
      return SET_ERROR;
    }
  },

  /**
   * The document is being held for communication to an entity in a batch. Commonly used when send to outside systems
   * that are more efficient receiving batches (e.g. Solr).
   */
  BATCHED {
    @Override
    public Marker getMarker() {
      return SET_READY;
    }
  },
  
  /**
   * The document has been accepted by the destination index, but may not be searchable until the next commit.
   */
  INDEXED {
    @Override
    public Marker getMarker() {
      return SET_INDEXED;
    }
  },

  /**
   * The resource is visible to users in the search index. This state is optional, and requires an index that
   * can report the status of committed documents. Most indexes don't do this without customization code.
   */
  SEARCHABLE {
    @Override
    public Marker getMarker() {
      return SET_SEARCHABLE;
    }
  },

  /**
   * Terminal state for resources that generate documents that will never succeed and cannot be processed.
   */
  DEAD {
    @Override
    public Marker getMarker() {
      return SET_DEAD;
    }
  };

  public abstract Marker getMarker();
}
