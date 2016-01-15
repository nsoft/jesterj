/*
 * Copyright 2014 Needham Software LLC
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

import static org.jesterj.ingest.logging.Markers.*;

/**
 * The conceptual states available for indexed resources.
 */
public enum Status {
  /**
   * Resource requires re-indexing. Scanners will look for this state when deciding whether to create
   * an item for processing.
   */
  DIRTY {
    @Override
    public Marker getMarker() {
      return SET_DIRTY;
    }
  },

  /**
   * A scanner has picked up resource, and item is in-flight and processing should continue.
   */
  PROCESSING {
    @Override
    public Marker getMarker() {
      return SET_PROCESSING;
    }
  },

  /**
   * This item was intentionally skipped by the pipeline. Further processing should be avoided.
   */
  DROPPED {
    @Override
    public Marker getMarker() {
      return SET_DROPPED;
    }
  },

  /**
   * Something went wrong, human being must intervene and evaluate. Further processing should be avoided, and stateful
   * Scanners should avoid creating new items for the resource.
   */
  ERROR {
    @Override
    public Marker getMarker() {
      return SET_ERROR;
    }
  },

  /**
   * The item has been accepted by the destination index, but may not be searchable until the getNext commit.
   */
  INDEXED {
    @Override
    public Marker getMarker() {
      return SET_INDEXED;
    }
  },

  /**
   * The resource is visible to users in the search index. This state is optional, and requires an index that
   * can report the status of committed items. Most indexes don't do this without customization code.
   */
  SEARCHABLE {
    @Override
    public Marker getMarker() {
      return SET_SEARCHABLE;
    }
  },

  /**
   * Terminal state for resources that generate items that will never succeed and cannot be processed.
   */
  DEAD {
    @Override
    public Marker getMarker() {
      return SET_DEAD;
    }
  };

  public abstract Marker getMarker();
}
