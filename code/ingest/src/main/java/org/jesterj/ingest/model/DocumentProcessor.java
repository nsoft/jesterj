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

package org.jesterj.ingest.model;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 9/28/14
 */

import java.io.Closeable;

public interface DocumentProcessor extends Configurable, Closeable {

  /**
   * Mutate, validate or transmit an document (to a search index). Implementations must not throw any
   * {@link java.lang.Throwable} that is not a JVM {@link java.lang.Error} and should be written expecting the
   * possibility that the code might be interrupted at any point. Practically this means Document processors
   * will perform a single persistent or externally visible action. Large complex processors that write to
   * disk, DB, or elsewhere multiple times run the risk of partial completion, and thus those actions must be
   * idempotent so that subsequent re-execution of the code is safe. "Check then write" is of course a performance
   * anti-pattern. All document may set status and status message via {@link Document#setStatus(Status)} and
   * {@link Document#setStatusMessage(String)}. The document processor has no need to add the document to the getNext
   * step in the plan as this will be handled by the Step's infrastructure based on the status of the plan.
   *
   * @param document the item to process
   * @return true if processing succeeded, false otherwise.
   */
  Document[] processDocument(Document document);

  default boolean hasExternalSideEffects() {
    return false;
  }

  default void close(){}
}
