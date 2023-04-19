/*
 * Copyright 2016 Needham Software LLC
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

/**
 * This interface defines operations fundamental to routing documents.
 */
public interface Router extends Configurable {
  /**
   * Indicates if this router will always route an identical document to the same downstream step or steps.
   * It is important to understand that this attribute refers to determinism with respect to document content
   * and anything that depends on the order in which documents are fed, is NOT deterministic.
   *
   * @return True if the document's destination is deterministic, false if it may vary.
   */
  boolean isDeterministic();

  /**
   * Indicates if the number documents entering this router have a constant linear relationship to the
   * number of documents exiting the router. This relationship need only be constant for a particular plan
   * configuration, and may vary when configured differently in different plans. A document may be considered
   * to have exited the router if it records a terminal status (i.e. {@link Status#DROPPED}, or if it is included in
   * the NextSteps object returned from {@link #route(Document)}
   *
   * @return True if one document in produces a constant number of documents out.
   */
  @SuppressWarnings("unused")
  boolean isConstantNumberOfOutputDocs();

  /**
   * Indicates how many copies of a document may be omitted by this router. If {@link #isConstantNumberOfOutputDocs()}
   * returns false this number represents the maximum output. A negative value indicates that there is no constraint
   * on the number of copies that may be omitted.
   *
   * @return the number of output copies expected.
   */
  int getNumberOfOutputCopies();

  /**
   * The step into which the router is configured.
   *
   * @return The step that owns this router.
   */
  Step getStep();

  /**
   * Decide where to send this document. The result may represent some or all of the steps down stream. Note that
   * it is illegal for a router not to select at least one destination, and this will cause errors. It is also
   * prohibited for the router to select a destination not listed on the document.
   *
   * @see Step#getEligibleNextSteps(Document)
   * @param doc The document to route
   * @return An object encapsulating the destinations to which the document (or copies of it) will be sent.
   */
  NextSteps route(Document doc);
}
