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

import org.jesterj.ingest.model.impl.StepImpl;

public interface DocumentProcessor extends Configurable {

  /**
   * Mutate, validate or transmit a document. Implementations must not throw any * {@link java.lang.Throwable}
   * that is not a JVM {@link java.lang.Error} and should be written expecting the possibility that the code might
   * be interrupted at any point. Practically this means Document processors should perform no more than one
   * persistent or externally visible actions and that action should be transactional. Large complex processors that
   * write to disk, DB, or elsewhere multiple times run the risk of partial completion. Similarly, since JesterJ is
   * a long-running system it will often cease operation due to unexpected outages (power cord, etc.), so it is not a
   * good idea to hold resources that require an explicit release or "return". "Check then write" is of course a
   * performance anti-pattern with respect to external networked or disk resources since network and disk io are
   * typically slow to access. Processors should feel free to set the status of a document and add a status message via
   * {@link Document#setStatus(Status)} and {@link Document#setStatusMessage(String)}. The document processor has no
   * need to add the document to the next step in the plan as this will be handled by the infrastructure in
   * {@link StepImpl} based on the status of the document.
   *
   * @param document the item to process
   * @return The documents that result from the processing in this step. Documents with status of
   * {@link Status#PROCESSING} will be processed by subsequent steps, and documents with any other status will
   * have their status recorded and will not be processed by subsequent steps.
   */
  Document[] processDocument(Document document);

  /**
   * Indicates if this processor can be re-executed multiple times safely. The concept is similar to "SAFE" http
   * requests. By default, this will return true unless explicitly overridden.
   *
   * @return true if the execution of this processor will have no externally persistent side effects.
   */
  default boolean isSafe() {return true;}

  /**
   * Indicates if this processor can be executed multiple times, without cumulative external side effects. This is
   * similar to the "IDEMPOTENT" concept for http methods. For example "RecordDocumentSeen" would be idempotent if
   * it set a flag on a database record to true since any number of repeated invocations would result in the same
   * external state. However, "DecrementBankBalance" would not be idempotent because repeated invocations continue
   * to change the external state. Be very careful when creating this type of processors that you do not rely on
   * ordering that your JesterJ plan does not guarantee. "SetBankBalance" would also be idempotent, but if repeated,
   * might undo the effect of an intervening "DecrementBankBalance".
   *
   * @return true if the repeated execution of this processor with the same inputs results in a constant external state
   */
  default boolean isIdempotent() { return false; }

  /**
   * Indicates a processor for which repeated invocations have cumulative external side effects. "Potent" is a term
   * coined for use in JesterJ meant to be faster to type and easier to think about than "non-idempotent". Potent processors
   * are the key processors that fault tolerance must avoid repeating, and thus adding potent processors increases the load
   * on the internal cassandra instance.
   *
   * @return true if the repeated execution of this processor with the same inputs results in cumulative or otherwise
   * inconstant external state.
   */
  default boolean isPotent() {
    return false;
  }

}
