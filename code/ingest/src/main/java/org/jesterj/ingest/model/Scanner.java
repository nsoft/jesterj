/*
 * Copyright 2013-2016 Needham Software LLC
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
 * Date: 11/10/13
 */


import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Monitors a document source for changes on a regular basis. When new files are found, they are submitted
 * to the supplied queue. Note that Scanners do not normally support the methods from
 * {@link java.util.concurrent.BlockingQueue} since they normally only output documents, and never receive them.
 * These methods may throw {@link java.lang.UnsupportedOperationException}
 */
public interface Scanner extends Step {

    /**
     * A function that can be used to provide a custom transformation of the identifier generated by the
     * scanning process. The default transformation is an identity transform.
     *
     * @return the function to map ID to new ID
     */
    default Function<String, String> getIdFunction() {
        return s -> s;
    }

    /**
     * Get a procedure that takes a document and uses this information to persist a record that this
     * document has been scanned. Typical implementations might be writing a status to cassandra, updating a row in
     * a database, or renaming the target file. By convention, the first element of the object array passed will
     * be a String identifier, and the second argument  will be the document object. Subsequent arguments are
     * unrestricted. The default implementation is a no-op.
     *
     * @return a {@link java.util.function.Consumer} that consumes data about a document and has the side effect of
     * persisting a record that the document was scanned.
     */
    default Consumer<Document> getDocumentTracker() {
        return document -> {
        };
    }

    /**
     * A callback that calls docFound() on the scanner when a document is found that needs to be indexed.
     * The call back should call {@link org.jesterj.ingest.model.impl.ScannerImpl#scanStarted()} when it starts
     * doing work, and {@link org.jesterj.ingest.model.impl.ScannerImpl#scanFinished()}
     * when it has completed any work for which concurrency might be relevant.
     * <p>
     *
     * @return a {@link Runnable} object that locates documents.
     */
    Runnable getScanOperation();

    /**
     * The interval for the scanner to fire. Scanners implementations must not begin a new scan more
     * frequently than this interval. There is no guarantee that the scan will begin this frequently
     * although implementations are encouraged to report any occasions on which scans are started
     * later than this interval would imply as warnings. An interval of less than zero indicates
     * that the scanner should only run once.
     *
     * @return the scan interval. Defaults to 30 minutes
     */
    long getInterval();

    /**
     * True if a new scan may be started. Implementations may choose not to start a new scan until the
     * old one has completed. This value is independent of {@link #isActive()}.
     *
     * @return true if a new scan should be started
     */
    boolean isReady();

    /**
     * Load a document based on the document's id.
     *
     * @param id the id of the document, see also {@link Document#getId()}
     * @return An optional that contains the document if it is possible to retreive the document by ID
     */
    Optional<Document> fetchById(String id);

}
