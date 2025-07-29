package org.jesterj.ingest.processors;

import java.util.List;

import org.jesterj.ingest.model.Document;

/**
 * Interface for classes wishing to be notified after a batch processor has attempted to send a batch.
 * Individual document statuses should be inspected to determine the result.
 */
public interface BatchSendListener {
  void batchSent(List<Document> sentDocs);
}
