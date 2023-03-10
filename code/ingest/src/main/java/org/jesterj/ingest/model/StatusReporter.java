package org.jesterj.ingest.model;

import org.jetbrains.annotations.NotNull;

import java.util.List;

public interface StatusReporter {
  void reportStatus(Document doc);

  @NotNull List<DocDestinationStatus> getChangedDestinations(DocStatusChange statusChange);

}
