package org.jesterj.ingest.processors;

import org.apache.logging.log4j.ThreadContext;
import org.jesterj.ingest.model.DocStatusChange;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.Status;

import java.io.Closeable;
import java.util.stream.Collectors;

public class DocumentLoggingContext implements Closeable {

  public DocumentLoggingContext(Document d) {
    for (ContextNames name : ContextNames.values()) {
      String val = name.fromDoc(d);
      ThreadContext.put(String.valueOf(name), val);
    }
  }

  /**
   * Wrap some code
   * @param r the code to execute in context
   */
  public void run(Runnable r)  {
    try (this) {
      r.run();
    }
  }

  @Override
  public void close() {
    for (ContextNames value : ContextNames.values()) {
      ThreadContext.remove(String.valueOf(value));
    }
  }

  public enum ContextNames {
    JJ_DOC_ID {
      @Override
      String fromDoc(Document d) {
        return d.getId();
      }
    },

    JJ_DOC_HASH {
      @Override
      String fromDoc(Document d) {
        return d.getHash();
      }
    },

    JJ_SCANNER_NAME {
      @Override
      String fromDoc(Document d) {
        return d.getSourceScannerName();
      }
    },

    JJ_PARENT_ID {
      @Override
      String fromDoc(Document d) {
        return d.getParentId();
      }
    },

    JJ_ORIG_PARENT_ID {
      @Override
      String fromDoc(Document d) {
        return d.getOrignalParentId();
      }
    },

    JJ_OUTPUT_STEP_CHANGES {
      @Override
      String fromDoc(Document d) {
        return String.join(",", d.listChangingDestinations()); // , not allowed in step name
      }
    },
    JJ_STATUS_CHANGES {
      @Override
      String fromDoc(Document d) {
        return d.listChangingDestinations().stream().map(dest -> d.getStatusChange().getStatus().toString())
            .collect(Collectors.joining(",")); // statuses won't have commas
      }
    };
     abstract String fromDoc(Document d);
  }
}
