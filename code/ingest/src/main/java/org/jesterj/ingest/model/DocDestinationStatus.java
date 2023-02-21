package org.jesterj.ingest.model;

import java.io.Serializable;
import java.util.Arrays;

public class DocDestinationStatus implements Serializable {
  private final Status status;

  private final String message;

  private final String potentStep;

  Serializable[] messageArgs;

  public DocDestinationStatus(Status status, String potentStep, String message, Serializable... messageArgs) {
    if (messageArgs != null) {
      for (Serializable messageArg : messageArgs) {
        if (messageArg != null) {
          if (Step.class.isAssignableFrom(messageArg.getClass())) {
            throw new RuntimeException("Never add a step object to the destination status message, use the step name instead");
          }
          if (Document.class.isAssignableFrom(messageArg.getClass())) {
            throw new RuntimeException("Never add a Document object to the destination status message, use the document" +
                " id or other string representation. Note that very long strings (like DocumentImpl.toString() " +
                "will take up a LOT of space in our internal Cassandra persistence and may impact performance");
          }
        }
      }
    }
    this.status = status;
    this.message = message;
    this.potentStep = potentStep;
    this.messageArgs = messageArgs;
  }

  public String getMessage() {
    return message;
  }

  public Status getStatus() {
    return status;
  }

  public String getPotentStep() {
    return potentStep;
  }

  @Override
  public String toString() {
    return "DocDestinationStatus{" +
        "status=" + status +
        ", message='" + message + '\'' +
        ", potentStep='" + potentStep + '\'' +
        ", messageArgs=" + Arrays.toString(messageArgs) +
        '}';
  }

  public Object[] getMessageParams() {
    return messageArgs;
  }
}
