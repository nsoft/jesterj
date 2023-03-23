package org.jesterj.ingest.model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;

public class DocStatusChange implements Serializable {
  private final Status status;

  private final String message;

  private final Collection<String> specificSteps;


  Serializable[] messageArgs;
  public DocStatusChange(Status status, String message, Serializable... messageArgs) {
    this(status,message,null,messageArgs);
  }
  public DocStatusChange(Status status, String message, Collection<String> specificSteps, Serializable... messageArgs) {
    this.specificSteps = specificSteps;
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
    this.messageArgs = messageArgs;
  }

  public String getMessage() {
    return message;
  }

  public Status getStatus() {
    return status;
  }


  @Override
  public String toString() {
    return "DocStatusChange{" +
        "status=" + status +
        ", message='" + message + '\'' +
        ", specificSteps=" + specificSteps +
        ", messageArgs=" + Arrays.toString(messageArgs) +
        '}';
  }

  public Object[] getMessageParams() {
    return messageArgs;
  }

  public Collection<String> getSpecificSteps() {
    return specificSteps;
  }
}
