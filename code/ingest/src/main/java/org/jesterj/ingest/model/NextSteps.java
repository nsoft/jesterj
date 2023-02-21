package org.jesterj.ingest.model;

import org.jesterj.ingest.utils.Cloner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.jesterj.ingest.model.NextSteps.StepStatus.*;

/**
 * Encapsulate and manage status of steps to which a document should be sent
 */
public class NextSteps {
  Map<Step, StepStatusHolder> steps = new HashMap<>();

  public NextSteps(Document doc, Step... next) {
    Objects.requireNonNull(doc);
    Objects.requireNonNull(next);
    if (next.length == 0) {
      throw new RuntimeException("Router selected no next steps. This is a bug in the router implementation " +
          "or a misconfigured router");
    }

    for (int i = 0; i < next.length; i++) {
      Step step = next[i];
      if (i == 0) {
        steps.put(step,new StepStatusHolder(TRY, doc));
        continue;
      }
      try {
        Cloner<Document> cloner = new Cloner<>();
        Document tmp = cloner.cloneObj(doc);
        steps.put(step,new StepStatusHolder(TRY,tmp));
      } catch (IOException | ClassNotFoundException e) {
        StepStatusHolder stepStatusHolder = new StepStatusHolder(FAIL, null);
        stepStatusHolder.setException(e);
        steps.put(step,stepStatusHolder);
      }


    }

  }

  public void update(Step step, StepStatus stepStatus) {
    steps.get(step).setStatus(stepStatus);
  }


  public int size() {
    return steps.size();
  }

  public List<Step> list() {
    return new ArrayList<>(steps.keySet());
  }

  public List<Map.Entry<Step,StepStatusHolder>> remaining() {
    return steps.entrySet().stream()
        .filter(e-> e.getValue().getStatus().ordinal() < SENT.ordinal())
        .collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return "NextSteps{" +
        "steps=" + steps +
        '}';
  }

  public enum StepStatus {
    TRY,
    RETRY,
    SENT,
    FAIL
  }

  public static class StepStatusHolder {
    private  StepStatus status;
    private final Document doc;
    private  Exception exception;

    StepStatusHolder(StepStatus status, Document doc) {
      this.status = status;
      this.doc = doc;
    }


    public Document getDoc() {
      return doc;
    }

    public Exception getException() {
      return exception;
    }

    public void setStatus(StepStatus status) {
      this.status = status;
    }

    public void setException(Exception exception) {
      this.exception = exception;
    }

    public StepStatus getStatus() {
      return status;
    }
  }
}
