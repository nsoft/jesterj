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
  public static final Cloner<Document> CLONER = new Cloner<>();
  Map<Step, StepStatusHolder> steps = new HashMap<>();

  public NextSteps(Document doc, Step... next) {
    Objects.requireNonNull(doc);
    Objects.requireNonNull(next);
    if (next.length == 0) {
      throw new RuntimeException("Router selected no next steps. This is a bug in the router implementation " +
          "or a misconfigured router");
    }
    if (next.length == 1) {
      steps.put(next[0],new StepStatusHolder(TRY, doc));
      return;
    }
    // below here only for cases where more than step. To handle that we make clones. The original will be used for
    // Drop Status updates and the clones will have their destinations adjusted to match their designated path.
    for (Step step : next) {
      try {
        Document tmp = CLONER.cloneObj(doc);
        tmp.removeAllOtherDestinationsQuietly(step.getOutputDestinationNames());
        if (tmp.getIncompleteOutputDestinations().length > 0) {
          // ONLY keep things that have a destination.
          steps.put(step, new StepStatusHolder(TRY, tmp));
        }
      } catch (IOException | ClassNotFoundException e) {
        StepStatusHolder stepStatusHolder = new StepStatusHolder(FAIL, null);
        stepStatusHolder.setException(e);
        steps.put(step, stepStatusHolder);
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
