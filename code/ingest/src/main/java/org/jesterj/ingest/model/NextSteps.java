package org.jesterj.ingest.model;

import org.jesterj.ingest.utils.Cloner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.jesterj.ingest.model.NextSteps.StepStatus.SENT;
import static org.jesterj.ingest.model.NextSteps.StepStatus.TRY;

/**
 * Encapsulate and manage status of steps to which a document should be sent
 */
public class NextSteps {


  public NextSteps(Document doc, Step... next) {
    Objects.requireNonNull(doc);
    Objects.requireNonNull(next);
    for (Step step : next) {
      steps.put(step,TRY);
    }
    Document original = doc;
    if (steps.size() > 0) {
      for (StepStatus stat : steps.values()) {
        if (doc != null) {
          stat.doc = doc;
          doc = null;
        } else {
          try {
            Cloner<Document> cloner = new Cloner<>();
            stat.doc = cloner.cloneObj(original);
          } catch (IOException | ClassNotFoundException e) {
            stat.exception = e;
          }
        }
      }
    }
  }

  public void update(Step step, StepStatus stepStatus) {
    steps.put(step,stepStatus);
  }

  public enum StepStatus {
    TRY,
    RETRY,
    SENT,
    FAIL;

    public Document doc;
    public Exception exception;
  }
  Map<Step, StepStatus> steps = new HashMap<>();

  public int size() {
    return steps.size();
  }

  public List<Step> list() {
    return new ArrayList<>(steps.keySet());
  }

  public List<Map.Entry<Step,StepStatus>> remaining() {
    return steps.entrySet().stream()
        .filter(e-> e.getValue().ordinal() < SENT.ordinal())
        .collect(Collectors.toList());
  }
}
