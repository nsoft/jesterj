package org.jesterj.ingest.model;

/**
 * Processors that are aware of the name of the step holding them. This is only useful for logging, or for setting
 * statuses from potent/idempotent steps. Note that stetting a status from a safe step will just waste
 * space in our persistence layer with no functional effect.
 */
public interface StepNameAware {
  void setStepName(String stepName);

  String getStepName();

}
