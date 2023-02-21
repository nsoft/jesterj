/*
 * Copyright 2014-2016 Needham Software LLC
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

package org.jesterj.ingest.model.impl;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.jesterj.ingest.config.Transient;
import org.jesterj.ingest.model.*;
import org.jesterj.ingest.processors.NoOpProcessor;
import org.jesterj.ingest.routers.RouteByStepName;
import org.jesterj.ingest.routers.RouterBase;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 9/28/14
 */

/**
 * The class that is used to run {@link DocumentProcessor}s. This class takes care of the handling of the document
 * ensures it is properly received and passed on. This class is not normally overridden, to implement custom
 * processing logic write a class that implements <code>DocumentProcessor</code> and then build a stepImpl that uses
 * an instance of your processor. Also note that one does not normally call build on a StepImpl or any of its
 * subclasses. The builder for this class is provided to a {@link org.jesterj.ingest.model.impl.PlanImpl.Builder} so
 * that the plan can validate the ordering of the steps and assemble the entire plan as an immutable DAG.
 */
public class StepImpl implements Step {

  private static final Logger log = LogManager.getLogger();

  DocumentConsumer documentConsumer = new DocumentConsumer(); // stateless bean
  private LinkedBlockingQueue<Document> queue;
  private int batchSize; // no concurrency by default
  private final LinkedHashMap<String, Step> nextSteps = new LinkedHashMap<>();
  private volatile boolean active;
  private String stepName;
  private Router router = new RouteByStepName();
  private volatile DocumentProcessor processor = new NoOpProcessor();
  private volatile Thread worker;
  private final Object WORKER_LOCK = new Object();
  private Plan plan;

  private final List<Runnable> deferred = new ArrayList<>();
  private final Object OUTPUT_STEP_LIST_LOCK = new Object();
  private volatile Step[] outputSteps;
  private int shutdownTimeout = 100;
  private final List<Step> priorSteps = new ArrayList<>();

  StepImpl() {
  }

  public Spliterator<Document> spliterator() {
    return queue.spliterator();
  }

  public boolean isEmpty() {
    return queue.isEmpty();
  }

  public Document element() {
    return queue.element();
  }

  public Document poll(long timeout, TimeUnit unit) throws InterruptedException {
    return queue.poll(timeout, unit);
  }

  public Stream<Document> parallelStream() {
    return queue.parallelStream();
  }

  public Document take() throws InterruptedException {
    return queue.take();
  }

  public void clear() {
    queue.clear();
  }

  public Iterator<Document> iterator() {
    return queue.iterator();
  }

  public boolean containsAll(Collection<?> c) {
    throw new UnsupportedOperationException("bulk operations not supported for steps");
  }

  public <T> T[] toArray(T[] a) {
    return queue.toArray(a);
  }

  public boolean addAll(Collection<? extends Document> c) {
    throw new UnsupportedOperationException("bulk operations supported for steps");
  }

  public int remainingCapacity() {
    return queue.remainingCapacity();
  }

  public Stream<Document> stream() {
    return queue.stream();
  }

  public boolean offer(Document document, long timeout, TimeUnit unit) throws InterruptedException {
    log.trace("{} offered (timeout) to {} at {}", document::getId, this::getName, () -> Arrays.asList(new RuntimeException().getStackTrace()).toString().replaceAll(",", "\n"));
    return queue.offer(document, timeout, unit);
  }

  public boolean offer(Document document) {
    if (active) {
      log.trace("{} offered to {} at {}", document::getId, this::getName, () -> Arrays.asList(new RuntimeException().getStackTrace()).toString().replaceAll(",", "\n"));
      return queue.offer(document);
    }
    return false;
  }

  public Document poll() {
    return queue.poll();
  }

  public int drainTo(Collection<? super Document> c, int maxElements) {
    return queue.drainTo(c, maxElements);
  }

  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException("bulk operations supported for steps");
  }

  /**
   * Attempt to send the document to this step blocking if the queue for this step
   * is full. This method does NOT guarantee delivery however, and will return
   * immediately if the destination step is shutting down.
   *
   * @param document the element to add
   * @throws InterruptedException if interrupted while waiting
   */
  public void put(Document document) throws InterruptedException {
    log.trace("{} put to {} at {}", document::getId, this::getName,
        () -> Arrays.asList(new RuntimeException().getStackTrace()).toString().replaceAll(",", "\n"));
    if (active) {
      queue.put(document);
    }
  }

  public Document peek() {
    return queue.peek();
  }

  public int size() {
    return queue.size();
  }

  public boolean contains(Object o) {
    return queue.contains(o);
  }

  public boolean remove(Object o) {
    return queue.remove(o);
  }

  public boolean removeAll(Collection<?> c) {
    return queue.removeAll(c);
  }

  public boolean add(Document document) {
    log.trace("{} added to {} at {}", document::getId, this::getName, () -> Arrays.asList(new RuntimeException().getStackTrace()).toString().replaceAll(",", "\n"));
    return queue.add(document);
  }

  public void forEach(Consumer<? super Document> action) {
    queue.forEach(action);
  }

  public Document remove() {
    return queue.remove();
  }

  public Object[] toArray() {
    return queue.toArray();
  }

  public boolean removeIf(Predicate<? super Document> filter) {
    return queue.removeIf(filter);
  }

  public int drainTo(Collection<? super Document> c) {
    return queue.drainTo(c);
  }

  @Override
  public int getBatchSize() {
    return batchSize;
  }

  @Override
  public NextSteps getNextSteps(Document doc) {
    if (nextSteps.size() == 0) {
      log.trace("No next steps for {} from {}", doc.getId(), getName());
      return null;
    }
    if (nextSteps.size() == 1) {
      log.trace("Single next step {} for {} from {}", () -> getNextSteps().keySet(), doc::getId, this::getName);
      return new NextSteps(doc, nextSteps.values().iterator().next());
    }
    log.trace("Routing among next steps {} for {}({}) from {} ", () -> getNextSteps().keySet(), doc::getId, doc::getOrigination, this::getName);
    return router.route(doc);
  }


  @Override
  public Plan getPlan() {
    return this.plan;
  }

  /**
   * Only to be used in PlanImpl
   *
   * @param plan the plan to which this step is attached
   */
  void setPlan(Plan plan) {
    this.plan = plan;
  }

  @Override
  public synchronized void activate() {
    log.info("Starting {} ", getName());
    if (worker == null || !worker.isAlive()) {
      synchronized (WORKER_LOCK) {
        log.info("Starting new thread for {} ", getName());
        worker = new Thread(this);
        worker.setName("jj-worker-" + this.stepName + "-" + System.currentTimeMillis());
        worker.setDaemon(true);
        this.active = true;
        worker.start();
        log.info("started {} ({})", worker.getName(), worker.getId());
      }
    }
    log.info("Started step {} ", getName());
  }

  @Override
  public synchronized void deactivate() {
    log.info("Deactivating step {}", getName());
    this.active = false;
    this.queue.clear();
    // make this method idempotent so that it can be called any number of times without NPE, and can be
    // called by the joined thread  without getting stuck in a join/interrupt loop.
    if (worker != null) {
      Thread workerShuttingDown;
      synchronized (WORKER_LOCK) {
        workerShuttingDown = worker;
        worker = null;
      }
      if (workerShuttingDown != null) {
        try {
          workerShuttingDown.join(shutdownTimeout);
          if (workerShuttingDown.isAlive()) {
            log.warn("{} was slow shutting down, interrupting..", getName());
            workerShuttingDown.interrupt();
          }
        } catch (InterruptedException e) {
          log.error("Thread on which shutdown was was interrupted while shutting down {}", getName());
        }
      }
    }
  }

  /**
   * Test if the step is active and should be processing. It is a good idea for operations running in the worker thread
   * to check this method in loops and before operations that could block or take a long time. Doing so promotes
   * timely shutdown.
   *
   * @return true if processing should continue false if the worker thread is trying to stop.
   */
  @Transient
  @Override
  public boolean isActive() {
    return this.active;
  }

  @Override
  public void sendToNext(Document doc) {
    pushToNextIfOk(doc);
  }

  @Override
  public Step[] geOutputSteps() {
    if (this.outputSteps == null) {
      synchronized (OUTPUT_STEP_LIST_LOCK) {
        if (this.outputSteps == null) {
          if (nextSteps.isEmpty()) {
            if (processor.isPotent()) {
              outputSteps = new Step[]{this};
            } else {
              // oddball case! but shouldn't
              outputSteps = new Step[0];
            }
          } else {
            Step[][] subEffects = new Step[nextSteps.size()][];
            ArrayList<Step> values = new ArrayList<>(nextSteps.values());
            for (int i = 0; i < values.size(); i++) {
              subEffects[i] = values.get(i).geOutputSteps();
            }
            ArrayList<Step> tmp = new ArrayList<>();
            for (Step[] subEffect : subEffects) {
              Collections.addAll(tmp, subEffect);
            }
            // since we fail if the last step is safe, this ensures that every path happens at least once.
            // idempotent steps not at the terminus don't need to be tracked, they will be guaranteed to be
            // executed en-route to a terminus. By definition, it's ok to execute them more than once.
            if (processor.isPotent() || processor.isIdempotent() && nextSteps.isEmpty()) {
              tmp.add(this);
            }
            outputSteps = tmp.toArray(new Step[0]);
          }
        }
      }
    }
    return outputSteps;
  }

  @Override
  public LinkedHashMap<String, Step> getNextSteps() {
    return nextSteps;
  }

  @Override
  public boolean isActivePriorSteps() {
    return getPriorSteps().stream().anyMatch(Step::isActive);
  }

  // visible for testing
  List<Step> getPriorSteps() {
    return priorSteps;
  }

  void pushToNextIfOk(Document document) {
    try {
      log.trace("starting push to next if ok {} for {}", getName(), document.getId());
      NextSteps next = getNextSteps(document);
      log.trace("Found {} next steps", next == null ? "(null)" : next.size());
      if (next == null) {

        // Here we check for sanity before declaring a document indexed by virtue of no remaining steps.

        if (!(this.getProcessor().isPotent() || this.getProcessor().isIdempotent())) {
          if (nextSteps.isEmpty()) {
            throw new RuntimeException("Your plan is misconfigured. you have dangling steps that have no external " +
                "outputs. The final step in each branch must be either POTENT or IDEMPOTENT. Note that a step that" +
                "increments a custom metric that can be externally observed somehow should be marked POTENT.");
          } else {
            throw new RuntimeException("Your router failed to select a destination. This is a bug in the router" +
                "implementation. If it is a standard JesterJ router, please report an issue in the project issue " +
                "tracker. Remaining incomplete steps:" + document.listIncompleteOutputSteps() + " Current Step:" +
                getName() + " Document:" + document);
          }
        }
        if (document.getIncompleteOutputSteps().length < 1 && nextSteps.isEmpty()) {
          throw new RuntimeException("Critical failure! No down stream step on Document on arrival at a final output " +
              "step. This is likely to be a bug in JesterJ, please report an issue in the project " +
              "issue tracker. Remaining incomplete steps:" + document.listIncompleteOutputSteps() + " Current Step:" +
              getName() + " Document:" + document);
        }
        if (document.getIncompleteOutputSteps().length > 1 && nextSteps.isEmpty()) {
          throw new RuntimeException("Critical failure! JesterJ calculated more than one down stream step " +
              "for a final step. This is likely to be a bug in JesterJ, please report an issue in the project " +
              "issue tracker. Remaining incomplete steps:" + document.listIncompleteOutputSteps() + " Current Step:" +
              getName() + " Document:" + document);
        }
        String incompleteOutputStep = document.getIncompleteOutputSteps()[0];
        if (!(getProcessor().isPotent() || getProcessor().isIdempotent())) {
          throw new RuntimeException("Somehow we have a destination output step, at the last step, but the last step" +
              "is not POTENT or IDEMPOTENT, or the name doesn't match the current step! Our Name:" + getName() +
              " Expected destination:" + incompleteOutputStep);
        }
        if (!incompleteOutputStep.equals(getName())) {
          throw new RuntimeException("We reached a valid final step, but it does not have the expected step name, " +
              "This is likely to be a bug in JesterJ please report an issue in the project issue tracker");
        }
        // we have a single step, we are the right type of step, and this is the expected step. Our work is done here!
        markIndexed(document, incompleteOutputStep);
      } else {
        // this is the case for non-terminal steps that have outputs.
        // todo: plan configuration option to allow the idempotent steps to be repeated if desired.
        if (this.getProcessor().isPotent() || this.getProcessor().isIdempotent()) {
          markIndexed(document, this.getName());
        }
        document.reportDocStatus();
        pushToNext(document, next);
      }
      document.reportDocStatus();
      log.trace("completing push to next if ok {} for {}", getName(), document.getId());
    } catch (
        Exception e) {
      // otherwise so very annoying to try to figure out which step is causing problems.
      log.error("Exception caught, exiting from step {}", getName());
      throw e;
    }
  }

  Router getRouter() {
    return this.router;
  }

  void markIndexed(Document document, String outputStep) {
    if (document.getStatus(outputStep) == Status.PROCESSING) {
      log.trace("{} finished processing {}, for {}", getName(), document.getId(), outputStep);
      document.setStatus(Status.INDEXED, outputStep, "Last available step {} completed OK,", getName());
    }
  }

  void pushToNext(Document document, NextSteps next) {
    List<Map.Entry<Step, NextSteps.StepStatusHolder>> remaining = next.remaining();
    if (remaining.size() == 1) {
      // simple case, do not make/use clones
      pushToStep(remaining.get(0), true);
    } else {
      // This loop allows us to push to any steps that are ready, and come back to the ones that are blocked.
      while (remaining.size() > 0) {
        for (Map.Entry<Step, NextSteps.StepStatusHolder> stepStatuEntry : next.remaining()) {
          Step destinationStep = stepStatuEntry.getKey();
          if (stepStatuEntry.getValue().getException() != null) {
            next.update(destinationStep, NextSteps.StepStatus.FAIL); // update this first to ensure loop terminates
            reportException(document, stepStatuEntry, "Failed to clone document when sending to multiple steps");
          } else {
            NextSteps.StepStatus stepStatus = pushToStep(stepStatuEntry, false);
            next.update(destinationStep, stepStatus);
          }
        }
        remaining = next.remaining();
      }
    }
  }

  private NextSteps.StepStatus pushToStep(Map.Entry<Step, NextSteps.StepStatusHolder> entry, boolean block) {
    Step step = entry.getKey();
    Document document = entry.getValue().getDoc();
    String name = step == null ? "null step name" : step.getName();
    log.trace("Pushing to {} DocId:{} Statuses:{}", name, document.getId(), document.dumpStatus());
    if (step != null) {
      boolean offer;
      // local processing is our only option, do blocking put.
      log.trace("starting put ( {} into {} )", getName(), name);
      if (block) {
        try {
          step.put(document);
          log.trace("completed put ( {} into {} )", getName(), name);
        } catch (InterruptedException e) {
          // This means the system is stopping and does not indicate an error with the document
          return NextSteps.StepStatus.FAIL;
        } catch (Exception e) {
          String message = "Exception while offering to " + name + ". Exception message:{}";
          reportException(document, entry, message, e);
          return NextSteps.StepStatus.FAIL;
        }
        offer = true;
      } else {
        offer = step.offer(document);
      }
      return offer ? NextSteps.StepStatus.SENT : NextSteps.StepStatus.RETRY;
    }
    throw new RuntimeException("Attempted to route to a null step");
  }

  @Override
  public void run() {
    addStepContext();
    try {
      while (this.active) {
        try {
          log.trace("active: {}", getName());
          Document document = queue.poll(10, TimeUnit.MILLISECONDS);
          if (document != null) {
            log.trace("{} took {} from queue", getName(), document.getId());
            boolean potent = this.getProcessor().isPotent();
            boolean thisStepNotRequired = !document.isPlanOutput(this.getName());
            if (potent && thisStepNotRequired) {
              // FTI determined that this step was not required, so skip processing
              log.trace("Skipping processing for {} at {}", document.getId(), getName());
              pushToNextIfOk(document);
              continue;
            }
            documentConsumer.accept(document);
          }
        } catch (InterruptedException e) {
          this.deactivate();
          break;
        }
      }
    } catch (Throwable t) {
      t.printStackTrace();
      log.error(t);
      System.out.println("Thread for " + getName() + " died. This should not happen and is always a bug in JesterJ " +
          "unless you killed the process with Ctrl-C or similar. This node is Shutting down. If the process was not " +
          "killed, and you got this message during normal running, please open a bug report at http://www.jesterj.org");
      System.out.flush();
      System.exit(2);
    }
  }

  void addStepContext() {
    ThreadContext.put(JJ_PLAN_NAME, getPlan().getName());
    ThreadContext.put(JJ_PLAN_VERSION, String.valueOf(getPlan().getVersion()));
  }

  void removeStepContext() {
    ThreadContext.remove(JJ_PLAN_NAME);
    ThreadContext.remove(JJ_PLAN_VERSION);
  }

  @Override
  public String getName() {
    return stepName;
  }

  protected Logger getLogger() {
    return log;
  }

  @SuppressWarnings("WeakerAccess")
  protected void reportException(Document doc, Map.Entry<Step, NextSteps.StepStatusHolder> entry, String message, Object... params) {
    StringWriter buff = new StringWriter();
    Exception e = entry.getValue().getException();
    e.printStackTrace(new PrintWriter(buff));
    String errorMsg = message + " " + e.getMessage() + "\n" + buff;
    List<String> downstreamForStep = Arrays.stream(entry.getKey().geOutputSteps())
        .map(Configurable::getName)
        .collect(Collectors.toList());

    // TODO: this fails if there's more than one path to the downstream step (diamond shaped plan). Will need to
    //  consider outputs that lead to a join point potent if the router may produce more than one output.
    //  The assumption will be that non-deterministic routers are choosing between equivalent paths and that
    //  any deterministic router that only emits 1 or 0 documents need not be tracked.
    for (String incompleteOutputStep : downstreamForStep) {
      doc.setStatus(Status.ERROR,incompleteOutputStep, errorMsg,  params);
    }
    doc.reportDocStatus();
    if (e instanceof InterruptedException) {
      log.debug("Step interrupted!", e);
    } else {
      log.error("Step Exception!", e);
    }
  }

  @SuppressWarnings("unused")
  public void executeDeferred() {
    deferred.forEach(Runnable::run);
  }

  @Override
  public void addDeferred(Runnable builderAction) {
    deferred.add(builderAction);
  }

  public DocumentProcessor getProcessor() {
    return this.processor;
  }


  private class DocumentConsumer implements Consumer<Document> {

    @Override
    public void accept(Document document) {
      Document[] documents;
      try {
        log.trace("DOC CONSUMER START");
        // by definition these statuses should never be processed.
        String[] incompleteOutputSteps = document.getIncompleteOutputSteps();
        for (String incompleteOutputStep : incompleteOutputSteps) {
          if (document.getStatus(incompleteOutputStep) == Status.ERROR ||
              document.getStatus(incompleteOutputStep) == Status.DROPPED ||
              document.getStatus(incompleteOutputStep) == Status.DEAD) {
            log.fatal("ATTEMPTED TO CONSUME {}} DOCUMENT!!", document.getStatus(incompleteOutputStep));
            log.fatal("offending doc:{}", document.getId());
            log.fatal("This is a bug in JesterJ");
            log.fatal(new RuntimeException("Bad Doc Status:" + document.getStatus(incompleteOutputStep)));
            Thread.dumpStack();
            System.exit(9999);
          }
        }

        String p1 = (StepImpl.this.processor == null) ? "null" : StepImpl.this.processor.getName();
        log.trace("accepting {}({}), sending to {} in {}", document.getId(), document.getOrigination(), p1, StepImpl.this.getName());
        documents = StepImpl.this.processor.processDocument(document);
        log.trace("finished {}({}), was sent to {} in {}", document.getId(), document.getOrigination(), p1, StepImpl.this.getName());
      } catch (Exception e) {
        log.warn("Exception processing step", e);
        for (String cantReach : document.getIncompleteOutputSteps()) {
          document.setStatus(Status.ERROR, cantReach, "Exception while processing document in {}. Message:{}", getName(), e.getMessage());
        }
        document.reportDocStatus();
        return;
      }
      if (documents != null) {
        for (Document documentResult : documents) {
          pushToNextIfOk(documentResult);
        }
      }
      log.trace("DOC CONSUMER END");
    }
  }

  public static class Builder extends NamedBuilder<StepImpl> {

    private StepImpl obj;

    public Builder() {
      if (whoAmI() == this.getClass()) {
        obj = new StepImpl();
      }
    }

    @SuppressWarnings("rawtypes")
    private Class whoAmI() {
      return new Object() {
      }.getClass().getEnclosingMethod().getDeclaringClass();
    }

    protected StepImpl getObj() {
      return obj;
    }

    public Builder batchSize(int size) {
      getObj().batchSize = size;
      getObj().queue = new LinkedBlockingQueue<>(size);
      return this;
    }

    public Builder named(String stepName) {
      getObj().stepName = stepName;
      return this;
    }

    public Builder withShutdownWait(int millis) {
      getObj().shutdownTimeout = millis;
      return this;
    }

    public Builder routingBy(RouterBase.Builder<? extends Router> router) {
      StepImpl currObj = getObj(); // make sure that this cant change after build() called.
      getObj().addDeferred(() -> currObj.router = router.forStep(getObj()).build());
      return this;
    }

    public Builder withProcessor(ConfiguredBuildable<? extends DocumentProcessor> processor) {
      StepImpl currObj = getObj(); // make sure that this can't change after build() called.
      getObj().addDeferred(() -> {
        currObj.processor = processor.build();
        if (currObj.processor instanceof StepNameAware) {
          ((StepNameAware) currObj.processor).setStepName(currObj.getName());
        }
      });
      return this;
    }


    /**
     * Used when assembling steps into a plan
     *
     * @return the name of the step
     */
    public String getStepName() {
      return getObj().stepName;
    }

    /**
     * Should only be called by a PlanImpl
     *
     * @return the immutable step instance.
     */
    public StepImpl build() {
      StepImpl object = getObj(); // if subclassed we want subclass not our obj. This is intentional
      object.executeDeferred();
      int batchSize = object.batchSize;
      object.queue = new LinkedBlockingQueue<>(batchSize > 0 ? batchSize : 50);
      obj = new StepImpl(); // subclasses such as scanners will mask this with their own obj field which is ok.
      return object;
    }

    /**
     * should only be used by PlanImpl
     *
     * @param step a fully built step
     */
    void addNextStep(Step step) {
      step.addPredecessor(getObj());
      getObj().nextSteps.put(step.getName(), step);
    }
  }

  @Override
  public void addPredecessor(StepImpl obj) {
    this.priorSteps.add(obj);
  }

  @Override
  public String toString() {
    return getName();
  }
}
