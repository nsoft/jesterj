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

import net.jini.space.JavaSpace;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.appender.AppenderLoggingException;
import org.jesterj.ingest.Main;
import org.jesterj.ingest.config.Transient;
import org.jesterj.ingest.logging.JesterJAppender;
import org.jesterj.ingest.model.ConfiguredBuildable;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.NextSteps;
import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.model.Router;
import org.jesterj.ingest.model.Status;
import org.jesterj.ingest.model.Step;
import org.jesterj.ingest.processors.DefaultWarningProcessor;
import org.jesterj.ingest.routers.RouteByStepName;
import org.jesterj.ingest.routers.RouterBase;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
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
 * an instance of your processor. Also note that one does not normally call build on a StepImpl or any of it's
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
  private JavaSpace outputSpace;
  private JavaSpace inputSpace;
  private String stepName;
  private Router router = new RouteByStepName();
  private volatile DocumentProcessor processor = new DefaultWarningProcessor();
  private volatile Thread worker;
  private final Object WORKER_LOCK = new Object();
  private Plan plan;

  private final List<Runnable> deferred = new ArrayList<>();
  private final Object potentStepListLock = new Object();
  private volatile Step[] potentSteps;
  private int shutdownTimeout = 1000;
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
    return queue.offer(document, timeout, unit);
  }

  public boolean offer(Document document) {
    if (active) {
      return queue.offer(document);
    } else {
      reportDocStatus(Status.ERROR, document, "Destination step was shutting down ({})", getName());
      return false;
    }
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
    if (active) {
      queue.put(document);
    } else {
      reportDocStatus(Status.ERROR, document, "Destination step was shutting down ({})", getName());
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
    if (nextSteps.size() == 0) return null;
    if (nextSteps.size() == 1) return new NextSteps(doc, nextSteps.values().iterator().next());
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
  public void advertise() {

  }

  @Override
  public void stopAdvertising() {

  }

  @Override
  public void acceptJiniRequests() {

  }

  @Override
  public void denyJiniRequests() {

  }

  @Override
  public boolean readyForJiniRequests() {
    return false;
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
  }

  @Override
  public synchronized void deactivate() {
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
    if (processor != null) {
      processor.close();
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
  public Step[] getDownstreamPotentSteps() {
    if (this.potentSteps == null) {
      synchronized (potentStepListLock) {
        if (this.potentSteps == null) {
          if (nextSteps.isEmpty()) {
            if (processor.hasExternalSideEffects()) {
              potentSteps = new Step[]{this};
            } else {
              // oddball case! but shoudlnt
              potentSteps = new Step[0];
            }
          } else {
            Step[][] subEffects = new Step[nextSteps.size()][];
            ArrayList<Step> values = new ArrayList<>(nextSteps.values());
            for (int i = 0; i < values.size(); i++) {
              subEffects[i] = values.get(i).getDownstreamPotentSteps();
            }
            ArrayList<Step> tmp = new ArrayList<>();
            for (Step[] subEffect : subEffects) {
              Collections.addAll(tmp, subEffect);
            }
            if (processor.hasExternalSideEffects()) {
              tmp.add(this);
            }
            potentSteps = tmp.toArray(new Step[0]);
          }
        }
      }
    }
    return potentSteps;
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

  private void pushToNextIfOk(Document document) {
    log.trace("starting push to next if ok {} for {}", getName(), document.getId());
    if (document.getStatus() == Status.PROCESSING) {
      log.trace("{} is ok, will push to next step(s)", getName());
      reportDocStatus(Status.PROCESSING, document, "{} finished processing in {}", getName(), document.getId());
      NextSteps next = getNextSteps(document);
      if (next == null) {
        if (getNextSteps().size() == 0) {
          reportDocStatus(Status.INDEXED, document, "Terminal Step {} OK", getName());
        } else {
          reportDocStatus(Status.DROPPED, document, "No qualifying next step found by {} in {}", router, getName());
        }
        return;
      }

      // if we get here there are steps down stream of this one, do distribution.
      List<Map.Entry<Step, NextSteps.StepStatus>> remaining = next.remaining();
      if (remaining.size() == 1) {
        // simple case, do not make/use clones
        pushToStep(document,remaining.get(0).getKey(),true);
      } else {
        while (remaining.size() > 0) {
          for (Map.Entry<Step, NextSteps.StepStatus> step : next.remaining()) {
            Step destinationStep = step.getKey();
            if (step.getValue().exception != null) {
              next.update(destinationStep, NextSteps.StepStatus.FAIL); // update this first to ensure loop terminates
              reportException(document, step.getValue().exception, "Failed to clone document when sending to multiple steps");
            } else {
              NextSteps.StepStatus stepStatus = pushToStep(step.getValue().doc, destinationStep, false);
              next.update(destinationStep, stepStatus);
            }
          }
          remaining = next.remaining();
        }
      }
    } else {
      log.trace("Dropping {}", document.getId());
      reportDocStatus(document.getStatus(), document,
          "Document processing for {} terminated ({}) after {}", document.getId(), document.getStatus(), getName());
    }
    log.trace("completing push to next if ok {} for {}", getName(), document.getId());
  }

  private NextSteps.StepStatus pushToStep(Document document, Step step, boolean block) {
    if (step != null) {
      if (this.outputSpace == null) { // placeholder if always true for now
        boolean offer;
        // local processing is our only option, do blocking put.
        log.trace("starting put ( {} into {} )", getName(), step.getName());
        if (block) {
          try {
            step.put(document);
            log.trace("completed put ( {} into {} )", getName(), step.getName());
          } catch (InterruptedException e) {
            String message = "Exception while offering to " + step.getName();
            reportException(document, e, message);
            return NextSteps.StepStatus.FAIL;
          }
          offer = true;
        } else {
          offer = step.offer(document);
        }
        return offer ? NextSteps.StepStatus.SENT : NextSteps.StepStatus.RETRY;
      } else {
        log.error("This code path (javaspaces) not yet supported");
        System.err.println("This code path (java spaces) not yet supported");
        System.exit(99);
//      if (this.isFinalHelper)) {
//        // remote processing is our only option.
//        log.debug("todo: send to JavaSpace");
//        // todo: put in JavaSpace
//      } else {
//        // Try to process this item locally first with a non-blocking add, and
//        // if the getNext step is bogged down send it out for processing by helpers.
//        try {
//          step.add(document);
//        } catch (IllegalStateException e) {
//          log.debug("todo: send to JavaSpace");
//          // todo: put in JavaSpace
//        }
//      }
      }
    }
    throw new RuntimeException("Attempted to route to a null step");
  }

  private void reportDocStatus(Status status, Document document, String message, Object... messageParams) {
    try {
      ThreadContext.put(JesterJAppender.JJ_INGEST_DOCID, document.getId());
      ThreadContext.put(JesterJAppender.JJ_INGEST_SOURCE_SCANNER, document.getSourceScannerName());
      document.setStatus(status);
      log.info(status.getMarker(), message, messageParams);
    } catch (AppenderLoggingException e) {
      if (Main.isNotShuttingDown()) {
        log.error("Could not contact our internal Cassandra!!!", e);
      }
    } finally {
      ThreadContext.clearAll();
    }
  }

  @Override
  public void run() {
    try {
      while (this.active) {
        try {
          log.trace("active: {}", getName());
          Document document = queue.take();
          log.trace("{} took {} from queue", getName(), document.getId());
          documentConsumer.accept(document);
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
      System.exit(2);
    }
  }

  @Override
  public String getName() {
    return stepName;
  }

  protected Logger getLogger() {
    return log;
  }

  @SuppressWarnings("WeakerAccess")
  protected void reportException(Document doc, Exception e, String message, Object... params) {
    StringWriter buff = new StringWriter();
    e.printStackTrace(new PrintWriter(buff));
    String errorMsg = message + " " + e.getMessage() + "\n" + buff;
    reportDocStatus(Status.ERROR, doc, errorMsg, params);
    if (e instanceof InterruptedException) {
      log.debug("Step interrupted!", e);
    } else {
      log.error("Step Exception!", e);
    }
  }

  @SuppressWarnings("unused")
  public JavaSpace getInputSpace() {
    return inputSpace;
  }

  public void executeDeferred() {
    deferred.forEach(Runnable::run);
  }

  @Override
  public void addDeferred(Runnable builderAction) {
    deferred.add(builderAction);
  }


  private class DocumentConsumer implements Consumer<Document> {

    @Override
    public void accept(Document document) {
      try {
        log.trace("DOC CONSUMER START");
        // by definition these statuses should never be processed.
        if (document.getStatus() == Status.ERROR ||
            document.getStatus() == Status.DROPPED ||
            document.getStatus() == Status.DEAD) {
          log.fatal("ATTEMPTED TO CONSUME {}} DOCUMENT!!", document.getStatus());
          log.fatal("offending doc:{}", document.getId());
          log.fatal(new RuntimeException("Bad Doc Status:" + document.getStatus()));
          Thread.dumpStack();
          System.exit(9999);
        }
        log.trace("accepting {}, sending to {} in {}", document.getId(),
            (StepImpl.this.processor == null) ? "null" : StepImpl.this.processor.getName(),
            StepImpl.this.getName());
        Document[] documents = StepImpl.this.processor.processDocument(document);
        log.trace("finished {}, was sent to {} in {}", document.getId(),
            (StepImpl.this.processor == null) ? "null" : StepImpl.this.processor.getName(),
            StepImpl.this.getName());
        for (Document documentResult : documents) {
          pushToNextIfOk(documentResult);
        }
      } catch (Exception e) {
        StepImpl.this.reportException(document, e, e.getMessage());
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


    @SuppressWarnings("UnusedReturnValue")
    public Builder outputSpace(JavaSpace outputSpace) {
      getObj().outputSpace = outputSpace;
      return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    public Builder inputSpace(JavaSpace inputSpace) {
      getObj().inputSpace = inputSpace;
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
      StepImpl currObj = getObj(); // make sure that this cant' change after build() called.
      getObj().addDeferred(() -> currObj.processor = processor.build());
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
      obj = new StepImpl(); // subclasses such as scanners will mask this with thier own obj field which is ok.
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
