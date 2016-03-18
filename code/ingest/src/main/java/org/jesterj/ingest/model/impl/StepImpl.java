/*
 * Copyright 2016 Needham Software LLC
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
import org.jesterj.ingest.logging.JesterJAppender;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.model.Router;
import org.jesterj.ingest.model.Status;
import org.jesterj.ingest.model.Step;
import org.jesterj.ingest.processors.DefaultWarningProcessor;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
 * processing logic write a class that implements <tt>DocumentProcessor</tt> and then build a stepImpl that uses
 * an instance of your processor. Also note that one does not normally call build on a StepImpl or any of it's
 * subclasses. The builder for this class is provided to a {@link org.jesterj.ingest.model.impl.PlanImpl.Builder} so
 * that the plan can validate the ordering of the steps and assemble the entire plan as an immutable DAG.
 */
public class StepImpl extends Thread implements Step {

  private static final Logger log = LogManager.getLogger();

  private LinkedBlockingQueue<Document> queue;
  private int batchSize; // no concurrency by default
  private LinkedHashMap<String, Step> nextSteps = new LinkedHashMap<>();
  private volatile boolean active;
  private JavaSpace outputSpace;
  private JavaSpace inputSpace;
  private String stepName;
  private Router router = new DefaultStepNameRouter();
  private DocumentProcessor processor = new DefaultWarningProcessor();

  StepImpl() {

    if (this.queue == null) {
      this.queue = new LinkedBlockingQueue<>(batchSize > 0 ? batchSize : 50);
    }
    this.setDaemon(true);

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
    throw new UnsupportedOperationException("bulk operations supported for steps");
  }

  public <T> T[] toArray(T[] a) {
    //noinspection SuspiciousToArrayCall
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
    return queue.offer(document);
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

  public void put(Document document) throws InterruptedException {
    queue.put(document);
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
  public Step getNext(Document doc) {
    if (nextSteps.size() == 0) return null;
    if (nextSteps.size() == 1) return nextSteps.values().iterator().next();
    return router.route(doc, nextSteps);
  }

  @Override
  public Step[] getSubsequentSteps() {
    //TODO: Something that isn't brain dead
    return new Step[0];
  }

  @Override
  public Plan getPlan() {
    return null;
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
  public void activate() {
    this.active = true;
  }

  @Override
  public void deactivate() {
    this.active = false;
  }

  @Override
  public boolean isActive() {
    return this.active;
  }

  @Override
  public void sendToNext(Document doc) {
    pushToNextIfOk(doc);
  }

  protected final void pushToNextIfOk(Document document) {
    if (document.getStatus() == Status.PROCESSING) {
      log.info(Status.PROCESSING.getMarker(), "{} finished processing {}", getStepName(), document.getId());
      Step next = getNext(document);
      if (this.outputSpace == null) {
        // local processing is our only option, do blocking put.
        try {
          next.put(document);
        } catch (InterruptedException e) {
          String message = "Exception while offering to " + next.getStepName();
          reportException(document, e, message);
        }
      } else {
        if (this.isFinalHelper()) {
          // remote processing is our only option.
          log.debug("todo: send to JavaSpace");
          // todo: put in JavaSpace
        } else {
          // Try to process this item locally first with a non-blocking add, and
          // if the getNext step is bogged down send it out for processing by helpers.
          try {
            next.add(document);
          } catch (IllegalStateException e) {
            log.debug("todo: send to JavaSpace");
            // todo: put in JavaSpace
          }
        }
      }
    }
  }

  @Override
  public void run() {

    //noinspection InfiniteLoopStatement
    while (true) {
      while (!this.active) {
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          // ignore
        }
      }
      if (queue.peek() != null) {
        parallelStream().forEach(new ItemConsumer());
      } else {
        try {
          // if we don't have work make sure we let others do their work.
          Thread.sleep(25);
        } catch (InterruptedException e) {
          // ignore
        }
      }
    }
  }

  @Override
  public String getStepName() {
    return stepName;
  }

  protected Logger getLogger() {
    return log;
  }

  protected void reportException(Document doc, Exception e, String message) {
    try {
      ThreadContext.put(JesterJAppender.JJ_INGEST_DOCID, doc.getId());
      StringWriter buff = new StringWriter();
      e.printStackTrace(new PrintWriter(buff));
      getLogger().error(Status.ERROR.getMarker(), message + e.getMessage() + "\n" + buff.toString());
    } finally {
      ThreadContext.clearAll();
    }
  }

  public JavaSpace getInputSpace() {
    return inputSpace;
  }

  private class ItemConsumer implements Consumer<Document> {

    @Override
    public void accept(Document document) {
      try {
        for (Document documentResult : StepImpl.this.processor.processDocument(document)) {
          pushToNextIfOk(documentResult);
        }
      } catch (Error e) {
        throw e;
      } catch (Throwable t) {
        t.printStackTrace();
      }
    }

  }

  public static class Builder {

    private StepImpl obj;

    public Builder() {
      if (whoAmI() == this.getClass()) {
        obj = new StepImpl();
      }
    }

    private Class whoAmI() {
      return new Object() {
      }.getClass().getEnclosingMethod().getDeclaringClass();
    }

    protected StepImpl getObject() {
      return obj;
    }

    public Builder batchSize(int size) {
      getObject().batchSize = size;
      getObject().queue = new LinkedBlockingQueue<>(size);
      return this;
    }


    public Builder outputSpace(JavaSpace outputSpace) {
      getObject().outputSpace = outputSpace;
      return this;
    }

    public Builder inputSpace(JavaSpace inputSpace) {
      getObject().inputSpace = inputSpace;
      return this;
    }

    public Builder stepName(String stepName) {
      getObject().stepName = stepName;
      return this;
    }

    public Builder routingBy(Router router) {
      getObject().router = router;
      return this;
    }

    public Builder withProcessor(DocumentProcessor processor) {
      getObject().processor = processor;
      return this;
    }

    /**
     * Used when assembling steps into a plan
     *
     * @return the name of the step
     */
    public String getStepName() {
      return getObject().stepName;
    }

    private void setObj(StepImpl obj) {
      this.obj = obj;
    }

    /**
     * Should only be called by a PlanImpl
     *
     * @return the immutable step instance.
     */
    StepImpl build() {
      StepImpl object = getObject();
      setObj(new StepImpl());
      return object;
    }

    /**
     * should only be used by PlanImpl
     *
     * @param step a fully built step
     */
    void addNextStep(Step step) {
      getObject().nextSteps.put(step.getStepName(), step);
    }
  }


}
