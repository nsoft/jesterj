/*
 * Copyright 2014 Needham Software LLC
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

package org.solrsystem.ingest.model.impl;

import net.jini.space.JavaSpace;
import org.solrsystem.ingest.model.Item;
import org.solrsystem.ingest.model.ItemProcessor;
import org.solrsystem.ingest.model.Step;

import java.util.Collection;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 9/28/14
 */
public class StepImpl extends Thread implements Step {

  LinkedBlockingQueue<Item> queue;
  AtomicInteger itemCount = new AtomicInteger(0);
  private int batchSize; // no concurrency by default
  private Step nextStep;
  private volatile boolean active;

  StepImpl(int batchSize, Step nextStep) {
    this.batchSize = batchSize;
    this.queue = new LinkedBlockingQueue<>(batchSize);
    this.nextStep = nextStep;
    this.setDaemon(true);
  }

  public Spliterator<Item> spliterator() {
    return queue.spliterator();
  }

  public boolean isEmpty() {
    return queue.isEmpty();
  }

  public Item element() {
    return queue.element();
  }

  public Item poll(long timeout, TimeUnit unit) throws InterruptedException {
    return queue.poll(timeout, unit);
  }

  public Stream<Item> parallelStream() {
    return queue.parallelStream();
  }

  public Item take() throws InterruptedException {
    return queue.take();
  }

  public void clear() {
    queue.clear();
  }

  public Iterator<Item> iterator() {
    return queue.iterator();
  }

  public boolean containsAll(Collection<?> c) {
    throw new UnsupportedOperationException("bulk operations supported for steps");
  }

  public <T> T[] toArray(T[] a) {
    //noinspection SuspiciousToArrayCall
    return queue.toArray(a);
  }

  public boolean addAll(Collection<? extends Item> c) {
    throw new UnsupportedOperationException("bulk operations supported for steps");
  }

  public int remainingCapacity() {
    return queue.remainingCapacity();
  }

  public Stream<Item> stream() {
    return queue.stream();
  }

  public boolean offer(Item item, long timeout, TimeUnit unit) throws InterruptedException {
    return queue.offer(item, timeout, unit);
  }

  public boolean offer(Item item) {
    return queue.offer(item);
  }

  public Item poll() {
    return queue.poll();
  }

  public int drainTo(Collection<? super Item> c, int maxElements) {
    return queue.drainTo(c, maxElements);
  }

  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException("bulk operations supported for steps");
  }

  public void put(Item item) throws InterruptedException {
    queue.put(item);
  }

  public Item peek() {
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

  public boolean add(Item item) {

    synchronized (item) {
      boolean add = queue.add(item);
      item.setQueueEntryNumber(itemCount.incrementAndGet());
      return add;
    }
  }

  public void forEach(Consumer<? super Item> action) {
    queue.forEach(action);
  }

  public Item remove() {
    return queue.remove();
  }

  public Object[] toArray() {
    return queue.toArray();
  }

  public boolean removeIf(Predicate<? super Item> filter) {
    return queue.removeIf(filter);
  }

  public int drainTo(Collection<? super Item> c) {
    return queue.drainTo(c);
  }

  @Override
  public int getBatchSize() {
    return batchSize;
  }

  @Override
  public Step next() {
    return this.nextStep;
  }


  @Override
  public void setInputJavaSpace(JavaSpace space) {

  }

  @Override
  public void setOutputJavaSpace(JavaSpace space) {

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

  }

  @Override
  public void deactivate() {

  }

  @Override
  public void run() {

    parallelStream().forEach(new ItemConsumer());

    while (!this.active) {
      try {
        if (!active) {
          Thread.sleep(500);
        }
      } catch (InterruptedException e) {
        // ignore
      }
    }
  }

  private static class ItemConsumer implements Consumer<Item> {
    public ItemProcessor processor;

    @Override
    public void accept(Item item) {
      try {
        this.processor.processItem(item);
        pushToNextIfOk(item);

      } catch (Error e) {
        throw e;
      } catch (Throwable t) {
        t.printStackTrace();
      }
    }

    private void pushToNextIfOk(Item item) {
      item.getSource().updateStatus(item, this.processor);
    }
  }
}
