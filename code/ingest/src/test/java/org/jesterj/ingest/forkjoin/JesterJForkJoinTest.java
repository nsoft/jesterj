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

package org.jesterj.ingest.forkjoin;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 9/12/16
 */
public class JesterJForkJoinTest {

  @Test
  public void testForkJoin() throws ExecutionException, InterruptedException {
    List<String> data = new ArrayList<>();
    data.add("foo");
    data.add("bar");
    data.add("baz");

    Thread thread = Thread.currentThread();
    ForkJoinPool forkJoinPool = new ForkJoinPool(2);
    forkJoinPool.submit(() ->
        //parallel task here, for example
        data.stream().parallel().map(new Function<String, Object>() {
          @Override
          public Object apply(String s) {
            assertFalse(Thread.currentThread().getClass() == NotSoInnocuousWorkerThread.class);
            return s.toUpperCase();
          }
        }).collect(toList())
    ).get();
    ForkJoinPool forkJoinPool2 = new ForkJoinPool(4, new JesterJForkJoinThreadFactory(), null, true);
    forkJoinPool2.submit(() ->
        //parallel task here, for example
        data.stream().parallel().map(new Function<String, Object>() {
          @Override
          public Object apply(String s) {
            Class<? extends Thread> threadClass = Thread.currentThread().getClass();
            System.out.println(threadClass);
            assertTrue(threadClass == NotSoInnocuousWorkerThread.class);
            return s.toUpperCase();
          }
        }).collect(toList())
    ).get();
  }
}
