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

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 3/20/16
 */

/**
 * A worker thread without all the crazy restrictions of {@link InnocuousForkJoinWorkerThread}. This
 * thread should be a normal thread with normal permissions just like the rest of the application.
 */
public class NotSoInnocuousWorkerThread extends ForkJoinWorkerThread {
  /**
   * Creates a ForkJoinWorkerThread operating in the given pool.
   *
   * @param pool the pool this thread works in
   * @throws NullPointerException if pool is null
   */
  protected NotSoInnocuousWorkerThread(ForkJoinPool pool) {
    super(pool);
  }
}
