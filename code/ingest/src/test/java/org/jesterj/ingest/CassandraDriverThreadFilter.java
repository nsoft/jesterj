package org.jesterj.ingest;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import org.apache.lucene.search.TimeLimitingCollector.TimerThread;

public class CassandraDriverThreadFilter implements ThreadFilter {

  @Override
  public boolean reject(Thread t) {
    String threadName = t.getName();
    if (threadName.matches("s\\d-io-\\d+")) {
      return true;
    }
    return false;
  }
}
