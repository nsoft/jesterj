package org.jesterj.ingest.trie;

import junit.framework.Test;
import org.apache.commons.collections4.OrderedMap;

/**
 * JUnit test of the OrderedMap interface of a PatriciaTrie.
 *
 * @since 4.0
 */
public class PatriciaTrie2Test<V> extends AbstractOrderedMapTest<CharSequence, V> {

  public PatriciaTrie2Test(final String testName) {
    super(testName);
  }

  public static Test suite() {
    return BulkTest.makeSuite(PatriciaTrie2Test.class);
  }

  @Override
  public OrderedMap<CharSequence, V> makeObject() {
    return new PatriciaTrie<>();
  }

  @Override
  public boolean isAllowNullKey() {
    return false;
  }

  //-----------------------------------------------------------------------

  @Override
  public String getCompatibilityVersion() {
    return "4";
  }

}