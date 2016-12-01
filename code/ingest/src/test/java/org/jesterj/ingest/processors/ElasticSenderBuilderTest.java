package org.jesterj.ingest.processors;


import org.junit.Test;

import java.net.UnknownHostException;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

public class ElasticSenderBuilderTest {

  @Test
  public void testIsValid() {
    ElasticSender.Builder b = new ElasticSender.Builder();
    assertFalse(b.isValid());
    b.forIndex("foo");
    assertFalse(b.isValid());
    b.forObjectType("bar");
    assertFalse(b.isValid());
    b.withServer("example.com","1234");
    assertFalse(b.isValid());
    b.named("foobar");
    assertTrue(b.isValid());
    b.withServer("example.com", "ABC");
    assertFalse(b.isValid());
  }

  @Test
  public void testBuild() {
    ElasticSender.Builder b = new ElasticSender.Builder();
    assertFalse(b.isValid());
    b.forIndex("foo");
    assertFalse(b.isValid());
    b.forObjectType("bar");
    assertFalse(b.isValid());
    b.withServer("example.com","1234");
    assertFalse(b.isValid());
    b.named("foobar");
    assertTrue(b.isValid());
    try {
      b.build();
    } catch (RuntimeException e) {
      assertTrue(e.getCause() instanceof UnknownHostException);
    }
  }
}
