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

package org.jesterj.ingest.logging;
/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 11/8/14
 */

import com.copyright.easiertest.ObjectUnderTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.SocketException;
import java.util.LinkedHashSet;
import java.util.Set;

import static com.copyright.easiertest.EasierMocks.prepareMocks;
import static com.copyright.easiertest.EasierMocks.replay;
import static com.copyright.easiertest.EasierMocks.reset;
import static com.copyright.easiertest.EasierMocks.verify;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class CassandraConfigTest {
  public static final String LOCALHOST = "127.0.0.1";
  public static final String LOCALHOSTV6 = "::1";
  public static final String LOCALHOSTV6_0 = "0:0:0:0:0:0:0:1";
  public static final String LOCALHOSTV6_FULL = "0000:0000:0000:0000:0000:0000:0000:0001";
  public static final String CLASS_C = "192.168.1.1";
  public static final String PUBLIC = "111.121.23.1";
  public static final String CLASS_B = "172.24.23.1";
  public static final String CLASS_A = "10.121.23.1";
  public static final String IPV6_PRIV = "fd30:0000:0000:0001:ff4e:003e:0009:000e";
  @ObjectUnderTest private CassandraConfig config;
  private Set<String> addrs = new LinkedHashSet<>();

  public CassandraConfigTest() {
    prepareMocks(this);
  }

  @Before
  public void setUp() {
    reset();
  }

  @After
  public void tearDown() {
    verify();
  }

  @Test
  public void testLocalhost() throws SocketException {
    addrs.add(LOCALHOST);
    expect(config.listAdresses()).andReturn(addrs);
    expect(config.better(LOCALHOST)).andReturn(false);
    expect(config.getListen_address()).andReturn(LOCALHOST);
    replay();
    assertEquals( LOCALHOST, config.guessIp());
  }

  @Test
  public void testSeveralShouldBelast() throws SocketException {
    addrs.add(LOCALHOST);
    addrs.add(LOCALHOSTV6);
    addrs.add("192.168.1.3");
    expect(config.listAdresses()).andReturn(addrs);
    expect(config.better(LOCALHOST)).andReturn(false);
    expect(config.better(LOCALHOSTV6)).andReturn(false);
    expect(config.better("192.168.1.3")).andReturn(true);
    config.setListen_address("192.168.1.3");
    expect(config.getListen_address()).andReturn("192.168.1.3");
    replay();
    assertEquals( "192.168.1.3", config.guessIp());
  }
  @Test
  public void testSeveralShouldBeMiddle() throws SocketException {
    addrs.add(LOCALHOST);
    addrs.add("192.168.1.3");
    addrs.add(LOCALHOSTV6);
    expect(config.listAdresses()).andReturn(addrs);
    expect(config.better(LOCALHOST)).andReturn(false);
    expect(config.better("192.168.1.3")).andReturn(true);
    config.setListen_address("192.168.1.3");
    expect(config.better(LOCALHOSTV6)).andReturn(false);
    expect(config.getListen_address()).andReturn("192.168.1.3");
    replay();
    assertEquals( "192.168.1.3", config.guessIp());
  }

  @Test
  public void testSeveralShouldBSetTwice() throws SocketException {
    addrs.add(LOCALHOST);
    addrs.add("192.168.1.3");
    addrs.add(PUBLIC);
    expect(config.listAdresses()).andReturn(addrs);
    expect(config.better(LOCALHOST)).andReturn(false);
    expect(config.better("192.168.1.3")).andReturn(true);
    config.setListen_address("192.168.1.3");
    expect(config.better(PUBLIC)).andReturn(true);
    config.setListen_address(PUBLIC);

    expect(config.getListen_address()).andReturn(PUBLIC);
    replay();
    assertEquals( PUBLIC, config.guessIp());
  }

  @Test
  public void testSeveralShouldBSetOnce() throws SocketException {
    addrs.add(LOCALHOST);
    addrs.add(PUBLIC);
    addrs.add("192.168.1.3");
    expect(config.listAdresses()).andReturn(addrs);
    expect(config.better(LOCALHOST)).andReturn(false);
    expect(config.better(PUBLIC)).andReturn(true);
    config.setListen_address(PUBLIC);
    expect(config.better("192.168.1.3")).andReturn(false);

    expect(config.getListen_address()).andReturn(PUBLIC);
    replay();
    assertEquals( PUBLIC, config.guessIp());
  }

  @Test
  public void testBetter() throws SocketException {
    replay();
    CassandraConfig example = new CassandraConfig();

    example.setListen_address(LOCALHOST);
    assertTrue(example.better(PUBLIC));
    assertTrue(example.better(IPV6_PRIV));
    assertTrue(example.better(CLASS_A));
    assertTrue(example.better(CLASS_B));
    assertTrue(example.better(CLASS_C));
    assertFalse(example.better(LOCALHOST));

    example.setListen_address(LOCALHOSTV6);
    assertTrue(example.better(PUBLIC));
    assertTrue(example.better(IPV6_PRIV));
    assertTrue(example.better(CLASS_A));
    assertTrue(example.better(CLASS_B));
    assertTrue(example.better(CLASS_C));
    assertFalse(example.better(LOCALHOST));

    example.setListen_address(LOCALHOSTV6_0);
    assertTrue(example.better(PUBLIC));
    assertTrue(example.better(IPV6_PRIV));
    assertTrue(example.better(CLASS_A));
    assertTrue(example.better(CLASS_B));
    assertTrue(example.better(CLASS_C));
    assertFalse(example.better(LOCALHOST));
    
    example.setListen_address(LOCALHOSTV6_FULL);
    assertTrue(example.better(PUBLIC));
    assertTrue(example.better(IPV6_PRIV));
    assertTrue(example.better(CLASS_A));
    assertTrue(example.better(CLASS_B));
    assertTrue(example.better(CLASS_C));
    assertFalse(example.better(LOCALHOST));
    
    example.setListen_address(CLASS_C);
    assertTrue(example.better(PUBLIC));
    assertTrue(example.better(IPV6_PRIV));
    assertTrue(example.better(CLASS_A));
    assertTrue(example.better(CLASS_B));
    assertFalse(example.better(CLASS_C));
    assertFalse(example.better(LOCALHOST));

    example.setListen_address(CLASS_B);
    assertTrue(example.better(PUBLIC));
    assertTrue(example.better(IPV6_PRIV));
    assertTrue(example.better(CLASS_A));
    assertFalse(example.better(CLASS_B));
    assertFalse(example.better(CLASS_C));
    assertFalse(example.better(LOCALHOST));

    example.setListen_address(CLASS_B);
    assertTrue(example.better(PUBLIC));
    assertTrue(example.better(IPV6_PRIV));
    assertTrue(example.better(CLASS_A));
    assertFalse(example.better(CLASS_B));
    assertFalse(example.better(CLASS_C));
    assertFalse(example.better(LOCALHOST));


  }

}
