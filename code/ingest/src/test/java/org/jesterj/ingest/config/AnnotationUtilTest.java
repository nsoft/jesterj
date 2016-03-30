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

package org.jesterj.ingest.config;
/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 3/30/16
 */

import com.copyright.easiertest.Mock;
import com.copyright.easiertest.ObjectUnderTest;
import org.jesterj.ingest.utils.AnnotationUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;

import static com.copyright.easiertest.EasierMocks.prepareMocks;
import static com.copyright.easiertest.EasierMocks.replay;
import static com.copyright.easiertest.EasierMocks.reset;
import static com.copyright.easiertest.EasierMocks.verify;

public class AnnotationUtilTest {
  @ObjectUnderTest private AnnotationUtil obj;
  @Mock private Runnable runnable;

  public AnnotationUtilTest() {
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
  public void testInheritedFromInterface() throws NoSuchMethodException {
    Method meth = Clazz.class.getMethod("methodS2");
    runnable.run();
    replay();
    obj.runIfMethodAnnotated(meth, runnable, true, Transient.class);
  }

  @Test
  public void testInheritedFromInterfaceDontCall() throws NoSuchMethodException {
    Method meth = Clazz.class.getMethod("methodS2");
    replay();
    obj.runIfMethodAnnotated(meth, runnable, false, Transient.class);
  }

  @Test
  public void testInheritedFromSuperClass() throws NoSuchMethodException {
    Method meth = Clazz.class.getMethod("methodS1");
    runnable.run();
    replay();
    obj.runIfMethodAnnotated(meth, runnable, true, Transient.class);
  }

  @Test
  public void testCallOnceEvenIfRepeatedAnnotation() throws NoSuchMethodException {
    Method meth = Clazz.class.getMethod("methodS4");
    runnable.run();
    replay();
    obj.runIfMethodAnnotated(meth, runnable, true, Transient.class);
  }

  @Test
  public void testWontCallIfNotAnnotated() throws NoSuchMethodException {
    Method meth = Clazz.class.getMethod("methodS3");
    replay();
    obj.runIfMethodAnnotated(meth, runnable, true, Transient.class);
  }

  @Test
  public void testWillCallIfNotAnnotated() throws NoSuchMethodException {
    Method meth = Clazz.class.getMethod("methodS3");
    runnable.run();
    replay();
    obj.runIfMethodAnnotated(meth, runnable, false, Transient.class);
  }

  private static class Super2 {
    @Transient
    public void methodS1() {
    }

    @Transient
    public void methodS4() {
    }

  }

  private static class Super1 extends Super2 {
    @Override
    public void methodS1() {
    }

    @Transient
    @Override
    public void methodS4() {
    }
  }

  private static class Clazz extends Super1 implements IFace1 {
    @Override
    public void methodS2() {
    }

    public void methodS3() {
    }
  }

  private interface IFace1 {
    @Transient
    void methodS2();
  }

}
