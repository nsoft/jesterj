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

package org.jesterj.ingest.utils;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 3/29/16
 * 
 */

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

/**
 * Class for handling common operations with annotations
 */
public class AnnotationUtil {

  /**
   * Execute the supplied runnable once (in same thread) if the supplied method has the supplied annotation.
   * This method supports inheritance of method annotations. If the supplied method overrides a superclass or
   * implements an interface with the annotation the runnable will be executed. Even if the annotation is available
   * from multiple levels of the class hierarchy the runnable will only execute once.
   *
   * @param meth         The method to test
   * @param r            The runnable that will run depending on the annotation
   * @param runIfPresent true to run when the annotation is found, false to run when it's not found.
   * @param annotation   The annotation we are looking for
   */
  public void runIfMethodAnnotated(Method meth, Runnable r, boolean runIfPresent,
                                   Class<? extends Annotation> annotation) {
    Set<Class> classes = new HashSet<>();
    Class<?> clazz = meth.getDeclaringClass();
    collectInterfaces(classes, clazz);
    while (clazz != Object.class) {
      classes.add(clazz);
      clazz = clazz.getSuperclass();
    }

    // now iterate all superclasses and interfaces looking for a method with identical signature that has 
    // the annotation in question.
    boolean found = false;
    for (Class<?> c : classes) {
      try {
        Method m = c.getMethod(meth.getName(), meth.getParameterTypes());
        Annotation[] declaredAnnotations = m.getDeclaredAnnotations();
        for (Annotation a : declaredAnnotations) {
          found |= annotation == a.annotationType();
          if (runIfPresent && found) {
            r.run();
            return;
          }
        }
      } catch (NoSuchMethodException ignored) {
      }
    }
    if (!runIfPresent && !found) {
      r.run();
    }
  }

  private void collectInterfaces(Set<Class> iFaces, Class startFrom) {
    Class[] inters = startFrom.getInterfaces();
    for (Class inter : inters) {
      collectInterfaces(iFaces, inter);
      iFaces.add(inter);
    }
  }

}