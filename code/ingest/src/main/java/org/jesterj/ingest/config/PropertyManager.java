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

import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.representer.Representer;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PropertyManager extends Representer {

  public static final AnnotationUtil AUTIL = new AnnotationUtil();

  @Override
  protected Set<Property> getProperties(Class<?> type) throws IntrospectionException {
    Set<Property> set = super.getProperties(type);
    Set<Property> filtered = new TreeSet<>();

    BeanInfo beanInfo = Introspector.getBeanInfo(type);
    PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
    Map<String, PropertyDescriptor> propMap = Arrays.asList(propertyDescriptors).stream().collect(Collectors.toMap(PropertyDescriptor::getName, Function.identity()));
    for (Property prop : set) {
      PropertyDescriptor pd = propMap.get(prop.getName());
      if (pd != null) {
        Method readMethod = pd.getReadMethod();
        AUTIL.runIfMethodAnnotated(readMethod, () -> filtered.add(prop), false, Transient.class);
      }
    }
    return filtered;
  }

//    @Override
//    protected NodeTuple representJavaBeanProperty(Object javaBean, Property property, Object propertyValue, Tag customTag) {
//      if (specialProps.keySet().contains(property.getName())) {
//        return specialProps.get(property.getName()).encode()
//      } else {
//        return super.representJavaBeanProperty(javaBean, property, propertyValue, customTag);
//      }
//    }
}
