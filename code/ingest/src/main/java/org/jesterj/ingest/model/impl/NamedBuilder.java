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

package org.jesterj.ingest.model.impl;

import org.jesterj.ingest.model.Configurable;
import org.jesterj.ingest.model.ConfiguredBuildable;
import org.yaml.snakeyaml.Yaml;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 3/26/16
 */
public abstract class NamedBuilder<TYPE extends Configurable> implements ConfiguredBuildable<TYPE> {
  private TYPE obj;

  public abstract NamedBuilder<TYPE> named(String name);

  protected TYPE getObj() {
    return obj;
  }

  private void setObj(TYPE obj) {
    this.obj = obj;
  }

  public boolean isValid() {
    return getObj().isValidName(getObj().getName());
  }

  public String toYaml(TYPE obj) {
    return new Yaml().dump(obj);
  }

  public ConfiguredBuildable<TYPE> fromYaml(String yaml) {
    //noinspection unchecked
    TYPE t = new Yaml().loadAs(yaml, (Class<TYPE>) getObj().getClass());
    setObj(t);
    return this;
  }
}
