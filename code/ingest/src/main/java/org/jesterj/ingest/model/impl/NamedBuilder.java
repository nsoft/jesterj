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

import org.jesterj.ingest.config.Required;
import org.jesterj.ingest.model.Configurable;
import org.jesterj.ingest.model.ConfiguredBuildable;
import org.yaml.snakeyaml.DumperOptions;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 3/26/16
 */
public abstract class NamedBuilder<TYPE extends Configurable> implements ConfiguredBuildable<TYPE> {

  private static final DumperOptions OPTS = new DumperOptions();

  static {
    OPTS.setAllowReadOnlyProperties(true);
    OPTS.setIndent(2);
  }

  @Required
  public abstract NamedBuilder<TYPE> named(String name);

  protected TYPE getObj() {
    return null; // abstract class, never used.
  }

  public boolean isValid() {
    return getObj().getName() != null && getObj().isValidName(getObj().getName());
  }

}
