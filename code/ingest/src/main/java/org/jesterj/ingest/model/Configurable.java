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

package org.jesterj.ingest.model;

import java.util.regex.Pattern;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 3/24/16
 */
public interface Configurable {

  Pattern VALID_NAME = Pattern.compile("^[A-Za-z][\\w\\.]*$");

  /**
   * A name for this object to distinguish it from other objects. This value is generally supplied
   * by the plan author. Every object in a plan must have a unique name, begin with a letter
   * and only contain letters, digits, underscores and periods.
   *
   * @return The user supplied name for this step
   */

  String getName();

  default boolean isValidName(String name) {
    return VALID_NAME.matcher(name).matches();
  }
}
