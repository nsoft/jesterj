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

package org.jesterj.ingest.model;/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 12/7/14
 */


// this may or may not remain...
public interface ConfiguredBuildable<T> extends Buildable<T> {

  /**
   * Determine if this builder will produce a valid object. When this method returns false
   * the {@link #build()} method should throw an exception
   *
   * @return true if it's safe to <code>build()</code> false otherwise
   */
  boolean isValid();

  /**
   * Represent the state of the supplied object as Yaml.
   *
   * @param obj the object to convert to yaml
   * @return a yaml string
   */
  String toYaml(T obj);

  /**
   * Construct an object from yaml and replace the current internal object with the newly constructed one.
   *
   * @param yaml A string containing Yaml
   * @return this buildable object
   */
  ConfiguredBuildable<T> fromYaml(String yaml);

//  /**
//   * Construct an object from yaml and replace the current internal object with the newly constructed one.
//   *
//   * @param stream A UTF-8 stream containing yaml
//   * @return this buildable object
//   */
//  default ConfiguredBuildable<T> fromYaml(InputStream stream) throws IOException {
//    return fromYaml(IOUtils.toString(stream, "UTF-8"));
//  }

}
