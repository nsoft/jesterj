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


import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

public class ConfigurableTest {
  private String name;
  Configurable obj = () -> name;


  @Test
  public void testObj() {
    assertFalse(obj.isValidName(name));
    name = "99redBaloons";
    assertFalse(obj.isValidName(name));
    name = "Arthur Dent";
    assertFalse(obj.isValidName(name));
    name = "Zarniwhoop";
    assertTrue(obj.isValidName(name));
    name = "HAL9000";
    assertTrue(obj.isValidName(name));
  }

}
