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

import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.Router;
import org.jesterj.ingest.model.Step;

import java.util.LinkedHashMap;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 3/18/16
 */
public class DefaultStepNameRouter implements Router {

  public static final String JESTERJ_NEXT_STEP_NAME = "__JESTERJ_NEXT_STEP_NAME__";

  @Override
  public Step route(Document doc, LinkedHashMap<String, Step> nextSteps) {
    return nextSteps.get(doc.getFirstValue(JESTERJ_NEXT_STEP_NAME));
  }
}
