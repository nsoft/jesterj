/*
 * Copyright 2014-2016 Needham Software LLC
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

import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;


/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 11/19/14
 */
public class Markers {
  public static final Marker FTI_MARKER = MarkerManager.getMarker("JJ_FTI");
  public static final Marker LOG_MARKER = MarkerManager.getMarker("JJ_REG");
}
