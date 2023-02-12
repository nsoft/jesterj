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

import static org.jesterj.ingest.model.Status.*;


/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 11/19/14
 */
public class Markers {
  public static final Marker FTI_MARKER = MarkerManager.getMarker("JJ_FTI");
  public static final Marker LOG_MARKER = MarkerManager.getMarker("JJ_REG");

  public static final Marker SET_FORCE = MarkerManager.getMarker(FORCE.toString()).setParents(FTI_MARKER);
  public static final Marker SET_RESTART = MarkerManager.getMarker(RESTART.toString()).setParents(FTI_MARKER);
  public static final Marker SET_DIRTY = MarkerManager.getMarker(DIRTY.toString()).setParents(FTI_MARKER);
  public static final Marker SET_PROCESSING = MarkerManager.getMarker(PROCESSING.toString()).setParents(FTI_MARKER);
  public static final Marker SET_DROPPED = MarkerManager.getMarker(DROPPED.toString()).setParents(FTI_MARKER);
  public static final Marker SET_ERROR = MarkerManager.getMarker(ERROR.toString()).setParents(FTI_MARKER);
  public static final Marker SET_INDEXED = MarkerManager.getMarker(INDEXED.toString()).setParents(FTI_MARKER);
  public static final Marker SET_READY = MarkerManager.getMarker(BATCHED.toString()).setParents(FTI_MARKER);
  public static final Marker SET_SEARCHABLE = MarkerManager.getMarker(SEARCHABLE.toString()).setParents(FTI_MARKER);
  public static final Marker SET_DEAD = MarkerManager.getMarker(DEAD.toString()).setParents(FTI_MARKER);
}
