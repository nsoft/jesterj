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

import org.jesterj.ingest.logging.Markers;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 10/19/16
 */
public class StatusTest {

  @Test
  public void testStatusMarkers() {
    assertEquals(Status.BATCHED.getMarker(), Markers.SET_READY);
    assertEquals(Status.DIRTY.getMarker(), Markers.SET_DIRTY);
    assertEquals(Status.DEAD.getMarker(), Markers.SET_DEAD);
    assertEquals(Status.DROPPED.getMarker(), Markers.SET_DROPPED);
    assertEquals(Status.ERROR.getMarker(), Markers.SET_ERROR);
    assertEquals(Status.INDEXED.getMarker(), Markers.SET_INDEXED);
    assertEquals(Status.PROCESSING.getMarker(), Markers.SET_PROCESSING);
    assertEquals(Status.SEARCHABLE.getMarker(), Markers.SET_SEARCHABLE);
  }
}
