/*
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

package io.cassandrareaper.core;

import io.cassandrareaper.core.RepairSegment.State;
import io.cassandrareaper.service.RingRange;

import java.util.UUID;

import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RepairSegmentTest {

  @Test
  public void startAndEndTimeAndStateAreConsistent1() {
    RepairSegment segment =
        RepairSegment.builder(new RingRange("1", "10"), UUID.randomUUID())
            .withRunId(UUID.randomUUID())
            .startTime(DateTime.now())
            .endTime(null)
            .state(State.NOT_STARTED)
            .build(UUID.randomUUID());

    assertEquals(true, segment.isConsistentOnTimesAndState());
  }

  @Test
  public void startAndEndTimeAndStateAreConsistent2() {
    RepairSegment segment =
        RepairSegment.builder(new RingRange("1", "10"), UUID.randomUUID())
            .withRunId(UUID.randomUUID())
            .startTime(DateTime.now())
            .endTime(DateTime.now())
            .state(State.DONE)
            .build(UUID.randomUUID());

    assertEquals(true, segment.isConsistentOnTimesAndState());
  }

  @Test
  public void startAndEndTimeAndStateAreNotConsistent1() {
    RepairSegment segment =
        RepairSegment.builder(new RingRange("1", "10"), UUID.randomUUID())
            .withRunId(UUID.randomUUID())
            .startTime(null)
            .endTime(DateTime.now())
            .state(State.DONE)
            .build(UUID.randomUUID());

    assertEquals(false, segment.isConsistentOnTimesAndState());
  }

  @Test
  public void startAndEndTimeAndStateAreNotConsistent2() {
    RepairSegment segment =
        RepairSegment.builder(new RingRange("1", "10"), UUID.randomUUID())
            .withRunId(UUID.randomUUID())
            .startTime(DateTime.now())
            .endTime(DateTime.now())
            .state(State.RUNNING)
            .build(UUID.randomUUID());

    assertEquals(false, segment.isConsistentOnTimesAndState());
  }

  @Test
  public void startAndEndTimeAndStateAreNotConsistent3() {
    RepairSegment segment =
        RepairSegment.builder(new RingRange("1", "10"), UUID.randomUUID())
            .withRunId(UUID.randomUUID())
            .startTime(null)
            .endTime(null)
            .state(State.RUNNING)
            .build(UUID.randomUUID());

    assertEquals(false, segment.isConsistentOnTimesAndState());
  }


}
