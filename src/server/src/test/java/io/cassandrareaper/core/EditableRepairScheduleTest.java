/*
 * Copyright 2015-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
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

package io.cassandrareaper.core;

import java.util.UUID;

import org.apache.cassandra.repair.RepairParallelism;
import org.assertj.core.api.Assertions;
import org.joda.time.DateTime;
import org.junit.Test;

public final class EditableRepairScheduleTest {

  @Test
  public void testEditableValueInheritance() {
    RepairSchedule repairSchedule =
        RepairSchedule.builder(UUID.randomUUID())
            .daysBetween(1)
            .intensity(1.0D)
            .segmentCountPerNode(2)
            .owner("test")
            .repairParallelism(RepairParallelism.PARALLEL)
            .nextActivation(DateTime.now())
            .build(UUID.randomUUID());

    Assertions.assertThat(repairSchedule.getDaysBetween()).isEqualTo(1);
    Assertions.assertThat(repairSchedule.getIntensity()).isEqualTo(1.0D);
    Assertions.assertThat(repairSchedule.getSegmentCountPerNode()).isEqualTo(2);
    Assertions.assertThat(repairSchedule.getOwner()).isEqualTo("test");
  }
}
