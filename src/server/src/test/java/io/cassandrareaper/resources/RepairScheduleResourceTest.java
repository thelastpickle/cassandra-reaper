/*
 * Copyright 2021-2021 DataStax, Inc.
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

package io.cassandrareaper.resources;

import io.cassandrareaper.core.RepairSchedule;

import java.util.UUID;

import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RepairScheduleResourceTest {
  @Test
  public void testApplyRepairPatchParamsValidParams() {
    DateTime nextActivation = DateTime.now();
    UUID uuid = UUID.randomUUID();
    RepairSchedule repairSchedule = RepairSchedule.builder(uuid)
        .nextActivation(nextActivation)
        .owner("test")
        .repairParallelism(RepairParallelism.PARALLEL)
        .intensity(1.0D)
        .daysBetween(1)
        .segmentCountPerNode(2)
        .build(uuid);

    assertTrue(nextActivation.equals(repairSchedule.getNextActivation()));
    assertTrue(uuid.equals(repairSchedule.getRepairUnitId()));
    assertTrue(uuid.equals(repairSchedule.getId()));
    assertEquals("test", repairSchedule.getOwner());
    assertEquals(RepairParallelism.PARALLEL, repairSchedule.getRepairParallelism());
    assertTrue(1.0D == repairSchedule.getIntensity());
    assertEquals(1, repairSchedule.getDaysBetween() != null
        ? repairSchedule.getDaysBetween().intValue()
        : -1);
    assertEquals(2, repairSchedule.getSegmentCountPerNode() != null
        ? repairSchedule.getSegmentCountPerNode().intValue()
        : -1);

    RepairSchedule patchedRepairSchedule = RepairScheduleResource.applyRepairPatchParams(
        repairSchedule,
        "test2",
        RepairParallelism.SEQUENTIAL,
        0.0D,
        2,
        3
    );

    assertTrue(nextActivation.equals(patchedRepairSchedule.getNextActivation()));
    assertTrue(uuid.equals(patchedRepairSchedule.getRepairUnitId()));
    assertTrue(uuid.equals(patchedRepairSchedule.getId()));
    assertEquals("test2", patchedRepairSchedule.getOwner());
    assertEquals(RepairParallelism.SEQUENTIAL, patchedRepairSchedule.getRepairParallelism());
    assertTrue(0.0D == patchedRepairSchedule.getIntensity());
    assertEquals(2, patchedRepairSchedule.getDaysBetween() != null
        ? patchedRepairSchedule.getDaysBetween().intValue()
        : -1);
    assertEquals(3, patchedRepairSchedule.getSegmentCountPerNode() != null
        ? patchedRepairSchedule.getSegmentCountPerNode().intValue()
        : -1);
  }
}
