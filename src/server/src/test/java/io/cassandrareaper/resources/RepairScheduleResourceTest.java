/*
 * Copyright 2021- DataStax Inc.
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
import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RepairScheduleResourceTest {
    @Test
    public void testValidateRepairPatchParamsValidParams() {
        String ownerValue = "test";
        String repairParallelismValue = RepairParallelism.PARALLEL.toString();
        Double intensityValue = 1.0D;
        Integer scheduleDaysBetweenValue = 2;
        Integer segmentCountPerNodeValue = 5;
        List<String> invalidParams = RepairScheduleResource.validateRepairPatchParams(ownerValue, repairParallelismValue, intensityValue, scheduleDaysBetweenValue, segmentCountPerNodeValue);
        assertTrue(invalidParams.isEmpty());
    }

    @Test
    public void testValidateRepairPatchParamsInvalidParams() {
        String ownerValue = null;
        String repairParallelismValue = null;
        Double intensityValue = null;
        Integer scheduleDaysBetweenValue = null;
        Integer segmentCountPerNodeValue = null;
        List<String> invalidParams = RepairScheduleResource.validateRepairPatchParams(ownerValue, repairParallelismValue, intensityValue, scheduleDaysBetweenValue, segmentCountPerNodeValue);
        assertTrue(invalidParams.size() == 5);
    }

    @Test
    public void testValidateRepairPatchParamsInvalidOwnerParam() {
        String ownerValue = null;
        String repairParallelismValue = RepairParallelism.PARALLEL.toString();
        Double intensityValue = 1.0D;
        Integer scheduleDaysBetweenValue = 2;
        Integer segmentCountPerNodeValue = 5;
        List<String> invalidParams = RepairScheduleResource.validateRepairPatchParams(ownerValue, repairParallelismValue, intensityValue, scheduleDaysBetweenValue, segmentCountPerNodeValue);
        assertTrue(invalidParams.size() == 1);
        assertTrue(invalidParams.contains("owner"));

        ownerValue = "";
        invalidParams = RepairScheduleResource.validateRepairPatchParams(ownerValue, repairParallelismValue, intensityValue, scheduleDaysBetweenValue, segmentCountPerNodeValue);
        assertTrue(invalidParams.size() == 1);
        assertTrue(invalidParams.contains("owner"));
    }

    @Test
    public void testValidateRepairPatchParamsInvalidRepairParallelismParam() {
        String ownerValue = "test";
        String repairParallelismValue = null;
        Double intensityValue = 1.0D;
        Integer scheduleDaysBetweenValue = 2;
        Integer segmentCountPerNodeValue = 5;
        List<String> invalidParams = RepairScheduleResource.validateRepairPatchParams(ownerValue, repairParallelismValue, intensityValue, scheduleDaysBetweenValue, segmentCountPerNodeValue);
        assertTrue(invalidParams.size() == 1);
        assertTrue(invalidParams.contains("repairParallelism"));

        repairParallelismValue = "";
        invalidParams = RepairScheduleResource.validateRepairPatchParams(ownerValue, repairParallelismValue, intensityValue, scheduleDaysBetweenValue, segmentCountPerNodeValue);
        assertTrue(invalidParams.size() == 1);
        assertTrue(invalidParams.contains("repairParallelism"));

        repairParallelismValue = "INVALID";
        invalidParams = RepairScheduleResource.validateRepairPatchParams(ownerValue, repairParallelismValue, intensityValue, scheduleDaysBetweenValue, segmentCountPerNodeValue);
        assertTrue(invalidParams.size() == 1);
        assertTrue(invalidParams.contains("repairParallelism"));
    }

    @Test
    public void testValidateRepairPatchParamsInvalidIntensityParam() {
        String ownerValue = "test";
        String repairParallelismValue = RepairParallelism.PARALLEL.toString();
        Double intensityValue = null;
        Integer scheduleDaysBetweenValue = 2;
        Integer segmentCountPerNodeValue = 5;
        List<String> invalidParams = RepairScheduleResource.validateRepairPatchParams(ownerValue, repairParallelismValue, intensityValue, scheduleDaysBetweenValue, segmentCountPerNodeValue);
        assertTrue(invalidParams.size() == 1);
        assertTrue(invalidParams.contains("intensity"));

        intensityValue = 1.1D;
        invalidParams = RepairScheduleResource.validateRepairPatchParams(ownerValue, repairParallelismValue, intensityValue, scheduleDaysBetweenValue, segmentCountPerNodeValue);
        assertTrue(invalidParams.size() == 1);
        assertTrue(invalidParams.contains("intensity"));
    }

    @Test
    public void testValidateRepairPatchParamsInvalidScheduleDaysBetweenParam() {
        String ownerValue = "test";
        String repairParallelismValue = RepairParallelism.PARALLEL.toString();
        Double intensityValue = 1.0D;
        Integer scheduleDaysBetweenValue = null;
        Integer segmentCountPerNodeValue = 5;
        List<String> invalidParams = RepairScheduleResource.validateRepairPatchParams(ownerValue, repairParallelismValue, intensityValue, scheduleDaysBetweenValue, segmentCountPerNodeValue);
        assertTrue(invalidParams.size() == 1);
        assertTrue(invalidParams.contains("scheduleDaysBetween"));

        scheduleDaysBetweenValue = 0;
        invalidParams = RepairScheduleResource.validateRepairPatchParams(ownerValue, repairParallelismValue, intensityValue, scheduleDaysBetweenValue, segmentCountPerNodeValue);
        assertTrue(invalidParams.size() == 1);
        assertTrue(invalidParams.contains("scheduleDaysBetween"));
    }

    @Test
    public void testValidateRepairPatchParamsInvalidSegmentCountPerNodeParam() {
        String ownerValue = "test";
        String repairParallelismValue = RepairParallelism.PARALLEL.toString();
        Double intensityValue = 1.0D;
        Integer scheduleDaysBetweenValue = 2;
        Integer segmentCountPerNodeValue = null;
        List<String> invalidParams = RepairScheduleResource.validateRepairPatchParams(ownerValue, repairParallelismValue, intensityValue, scheduleDaysBetweenValue, segmentCountPerNodeValue);
        assertTrue(invalidParams.size() == 1);
        assertTrue(invalidParams.contains("segmentCountPerNode"));

        segmentCountPerNodeValue = 0;
        invalidParams = RepairScheduleResource.validateRepairPatchParams(ownerValue, repairParallelismValue, intensityValue, scheduleDaysBetweenValue, segmentCountPerNodeValue);
        assertTrue(invalidParams.size() == 1);
        assertTrue(invalidParams.contains("segmentCountPerNode"));
    }

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
        assertEquals(1, repairSchedule.getDaysBetween());
        assertEquals(2, repairSchedule.getSegmentCountPerNode());

        RepairSchedule patchedRepairSchedule = RepairScheduleResource.applyRepairPatchParams(
                repairSchedule,
                "test2",
                RepairParallelism.SEQUENTIAL.toString(),
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
        assertEquals(2, patchedRepairSchedule.getDaysBetween());
        assertEquals(3, patchedRepairSchedule.getSegmentCountPerNode());
    }
}
