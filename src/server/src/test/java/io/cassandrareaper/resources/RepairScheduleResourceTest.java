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

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.EditableRepairSchedule;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.resources.view.RepairScheduleStatus;
import io.cassandrareaper.service.TestRepairConfiguration;
import io.cassandrareaper.storage.MemoryStorage;

import java.net.URI;
import java.util.UUID;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RepairScheduleResourceTest {
  private static final URI REPAIR_SCHEDULE_URI = URI.create("http://reaper_host/repair_schedule/");

  @Test
  public void testPatchRepairSchedule() throws ReaperException {
    final MockObjects mocks = initMocks(REPAIR_SCHEDULE_URI);
    final RepairSchedule mockRepairSchedule = mocks.getRepairSchedule();

    // Create a set of changes to patch
    RepairScheduleResource repairScheduleResource = new RepairScheduleResource(mocks.context);
    EditableRepairSchedule editableRepairSchedule = new EditableRepairSchedule();
    editableRepairSchedule.setIntensity(1.0D);
    editableRepairSchedule.setOwner("owner-test-2");
    editableRepairSchedule.setDaysBetween(10);
    editableRepairSchedule.setSegmentCountPerNode(20);
    editableRepairSchedule.setRepairParallelism(RepairParallelism.SEQUENTIAL);

    // Apply the changes
    Response response = repairScheduleResource.patchRepairSchedule(
        mocks.uriInfo,
        mockRepairSchedule.getId(),
        editableRepairSchedule
    );

    // Validate that we got back an expected response
    assertNotNull(response);
    assertEquals(200, response.getStatus());
    assertNotNull(response.getEntity());

    RepairScheduleStatus patchedRepairSchedule = (RepairScheduleStatus) response.getEntity();

    // Check that our modified fields were in fact updated
    // Compare the edited version to the patched version

    // owner
    assertNotNull(patchedRepairSchedule.getOwner());
    assertEquals(editableRepairSchedule.getOwner(), patchedRepairSchedule.getOwner());
    // intensity
    assertNotNull(patchedRepairSchedule.getIntensity());
    assertEquals(editableRepairSchedule.getIntensity().doubleValue(), patchedRepairSchedule.getIntensity(), 0D);
    // segmentCountPerNode
    assertNotNull(patchedRepairSchedule.getSegmentCountPerNode());
    assertEquals(
        editableRepairSchedule.getSegmentCountPerNode().intValue(),
        patchedRepairSchedule.getSegmentCountPerNode()
    );
    // daysBetween
    assertNotNull(patchedRepairSchedule.getDaysBetween());
    assertEquals(editableRepairSchedule.getDaysBetween().intValue(), patchedRepairSchedule.getDaysBetween());
    // repairParallelism
    assertNotNull(patchedRepairSchedule.getRepairParallelism());
    assertEquals(editableRepairSchedule.getRepairParallelism(), patchedRepairSchedule.getRepairParallelism());

    // Check that other fields were left untouched
    // Compare the mocked version to the patched version

    // id
    assertNotNull(patchedRepairSchedule.getId());
    assertEquals(mockRepairSchedule.getId(), patchedRepairSchedule.getId());
    // repairUnitId
    assertNotNull(patchedRepairSchedule.getRepairUnitId());
    assertEquals(mockRepairSchedule.getRepairUnitId(), patchedRepairSchedule.getRepairUnitId());
    // segmentCount
    assertNotNull(patchedRepairSchedule.getSegmentCount());
    assertEquals(mockRepairSchedule.getSegmentCount(), patchedRepairSchedule.getSegmentCount());
    // creationTime
    assertNotNull(patchedRepairSchedule.getCreationTime());
    assertEquals(mockRepairSchedule.getCreationTime(), patchedRepairSchedule.getCreationTime());
    // state
    assertNotNull(patchedRepairSchedule.getState());
    assertEquals(mockRepairSchedule.getState(), patchedRepairSchedule.getState());
  }

  @Test
  public void testApplyRepairPatch() {
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

  private static MockObjects initMocks(URI uri) throws ReaperException {
    MockObjects mockObjects = new MockObjects();

    AppContext context = new AppContext();
    context.storage = new MemoryStorage();
    context.config = TestRepairConfiguration.defaultConfig();
    mockObjects.setContext(context);

    UriInfo uriInfo = mock(UriInfo.class);
    when(uriInfo.getBaseUriBuilder()).thenReturn(UriBuilder.fromUri(uri));
    mockObjects.setUriInfo(uriInfo);

    RepairUnit.Builder mockRepairUnitBuilder = RepairUnit.builder()
        .incrementalRepair(false)
        .repairThreadCount(1)
        .clusterName("cluster-test")
        .keyspaceName("keyspace-test");
    RepairUnit repairUnit = context.storage.addRepairUnit(mockRepairUnitBuilder);
    mockObjects.setRepairUnit(repairUnit);

    RepairSchedule.Builder mockRepairScheduleBuilder = RepairSchedule.builder(repairUnit.getId())
        .daysBetween(1)
        .nextActivation(DateTime.now())
        .segmentCountPerNode(2)
        .owner("owner-test")
        .repairParallelism(RepairParallelism.PARALLEL)
        .creationTime(DateTime.now())
        .intensity(0.5D)
        .state(RepairSchedule.State.ACTIVE);
    RepairSchedule repairSchedule = context.storage.addRepairSchedule(mockRepairScheduleBuilder);
    mockObjects.setRepairSchedule(repairSchedule);

    return mockObjects;
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  private static final class MockObjects {
    private AppContext context;
    private UriInfo uriInfo;
    private RepairSchedule repairSchedule;
    private RepairUnit repairUnit;

    MockObjects(AppContext context, UriInfo uriInfo) {
      this.context = context;
      this.uriInfo = uriInfo;
    }
  }
}
