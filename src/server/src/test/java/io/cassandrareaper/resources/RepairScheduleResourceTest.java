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
import io.cassandrareaper.core.EditableRepairSchedule;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.resources.view.RepairScheduleStatus;
import io.cassandrareaper.service.TestRepairConfiguration;
import io.cassandrareaper.storage.IStorage;
import io.cassandrareaper.storage.MemoryStorageFacade;
import io.cassandrareaper.storage.repairschedule.IRepairSchedule;

import java.net.URI;
import java.util.Optional;
import java.util.UUID;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import io.dropwizard.jersey.validation.ValidationErrorMessage;
import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RepairScheduleResourceTest {
  private static final URI REPAIR_SCHEDULE_URI = URI.create("http://reaper_host/repair_schedule/");

  private static MockObjects initFailedUpdateStorageMocks(URI uri, UUID repairScheduleId) {
    MockObjects mockObjects = new MockObjects();

    final AppContext context = new AppContext();

    IStorage mockedStorage = mock(IStorage.class);
    RepairSchedule repairSchedule = RepairSchedule.builder(UUID.randomUUID())
        .nextActivation(DateTime.now())
        .owner("test")
        .repairParallelism(RepairParallelism.PARALLEL)
        .intensity(1.0D)
        .daysBetween(1)
        .segmentCountPerNode(2)
        .build(repairScheduleId);
    mockObjects.setRepairSchedule(repairSchedule);

    IRepairSchedule mockedRepairScheduleDao = mock(IRepairSchedule.class);
    when(mockedStorage.getRepairScheduleDao()).thenReturn(mockedRepairScheduleDao);
    when(mockedRepairScheduleDao.getRepairSchedule(repairScheduleId)).thenReturn(Optional.of(repairSchedule));
    when(mockedStorage.getRepairScheduleDao().updateRepairSchedule(any())).thenReturn(false);
    context.storage = mockedStorage;

    context.config = TestRepairConfiguration.defaultConfig();
    mockObjects.setContext(context);

    UriInfo uriInfo = mock(UriInfo.class);
    when(uriInfo.getBaseUriBuilder()).thenReturn(UriBuilder.fromUri(uri));
    mockObjects.setUriInfo(uriInfo);

    return mockObjects;
  }

  private static MockObjects initInMemoryMocks(URI uri) {
    MockObjects mockObjects = new MockObjects();

    AppContext context = new AppContext();
    context.storage = new MemoryStorageFacade();
    context.config = TestRepairConfiguration.defaultConfig();
    mockObjects.setContext(context);

    UriInfo uriInfo = mock(UriInfo.class);
    when(uriInfo.getBaseUriBuilder()).thenReturn(UriBuilder.fromUri(uri));
    mockObjects.setUriInfo(uriInfo);

    RepairUnit.Builder mockRepairUnitBuilder = RepairUnit.builder()
        .incrementalRepair(false)
        .repairThreadCount(1)
        .clusterName("cluster-test")
        .keyspaceName("keyspace-test")
        .timeout(30);
    RepairUnit repairUnit = context.storage.getRepairUnitDao().addRepairUnit(mockRepairUnitBuilder);
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
    RepairSchedule repairSchedule = context.storage.getRepairScheduleDao().addRepairSchedule(mockRepairScheduleBuilder);
    mockObjects.setRepairSchedule(repairSchedule);

    return mockObjects;
  }

  private static RepairSchedule buildBasicTestRepairSchedule() {
    DateTime nextActivation = DateTime.now();
    UUID id = UUID.randomUUID();
    UUID unitId = UUID.randomUUID();
    RepairSchedule repairSchedule = RepairSchedule.builder(unitId)
        .nextActivation(nextActivation)
        .owner("test")
        .repairParallelism(RepairParallelism.PARALLEL)
        .intensity(1.0D)
        .daysBetween(1)
        .segmentCountPerNode(2)
        .build(id);
    return repairSchedule;
  }

  private static EditableRepairSchedule buildBasicTestEditableRepairSchedule() {
    EditableRepairSchedule editableRepairSchedule = new EditableRepairSchedule();
    editableRepairSchedule.setIntensity(1.0D);
    editableRepairSchedule.setOwner("owner-test-2");
    editableRepairSchedule.setDaysBetween(10);
    editableRepairSchedule.setSegmentCountPerNode(20);
    editableRepairSchedule.setRepairParallelism(RepairParallelism.SEQUENTIAL);
    return editableRepairSchedule;
  }

  @Test
  public void testPatchRepairScheduleBadPathParam() {
    final MockObjects mocks = initInMemoryMocks(REPAIR_SCHEDULE_URI);

    RepairScheduleResource repairScheduleResource = new RepairScheduleResource(mocks.context,
        mocks.context.storage.getRepairRunDao());

    Response response = repairScheduleResource.patchRepairSchedule(
        mocks.uriInfo,
        null,
        null
    );

    // Validate that we got back 400 - an invalid schedule-id should return a 400
    assertThat(response).isNotNull();
    assertThat(Response.Status.BAD_REQUEST.getStatusCode()).isEqualTo(response.getStatus());
    assertThat(response.getEntity()).isNotNull();

    ValidationErrorMessage errorMessage = (ValidationErrorMessage) response.getEntity();
    assertThat(errorMessage.getErrors()).isNotNull().isNotEmpty();
  }

  @Test
  public void testPatchRepairScheduleEmptyBody() {
    final MockObjects mocks = initInMemoryMocks(REPAIR_SCHEDULE_URI);

    RepairScheduleResource repairScheduleResource = new RepairScheduleResource(mocks.context,
        mocks.context.storage.getRepairRunDao());

    Response response = repairScheduleResource.patchRepairSchedule(
        mocks.uriInfo,
        mocks.getRepairSchedule().getId(),
        null
    );

    // Validate that we got back 400 - an invalid editable-repair-schedule (body) should return a 400
    assertThat(response).isNotNull();
    assertThat(response.getStatus()).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
    assertThat(response.getEntity()).isNotNull();

    ValidationErrorMessage errorMessage = (ValidationErrorMessage) response.getEntity();
    assertThat(errorMessage.getErrors()).isNotNull().isNotEmpty();
  }

  @Test
  public void testPatchRepairScheduleFailedUpdate() {
    final MockObjects mocks = initFailedUpdateStorageMocks(REPAIR_SCHEDULE_URI, UUID.randomUUID());
    final RepairSchedule mockRepairSchedule = mocks.getRepairSchedule();

    // Create a set of changes to patch
    RepairScheduleResource repairScheduleResource = new RepairScheduleResource(mocks.context,
        mocks.context.storage.getRepairRunDao());
    EditableRepairSchedule editableRepairSchedule = buildBasicTestEditableRepairSchedule();

    // Apply the changes
    Response response = repairScheduleResource.patchRepairSchedule(
        mocks.uriInfo,
        mockRepairSchedule.getId(),
        editableRepairSchedule
    );

    // Validate that we got back a 500 when the update was valid but fails
    assertThat(response).isNotNull();
    assertThat(response.getStatus()).isEqualTo(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
  }

  @Test
  public void testPatchRepairScheduleNotFoundSchedule() {
    final MockObjects mocks = initFailedUpdateStorageMocks(REPAIR_SCHEDULE_URI, UUID.randomUUID());

    // Create a set of changes to patch
    RepairScheduleResource repairScheduleResource = new RepairScheduleResource(mocks.context,
        mocks.context.storage.getRepairRunDao());
    EditableRepairSchedule editableRepairSchedule = buildBasicTestEditableRepairSchedule();

    // Apply the changes - with a random UUID that won't be found
    Response response = repairScheduleResource.patchRepairSchedule(
        mocks.uriInfo,
        UUID.randomUUID(),
        editableRepairSchedule
    );

    // Validate that we got back a 500 when the update was valid but fails
    assertThat(response).isNotNull();
    assertThat(response.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
  }

  @Test
  public void testPatchRepairSchedule() {
    final MockObjects mocks = initInMemoryMocks(REPAIR_SCHEDULE_URI);
    final RepairSchedule mockRepairSchedule = mocks.getRepairSchedule();

    // Create a set of changes to patch
    RepairScheduleResource repairScheduleResource = new RepairScheduleResource(mocks.context,
        mocks.context.storage.getRepairRunDao());
    EditableRepairSchedule editableRepairSchedule = buildBasicTestEditableRepairSchedule();

    // Apply the changes
    Response response = repairScheduleResource.patchRepairSchedule(
        mocks.uriInfo,
        mockRepairSchedule.getId(),
        editableRepairSchedule
    );

    // Validate that we got back an expected response
    assertThat(response).isNotNull();
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getEntity()).isNotNull();

    RepairScheduleStatus patchedRepairSchedule = (RepairScheduleStatus) response.getEntity();

    // Check that our modified fields were in fact updated
    // Compare the edited version to the patched version

    // owner
    assertThat(patchedRepairSchedule.getOwner()).isNotNull();
    assertThat(patchedRepairSchedule.getOwner()).isEqualTo(editableRepairSchedule.getOwner());
    // intensity
    assertThat(patchedRepairSchedule.getIntensity()).isNotNull();
    assertThat(patchedRepairSchedule.getIntensity()).isEqualTo(editableRepairSchedule.getIntensity().doubleValue());
    // segmentCountPerNode
    assertThat(patchedRepairSchedule.getSegmentCountPerNode()).isNotNull();
    assertThat(patchedRepairSchedule.getSegmentCountPerNode())
        .isEqualTo(editableRepairSchedule.getSegmentCountPerNode().intValue());
    // daysBetween
    assertThat(patchedRepairSchedule.getDaysBetween()).isNotNull();
    assertThat(patchedRepairSchedule.getDaysBetween()).isEqualTo(editableRepairSchedule.getDaysBetween().intValue());
    // repairParallelism
    assertThat(patchedRepairSchedule.getRepairParallelism()).isNotNull();
    assertThat(patchedRepairSchedule.getRepairParallelism()).isEqualTo(editableRepairSchedule.getRepairParallelism());

    // Check that other fields were left untouched
    // Compare the mocked version to the patched version

    // id
    assertThat(patchedRepairSchedule.getId()).isNotNull();
    assertThat(patchedRepairSchedule.getId()).isEqualTo(mockRepairSchedule.getId());
    // repairUnitId
    assertThat(patchedRepairSchedule.getRepairUnitId()).isNotNull();
    assertThat(patchedRepairSchedule.getRepairUnitId()).isEqualTo(mockRepairSchedule.getRepairUnitId());
    // creationTime
    assertThat(patchedRepairSchedule.getCreationTime()).isNotNull();
    assertThat(patchedRepairSchedule.getCreationTime()).isEqualTo(mockRepairSchedule.getCreationTime());
    // state
    assertThat(patchedRepairSchedule.getState()).isNotNull();
    assertThat(patchedRepairSchedule.getState()).isEqualTo(mockRepairSchedule.getState());
  }

  @Test
  public void testApplyRepairPatchBadParams() {
    RepairSchedule patchedRepairSchedule = RepairScheduleResource.applyRepairPatchParams(
        null,
        "test2",
        RepairParallelism.SEQUENTIAL,
        0.0D,
        2,
        3,
        false,
        null
    );

    assertThat(patchedRepairSchedule).isNull();
  }

  @Test
  public void testApplyRepairPatchParamsEmpty() {
    RepairSchedule repairSchedule = buildBasicTestRepairSchedule();

    RepairSchedule patchedRepairSchedule = RepairScheduleResource.applyRepairPatchParams(
        repairSchedule,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );

    assertThat(patchedRepairSchedule.getNextActivation()).isNotNull().isEqualTo(repairSchedule.getNextActivation());
    assertThat(patchedRepairSchedule.getRepairUnitId()).isNotNull().isEqualTo(repairSchedule.getRepairUnitId());
    assertThat(patchedRepairSchedule.getId()).isNotNull().isEqualTo(repairSchedule.getId());
    assertThat(patchedRepairSchedule.getOwner()).isNotNull().isEqualTo(repairSchedule.getOwner());
    assertThat(patchedRepairSchedule.getRepairParallelism())
        .isNotNull()
        .isEqualTo(repairSchedule.getRepairParallelism());
    assertThat(patchedRepairSchedule.getIntensity()).isNotNull().isEqualTo(repairSchedule.getIntensity());
    assertThat(patchedRepairSchedule.getDaysBetween()).isNotNull().isEqualTo(repairSchedule.getDaysBetween());
    assertThat(patchedRepairSchedule.getSegmentCountPerNode())
        .isNotNull()
        .isEqualTo(repairSchedule.getSegmentCountPerNode());
  }

  @Test
  public void testApplyRepairPatchParams() {
    RepairSchedule repairSchedule = buildBasicTestRepairSchedule();

    RepairSchedule patchedRepairSchedule = RepairScheduleResource.applyRepairPatchParams(
        repairSchedule,
        "test2",
        RepairParallelism.SEQUENTIAL,
        0.0D,
        2,
        3,
        false,
        null
    );

    assertThat(patchedRepairSchedule.getNextActivation()).isNotNull().isEqualTo(repairSchedule.getNextActivation());
    assertThat(patchedRepairSchedule.getRepairUnitId()).isNotNull().isEqualTo(repairSchedule.getRepairUnitId());
    assertThat(patchedRepairSchedule.getId()).isNotNull().isEqualTo(repairSchedule.getId());
    assertThat(patchedRepairSchedule.getOwner()).isNotNull().isEqualTo("test2");
    assertThat(patchedRepairSchedule.getRepairParallelism()).isNotNull().isEqualTo(RepairParallelism.SEQUENTIAL);
    assertThat(patchedRepairSchedule.getIntensity()).isNotNull().isEqualTo(0.0D);
    assertThat(patchedRepairSchedule.getDaysBetween()).isNotNull().isEqualTo(2);
    assertThat(patchedRepairSchedule.getSegmentCountPerNode()).isNotNull().isEqualTo(3);
  }

  private static final class MockObjects {
    private AppContext context;
    private UriInfo uriInfo;
    private RepairSchedule repairSchedule;
    private RepairUnit repairUnit;

    MockObjects() {
      context = null;
      uriInfo = null;
      repairSchedule = null;
      repairUnit = null;
    }

    MockObjects(AppContext context, UriInfo uriInfo) {
      this.context = context;
      this.uriInfo = uriInfo;
    }

    public AppContext getContext() {
      return context;
    }

    public void setContext(AppContext context) {
      this.context = context;
    }

    public UriInfo getUriInfo() {
      return uriInfo;
    }

    public void setUriInfo(UriInfo uriInfo) {
      this.uriInfo = uriInfo;
    }

    public RepairSchedule getRepairSchedule() {
      return repairSchedule;
    }

    public void setRepairSchedule(RepairSchedule repairSchedule) {
      this.repairSchedule = repairSchedule;
    }

    public RepairUnit getRepairUnit() {
      return repairUnit;
    }

    public void setRepairUnit(RepairUnit repairUnit) {
      this.repairUnit = repairUnit;
    }
  }
}