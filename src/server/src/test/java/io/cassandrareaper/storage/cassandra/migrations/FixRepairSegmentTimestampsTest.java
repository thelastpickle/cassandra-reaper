/*
 * Copyright 2017-2017 Spotify AB
 * Copyright 2017-2019 The Last Pickle Ltd
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

package io.cassandrareaper.storage.cassandra.migrations;

import io.cassandrareaper.core.RepairSegment;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public final class FixRepairSegmentTimestampsTest {

  @Mock private CqlSession mockSession;
  @Mock private ResultSet mockResultSet;
  @Mock private Row mockRow;
  @Mock private PreparedStatement mockPreparedStatement;
  @Mock private BoundStatement mockBoundStatement;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    when(mockSession.execute(any(SimpleStatement.class))).thenReturn(mockResultSet);
    when(mockSession.prepare(any(SimpleStatement.class))).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.bind(any(), any(), any(), any())).thenReturn(mockBoundStatement);
  }

  @Test
  public void testMigrateWithNoRows_ShouldCompleteWithoutUpdates() {
    // Setup - No rows in result set
    when(mockResultSet.iterator()).thenReturn(Collections.emptyIterator());

    // Execute
    FixRepairSegmentTimestamps.migrate(mockSession);

    // Verify - No async executions should happen
    verify(mockSession, never()).executeAsync(any(BoundStatement.class));
  }

  @Test
  public void testMigrateWithNotStartedSegment_ValidTimestamps_ShouldNotUpdate() {
    // Setup - NOT_STARTED segment with null start_time (correct)
    setupMockRow(RepairSegment.State.NOT_STARTED, null, null);
    when(mockResultSet.iterator()).thenReturn(Arrays.asList(mockRow).iterator());

    // Execute
    FixRepairSegmentTimestamps.migrate(mockSession);

    // Verify - No updates needed
    verify(mockSession, never()).executeAsync(any(BoundStatement.class));
  }

  @Test
  public void testMigrateWithRunningSegment_NullStartTime_ShouldUpdate() {
    // Setup - RUNNING segment with null start_time (incorrect)
    setupMockRow(RepairSegment.State.RUNNING, null, null);
    when(mockResultSet.iterator()).thenReturn(Arrays.asList(mockRow).iterator());

    // Execute
    FixRepairSegmentTimestamps.migrate(mockSession);

    // Verify - Should update to set start_time to EPOCH
    verify(mockSession, times(1)).executeAsync(mockBoundStatement);
  }

  @Test
  public void testMigrateWithDoneSegment_NullEndTime_ShouldUpdate() {
    // Setup - DONE segment with null end_time (incorrect)
    Instant startTime = Instant.now();
    setupMockRow(RepairSegment.State.DONE, startTime, null);
    when(mockResultSet.iterator()).thenReturn(Arrays.asList(mockRow).iterator());

    // Execute
    FixRepairSegmentTimestamps.migrate(mockSession);

    // Verify - Should update to set end_time
    verify(mockSession, times(1)).executeAsync(mockBoundStatement);
  }

  @Test
  public void testMigrateWithRunningSegment_InvalidEndTime_ShouldUpdate() {
    // Setup - RUNNING segment with non-null end_time (incorrect)
    Instant startTime = Instant.now();
    Instant invalidEndTime = Instant.now();
    setupMockRow(RepairSegment.State.RUNNING, startTime, invalidEndTime);
    when(mockResultSet.iterator()).thenReturn(Arrays.asList(mockRow).iterator());

    // Execute
    FixRepairSegmentTimestamps.migrate(mockSession);

    // Verify - Should update to clear invalid end_time
    verify(mockSession, times(1)).executeAsync(mockBoundStatement);
  }

  @Test
  public void testMigrateWithValidDoneSegment_ShouldNotUpdate() {
    // Setup - DONE segment with valid timestamps
    Instant startTime = Instant.now();
    Instant endTime = Instant.now();
    setupMockRow(RepairSegment.State.DONE, startTime, endTime);
    when(mockResultSet.iterator()).thenReturn(Arrays.asList(mockRow).iterator());

    // Execute
    FixRepairSegmentTimestamps.migrate(mockSession);

    // Verify - No updates needed for valid state
    verify(mockSession, never()).executeAsync(any(BoundStatement.class));
  }

  @Test
  public void testMigrateWithValidRunningSegment_ShouldNotUpdate() {
    // Setup - RUNNING segment with valid timestamps
    Instant startTime = Instant.now();
    setupMockRow(RepairSegment.State.RUNNING, startTime, null);
    when(mockResultSet.iterator()).thenReturn(Arrays.asList(mockRow).iterator());

    // Execute
    FixRepairSegmentTimestamps.migrate(mockSession);

    // Verify - No updates needed for valid state
    verify(mockSession, never()).executeAsync(any(BoundStatement.class));
  }

  @Test
  public void testMigrateWithMultipleRows_ShouldProcessAll() {
    // Setup - Multiple rows with different issues
    Row row1 = mockRow;
    Row row2 = org.mockito.Mockito.mock(Row.class);

    // Row 1: RUNNING with null start_time (needs fix)
    when(row1.getUuid("id")).thenReturn(UUID.randomUUID());
    when(row1.getUuid("segment_id")).thenReturn(UUID.randomUUID());
    when(row1.getInt("segment_state")).thenReturn(RepairSegment.State.RUNNING.ordinal());
    when(row1.getInstant("segment_start_time")).thenReturn(null);
    when(row1.getInstant("segment_end_time")).thenReturn(null);

    // Row 2: DONE with valid timestamps (no fix needed)
    when(row2.getUuid("id")).thenReturn(UUID.randomUUID());
    when(row2.getUuid("segment_id")).thenReturn(UUID.randomUUID());
    when(row2.getInt("segment_state")).thenReturn(RepairSegment.State.DONE.ordinal());
    when(row2.getInstant("segment_start_time")).thenReturn(Instant.now());
    when(row2.getInstant("segment_end_time")).thenReturn(Instant.now());

    when(mockResultSet.iterator()).thenReturn(Arrays.asList(row1, row2).iterator());

    // Execute
    FixRepairSegmentTimestamps.migrate(mockSession);

    // Verify - Only row1 should be updated
    verify(mockSession, times(1)).executeAsync(mockBoundStatement);
  }

  @Test
  public void testMigrateWithDoneSegmentNullStartTime_ShouldSetEndTimeToStartTime() {
    // Setup - DONE segment with null end_time, migration sets end_time = start_time
    Instant startTime = Instant.now();
    setupMockRow(RepairSegment.State.DONE, startTime, null);
    when(mockResultSet.iterator()).thenReturn(Arrays.asList(mockRow).iterator());

    // Execute
    FixRepairSegmentTimestamps.migrate(mockSession);

    // Verify - Should update to set end_time to start_time
    verify(mockSession, times(1)).executeAsync(mockBoundStatement);
  }

  private void setupMockRow(RepairSegment.State state, Instant startTime, Instant endTime) {
    when(mockRow.getUuid("id")).thenReturn(UUID.randomUUID());
    when(mockRow.getUuid("segment_id")).thenReturn(UUID.randomUUID());
    when(mockRow.getInt("segment_state")).thenReturn(state.ordinal());
    when(mockRow.getInstant("segment_start_time")).thenReturn(startTime);
    when(mockRow.getInstant("segment_end_time")).thenReturn(endTime);
  }
}
