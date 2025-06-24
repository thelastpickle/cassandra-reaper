/*
 *
 * Copyright 2019-2019 The Last Pickle Ltd
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

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
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

public final class FixRepairRunTimestampsTest {

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
    FixRepairRunTimestamps.migrate(mockSession);

    // Verify - No async executions should happen
    verify(mockSession, never()).executeAsync(any(BoundStatement.class));
  }

  @Test
  public void testMigrateWithNotStartedState_ValidTimestamps_ShouldNotUpdate() {
    // Setup - NOT_STARTED state with null start_time (correct)
    setupMockRow("NOT_STARTED", null, null, null);
    when(mockResultSet.iterator()).thenReturn(Arrays.asList(mockRow).iterator());

    // Execute
    FixRepairRunTimestamps.migrate(mockSession);

    // Verify - No updates needed
    verify(mockSession, never()).executeAsync(any(BoundStatement.class));
  }

  @Test
  public void testMigrateWithNotStartedState_InvalidStartTime_ShouldUpdate() {
    // Setup - NOT_STARTED state with non-null start_time (incorrect)
    Instant invalidStartTime = Instant.now();
    setupMockRow("NOT_STARTED", invalidStartTime, null, null);
    when(mockResultSet.iterator()).thenReturn(Arrays.asList(mockRow).iterator());

    // Execute
    FixRepairRunTimestamps.migrate(mockSession);

    // Verify - Should update to fix invalid start_time
    verify(mockSession, times(1)).executeAsync(mockBoundStatement);
  }

  @Test
  public void testMigrateWithRunningState_NullStartTime_ShouldUpdate() {
    // Setup - RUNNING state with null start_time (incorrect)
    setupMockRow("RUNNING", null, null, null);
    when(mockResultSet.iterator()).thenReturn(Arrays.asList(mockRow).iterator());

    // Execute
    FixRepairRunTimestamps.migrate(mockSession);

    // Verify - Should update to set start_time to EPOCH
    verify(mockSession, times(1)).executeAsync(mockBoundStatement);
  }

  @Test
  public void testMigrateWithPausedState_NullPauseTime_ShouldUpdate() {
    // Setup - PAUSED state with null pause_time (incorrect)
    Instant startTime = Instant.now();
    setupMockRow("PAUSED", startTime, null, null);
    when(mockResultSet.iterator()).thenReturn(Arrays.asList(mockRow).iterator());

    // Execute
    FixRepairRunTimestamps.migrate(mockSession);

    // Verify - Should update to set pause_time
    verify(mockSession, times(1)).executeAsync(mockBoundStatement);
  }

  @Test
  public void testMigrateWithRunningState_InvalidPauseTime_ShouldUpdate() {
    // Setup - RUNNING state with non-null pause_time (incorrect)
    Instant startTime = Instant.now();
    Instant invalidPauseTime = Instant.now();
    setupMockRow("RUNNING", startTime, invalidPauseTime, null);
    when(mockResultSet.iterator()).thenReturn(Arrays.asList(mockRow).iterator());

    // Execute
    FixRepairRunTimestamps.migrate(mockSession);

    // Verify - Should update to clear invalid pause_time
    verify(mockSession, times(1)).executeAsync(mockBoundStatement);
  }

  @Test
  public void testMigrateWithDoneState_NullEndTime_ShouldUpdate() {
    // Setup - DONE state with null end_time (incorrect)
    Instant startTime = Instant.now();
    setupMockRow("DONE", startTime, null, null);
    when(mockResultSet.iterator()).thenReturn(Arrays.asList(mockRow).iterator());

    // Execute
    FixRepairRunTimestamps.migrate(mockSession);

    // Verify - Should update to set end_time
    verify(mockSession, times(1)).executeAsync(mockBoundStatement);
  }

  @Test
  public void testMigrateWithRunningState_InvalidEndTime_ShouldUpdate() {
    // Setup - RUNNING state with non-null end_time (incorrect)
    Instant startTime = Instant.now();
    Instant invalidEndTime = Instant.now();
    setupMockRow("RUNNING", startTime, null, invalidEndTime);
    when(mockResultSet.iterator()).thenReturn(Arrays.asList(mockRow).iterator());

    // Execute
    FixRepairRunTimestamps.migrate(mockSession);

    // Verify - Should update to clear invalid end_time
    verify(mockSession, times(1)).executeAsync(mockBoundStatement);
  }

  @Test
  public void testMigrateWithErrorState_NullEndTime_ShouldUpdate() {
    // Setup - ERROR state (terminated) with null end_time (incorrect)
    Instant startTime = Instant.now();
    setupMockRow("ERROR", startTime, null, null);
    when(mockResultSet.iterator()).thenReturn(Arrays.asList(mockRow).iterator());

    // Execute
    FixRepairRunTimestamps.migrate(mockSession);

    // Verify - Should update to set end_time for terminated state
    verify(mockSession, times(1)).executeAsync(mockBoundStatement);
  }

  @Test
  public void testMigrateWithAbortedState_NullEndTime_ShouldUpdate() {
    // Setup - ABORTED state (terminated) with null end_time (incorrect)
    Instant startTime = Instant.now();
    setupMockRow("ABORTED", startTime, null, null);
    when(mockResultSet.iterator()).thenReturn(Arrays.asList(mockRow).iterator());

    // Execute
    FixRepairRunTimestamps.migrate(mockSession);

    // Verify - Should update to set end_time for terminated state
    verify(mockSession, times(1)).executeAsync(mockBoundStatement);
  }

  @Test
  public void testMigrateWithValidRunningState_ShouldNotUpdate() {
    // Setup - RUNNING state with valid timestamps
    Instant startTime = Instant.now();
    setupMockRow("RUNNING", startTime, null, null);
    when(mockResultSet.iterator()).thenReturn(Arrays.asList(mockRow).iterator());

    // Execute
    FixRepairRunTimestamps.migrate(mockSession);

    // Verify - No updates needed for valid state
    verify(mockSession, never()).executeAsync(any(BoundStatement.class));
  }

  @Test
  public void testMigrateWithValidPausedState_ShouldNotUpdate() {
    // Setup - PAUSED state with valid timestamps
    Instant startTime = Instant.now();
    Instant pauseTime = Instant.now();
    setupMockRow("PAUSED", startTime, pauseTime, null);
    when(mockResultSet.iterator()).thenReturn(Arrays.asList(mockRow).iterator());

    // Execute
    FixRepairRunTimestamps.migrate(mockSession);

    // Verify - No updates needed for valid state
    verify(mockSession, never()).executeAsync(any(BoundStatement.class));
  }

  @Test
  public void testMigrateWithValidDoneState_ShouldNotUpdate() {
    // Setup - DONE state with valid timestamps
    Instant startTime = Instant.now();
    Instant endTime = Instant.now();
    setupMockRow("DONE", startTime, null, endTime);
    when(mockResultSet.iterator()).thenReturn(Arrays.asList(mockRow).iterator());

    // Execute
    FixRepairRunTimestamps.migrate(mockSession);

    // Verify - No updates needed for valid state
    verify(mockSession, never()).executeAsync(any(BoundStatement.class));
  }

  @Test
  public void testMigrateWithMultipleRows_ShouldProcessAll() {
    // Setup - Multiple rows with different issues
    Row row1 = mock(Row.class);
    Row row2 = mock(Row.class);

    // Row 1: NOT_STARTED with invalid start_time
    when(row1.getUuid("id")).thenReturn(UUID.randomUUID());
    when(row1.getString("state")).thenReturn("NOT_STARTED");
    when(row1.getInstant("start_time")).thenReturn(Instant.now());
    when(row1.getInstant("pause_time")).thenReturn(null);
    when(row1.getInstant("end_time")).thenReturn(null);

    // Row 2: RUNNING with valid timestamps
    when(row2.getUuid("id")).thenReturn(UUID.randomUUID());
    when(row2.getString("state")).thenReturn("RUNNING");
    when(row2.getInstant("start_time")).thenReturn(Instant.now());
    when(row2.getInstant("pause_time")).thenReturn(null);
    when(row2.getInstant("end_time")).thenReturn(null);

    when(mockResultSet.iterator()).thenReturn(Arrays.asList(row1, row2).iterator());

    // Execute
    FixRepairRunTimestamps.migrate(mockSession);

    // Verify - Only row1 should be updated
    verify(mockSession, times(1)).executeAsync(mockBoundStatement);
  }

  private void setupMockRow(String state, Instant startTime, Instant pauseTime, Instant endTime) {
    when(mockRow.getUuid("id")).thenReturn(UUID.randomUUID());
    when(mockRow.getString("state")).thenReturn(state);
    when(mockRow.getInstant("start_time")).thenReturn(startTime);
    when(mockRow.getInstant("pause_time")).thenReturn(pauseTime);
    when(mockRow.getInstant("end_time")).thenReturn(endTime);
  }
}
