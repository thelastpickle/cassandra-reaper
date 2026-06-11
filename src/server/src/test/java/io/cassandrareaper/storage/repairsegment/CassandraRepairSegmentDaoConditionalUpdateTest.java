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

package io.cassandrareaper.storage.repairsegment;

import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.Segment;
import io.cassandrareaper.service.RingRange;
import io.cassandrareaper.storage.cassandra.CassandraConcurrencyDao;
import io.cassandrareaper.storage.repairunit.CassandraRepairUnitDao;

import java.math.BigInteger;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for CassandraRepairSegmentDao.updateRepairSegmentStateConditional() method. Tests the
 * conditional update logic with LWT (lightweight transactions) for topology change segment
 * splitting feature.
 */
public class CassandraRepairSegmentDaoConditionalUpdateTest {

  private CqlSession mockSession;
  private CassandraConcurrencyDao mockConcurrencyDao;
  private CassandraRepairUnitDao mockRepairUnitDao;
  private CassandraRepairSegmentDao dao;
  private PreparedStatement mockGetSegmentStmt;
  private PreparedStatement mockConditionalUpdateStmt;
  private BoundStatement mockBoundStatement;
  private ResultSet mockResultSet;
  private Row mockRow;
  private UUID runId;
  private UUID segmentId;
  private UUID repairUnitId;

  @Before
  public void setUp() {
    mockSession = mock(CqlSession.class);
    mockConcurrencyDao = mock(CassandraConcurrencyDao.class);
    mockRepairUnitDao = mock(CassandraRepairUnitDao.class);
    mockGetSegmentStmt = mock(PreparedStatement.class);
    mockConditionalUpdateStmt = mock(PreparedStatement.class);
    mockBoundStatement = mock(BoundStatement.class);
    mockResultSet = mock(ResultSet.class);
    mockRow = mock(Row.class);

    runId = UUID.randomUUID();
    segmentId = Uuids.timeBased();
    repairUnitId = UUID.randomUUID();

    // Mock the session.prepare() calls for various statements
    when(mockSession.prepare(any(SimpleStatement.class))).thenReturn(mockConditionalUpdateStmt);
    when(mockConditionalUpdateStmt.bind(any(), any(), any(), any(), any(), any()))
        .thenReturn(mockBoundStatement);
    when(mockConditionalUpdateStmt.bind(any(), any(), any(), any(), any()))
        .thenReturn(mockBoundStatement);
    when(mockConditionalUpdateStmt.bind(any(), any(), any(), any())).thenReturn(mockBoundStatement);
    when(mockSession.execute(any(BoundStatement.class))).thenReturn(mockResultSet);

    dao = new CassandraRepairSegmentDao(mockConcurrencyDao, mockRepairUnitDao, mockSession);
    dao.getRepairSegmentPrepStmt = mockGetSegmentStmt;
  }

  private RepairSegment createTestSegment(
      RepairSegment.State state, DateTime startTime, DateTime endTime) {
    Map<String, String> replicas = new HashMap<>();
    replicas.put("node1", "dc1");

    RepairSegment.Builder builder =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(
                        new RingRange(BigInteger.valueOf(1000), BigInteger.valueOf(2000)))
                    .withReplicas(replicas)
                    .build(),
                repairUnitId)
            .withRunId(runId)
            .withId(segmentId)
            .withState(state);

    if (startTime != null) {
      builder.withStartTime(startTime);
    }
    if (endTime != null) {
      builder.withEndTime(endTime);
    }

    return builder.build();
  }

  private void mockGetRepairSegment(RepairSegment segment) {
    when(mockGetSegmentStmt.bind(runId, segmentId)).thenReturn(mockBoundStatement);
    when(mockResultSet.one()).thenReturn(mockRow);

    // Mock the row data for segment retrieval
    when(mockRow.getUuid("id")).thenReturn(runId);
    when(mockRow.getUuid("segment_id")).thenReturn(segmentId);
    when(mockRow.getUuid("repair_unit_id")).thenReturn(repairUnitId);
    when(mockRow.getInt("segment_state")).thenReturn(segment.getState().ordinal());
    when(mockRow.getString("token_ranges")).thenReturn(null);
    when(mockRow.getBigInteger("start_token")).thenReturn(segment.getStartToken());
    when(mockRow.getBigInteger("end_token")).thenReturn(segment.getEndToken());
    when(mockRow.getInt("fail_count")).thenReturn(0);
    when(mockRow.getString("coordinator_host")).thenReturn("node1");
    when(mockRow.getString("replicas")).thenReturn("{\"node1\":\"dc1\"}");

    if (segment.getStartTime() != null) {
      when(mockRow.getInstant("segment_start_time"))
          .thenReturn(Instant.ofEpochMilli(segment.getStartTime().getMillis()));
    } else {
      when(mockRow.getInstant("segment_start_time")).thenReturn(null);
    }

    if (segment.getEndTime() != null) {
      when(mockRow.getInstant("segment_end_time"))
          .thenReturn(Instant.ofEpochMilli(segment.getEndTime().getMillis()));
    } else {
      when(mockRow.getInstant("segment_end_time")).thenReturn(null);
    }
  }

  @Test
  public void testDoneStateWithNullTimestamps_setsBothTimestamps() {
    // Create segment in NOT_STARTED state with no timestamps
    RepairSegment segment = createTestSegment(RepairSegment.State.NOT_STARTED, null, null);
    mockGetRepairSegment(segment);

    // Mock LWT success
    Row lwtRow = mock(Row.class);
    when(lwtRow.getBoolean("[applied]")).thenReturn(true);
    ResultSet lwtResultSet = mock(ResultSet.class);
    when(lwtResultSet.one()).thenReturn(lwtRow);
    when(mockSession.execute(any(BoundStatement.class)))
        .thenReturn(mockResultSet) // First call for getRepairSegment
        .thenReturn(lwtResultSet); // Second call for conditional update

    // Execute conditional update to DONE
    boolean result =
        dao.updateRepairSegmentStateConditional(
            runId, segmentId, RepairSegment.State.DONE, RepairSegment.State.NOT_STARTED);

    assertTrue("Update should succeed", result);

    // Verify that session.execute was called (once for getRepairSegment, once for conditional
    // update)
    verify(mockSession, times(2)).execute(any(BoundStatement.class));
  }

  @Test
  public void testDoneStateWithExistingTimestamps_preservesTimestamps() {
    // Create segment with existing timestamps
    DateTime existingStart = DateTime.now().minusMinutes(10);
    DateTime existingEnd = DateTime.now().minusMinutes(5);
    RepairSegment segment = createTestSegment(RepairSegment.State.RUNNING, existingStart, null);
    mockGetRepairSegment(segment);

    // Mock LWT success
    Row lwtRow = mock(Row.class);
    when(lwtRow.getBoolean("[applied]")).thenReturn(true);
    ResultSet lwtResultSet = mock(ResultSet.class);
    when(lwtResultSet.one()).thenReturn(lwtRow);
    when(mockSession.execute(any(BoundStatement.class)))
        .thenReturn(mockResultSet)
        .thenReturn(lwtResultSet);

    // Execute conditional update to DONE
    boolean result =
        dao.updateRepairSegmentStateConditional(
            runId, segmentId, RepairSegment.State.DONE, RepairSegment.State.RUNNING);

    assertTrue("Update should succeed", result);
  }

  @Test
  public void testRunningStateWithNullStartTime_setsStartTime() {
    // Create segment in NOT_STARTED state with no timestamps
    RepairSegment segment = createTestSegment(RepairSegment.State.NOT_STARTED, null, null);
    mockGetRepairSegment(segment);

    // Mock LWT success
    Row lwtRow = mock(Row.class);
    when(lwtRow.getBoolean("[applied]")).thenReturn(true);
    ResultSet lwtResultSet = mock(ResultSet.class);
    when(lwtResultSet.one()).thenReturn(lwtRow);
    when(mockSession.execute(any(BoundStatement.class)))
        .thenReturn(mockResultSet)
        .thenReturn(lwtResultSet);

    // Execute conditional update to RUNNING
    boolean result =
        dao.updateRepairSegmentStateConditional(
            runId, segmentId, RepairSegment.State.RUNNING, RepairSegment.State.NOT_STARTED);

    assertTrue("Update should succeed", result);
  }

  @Test
  public void testRunningStateWithExistingStartTime_preservesStartTime() {
    // Create segment with existing start time
    DateTime existingStart = DateTime.now().minusMinutes(10);
    RepairSegment segment = createTestSegment(RepairSegment.State.NOT_STARTED, existingStart, null);
    mockGetRepairSegment(segment);

    // Mock LWT success
    Row lwtRow = mock(Row.class);
    when(lwtRow.getBoolean("[applied]")).thenReturn(true);
    ResultSet lwtResultSet = mock(ResultSet.class);
    when(lwtResultSet.one()).thenReturn(lwtRow);
    when(mockSession.execute(any(BoundStatement.class)))
        .thenReturn(mockResultSet)
        .thenReturn(lwtResultSet);

    // Execute conditional update to RUNNING
    boolean result =
        dao.updateRepairSegmentStateConditional(
            runId, segmentId, RepairSegment.State.RUNNING, RepairSegment.State.NOT_STARTED);

    assertTrue("Update should succeed", result);
  }

  @Test
  public void testDefaultStateTransition_noTimestamps() {
    // Create segment in RUNNING state
    RepairSegment segment = createTestSegment(RepairSegment.State.RUNNING, DateTime.now(), null);
    mockGetRepairSegment(segment);

    // Mock LWT success
    Row lwtRow = mock(Row.class);
    when(lwtRow.getBoolean("[applied]")).thenReturn(true);
    ResultSet lwtResultSet = mock(ResultSet.class);
    when(lwtResultSet.one()).thenReturn(lwtRow);
    when(mockSession.execute(any(BoundStatement.class)))
        .thenReturn(mockResultSet)
        .thenReturn(lwtResultSet);

    // Execute conditional update to NOT_STARTED (unusual but tests default path)
    boolean result =
        dao.updateRepairSegmentStateConditional(
            runId, segmentId, RepairSegment.State.NOT_STARTED, RepairSegment.State.RUNNING);

    assertTrue("Update should succeed", result);
  }

  @Test
  public void testSegmentNotFound_returnsFalse() {
    // Mock segment not found
    when(mockGetSegmentStmt.bind(runId, segmentId)).thenReturn(mockBoundStatement);
    when(mockResultSet.one()).thenReturn(null);

    // Execute conditional update
    boolean result =
        dao.updateRepairSegmentStateConditional(
            runId, segmentId, RepairSegment.State.DONE, RepairSegment.State.NOT_STARTED);

    assertFalse("Update should fail when segment not found", result);
  }

  @Test
  public void testStateMismatch_returnsFalse() {
    // Create segment in RUNNING state
    RepairSegment segment = createTestSegment(RepairSegment.State.RUNNING, DateTime.now(), null);
    mockGetRepairSegment(segment);

    // Try to update from NOT_STARTED (but segment is RUNNING)
    boolean result =
        dao.updateRepairSegmentStateConditional(
            runId, segmentId, RepairSegment.State.DONE, RepairSegment.State.NOT_STARTED);

    assertFalse("Update should fail when state doesn't match", result);
  }

  @Test
  public void testLwtAppliedTrue_returnsTrue() {
    // Create segment in NOT_STARTED state
    RepairSegment segment = createTestSegment(RepairSegment.State.NOT_STARTED, null, null);
    mockGetRepairSegment(segment);

    // Mock LWT success (applied=true)
    Row lwtRow = mock(Row.class);
    when(lwtRow.getBoolean("[applied]")).thenReturn(true);
    ResultSet lwtResultSet = mock(ResultSet.class);
    when(lwtResultSet.one()).thenReturn(lwtRow);
    when(mockSession.execute(any(BoundStatement.class)))
        .thenReturn(mockResultSet)
        .thenReturn(lwtResultSet);

    // Execute conditional update
    boolean result =
        dao.updateRepairSegmentStateConditional(
            runId, segmentId, RepairSegment.State.DONE, RepairSegment.State.NOT_STARTED);

    assertTrue("Update should succeed when LWT applied=true", result);
  }

  @Test
  public void testLwtAppliedFalse_returnsFalse() {
    // Create segment in NOT_STARTED state
    RepairSegment segment = createTestSegment(RepairSegment.State.NOT_STARTED, null, null);
    mockGetRepairSegment(segment);

    // Mock LWT failure (applied=false)
    Row lwtRow = mock(Row.class);
    when(lwtRow.getBoolean("[applied]")).thenReturn(false);
    ResultSet lwtResultSet = mock(ResultSet.class);
    when(lwtResultSet.one()).thenReturn(lwtRow);
    when(mockSession.execute(any(BoundStatement.class)))
        .thenReturn(mockResultSet)
        .thenReturn(lwtResultSet);

    // Execute conditional update
    boolean result =
        dao.updateRepairSegmentStateConditional(
            runId, segmentId, RepairSegment.State.DONE, RepairSegment.State.NOT_STARTED);

    assertFalse("Update should fail when LWT applied=false", result);
  }

  @Test
  public void testNullLwtRow_returnsFalse() {
    // Create segment in NOT_STARTED state
    RepairSegment segment = createTestSegment(RepairSegment.State.NOT_STARTED, null, null);
    mockGetRepairSegment(segment);

    // Mock null row from LWT result
    ResultSet lwtResultSet = mock(ResultSet.class);
    when(lwtResultSet.one()).thenReturn(null);
    when(mockSession.execute(any(BoundStatement.class)))
        .thenReturn(mockResultSet)
        .thenReturn(lwtResultSet);

    // Execute conditional update
    boolean result =
        dao.updateRepairSegmentStateConditional(
            runId, segmentId, RepairSegment.State.DONE, RepairSegment.State.NOT_STARTED);

    assertFalse("Update should fail when LWT returns null row", result);
  }
}
