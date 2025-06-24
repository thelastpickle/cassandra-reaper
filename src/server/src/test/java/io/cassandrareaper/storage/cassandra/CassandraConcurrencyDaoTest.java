/*
 * Copyright 2016-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
 * Copyright 2020-2020 DataStax, Inc.
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

package io.cassandrareaper.storage.cassandra;

import io.cassandrareaper.core.RepairSegment;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public final class CassandraConcurrencyDaoTest {

  @Mock private CqlSession mockSession;
  @Mock private PreparedStatement mockPreparedStatement;
  @Mock private PreparedStatement mockGetRunningReapersCountPrepStmt;
  @Mock private BoundStatement mockBoundStatement;
  @Mock private ResultSet mockResultSet;
  @Mock private Row mockRow;
  @Mock private ColumnDefinitions mockColumnDefinitions;
  @Mock private RepairSegment mockRepairSegment;

  private CassandraConcurrencyDao concurrencyDao;
  private final UUID reaperInstanceId = UUID.randomUUID();
  private final Version version = Version.parse("3.11.0");

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    // Mock prepared statement creation
    when(mockSession.prepare(any(SimpleStatement.class))).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.bind(any())).thenReturn(mockBoundStatement);
    when(mockPreparedStatement.bind()).thenReturn(mockBoundStatement);
    when(mockGetRunningReapersCountPrepStmt.bind()).thenReturn(mockBoundStatement);

    concurrencyDao = new CassandraConcurrencyDao(version, reaperInstanceId, mockSession);

    // Use reflection to set the specific prepared statement field that's used by
    // getRunningReapers()
    java.lang.reflect.Field field =
        CassandraConcurrencyDao.class.getDeclaredField("getRunningReapersCountPrepStmt");
    field.setAccessible(true);
    field.set(concurrencyDao, mockGetRunningReapersCountPrepStmt);
  }

  @Test
  public void testConstructorPreparesStatements() {
    // Verify that constructor prepares all required statements
    verify(mockSession, times(5)).prepare(any(SimpleStatement.class));
  }

  @Test
  public void testTakeLeadSuccess() {
    // Setup
    UUID leaderId = UUID.randomUUID();
    when(mockSession.execute(mockBoundStatement)).thenReturn(mockResultSet);
    when(mockResultSet.wasApplied()).thenReturn(true);

    // Execute
    boolean result = concurrencyDao.takeLead(leaderId);

    // Verify
    assertTrue(result);
    verify(mockSession).execute(mockBoundStatement);
  }

  @Test
  public void testTakeLeadFailure() {
    // Setup
    UUID leaderId = UUID.randomUUID();
    when(mockSession.execute(mockBoundStatement)).thenReturn(mockResultSet);
    when(mockResultSet.wasApplied()).thenReturn(false);

    // Execute
    boolean result = concurrencyDao.takeLead(leaderId);

    // Verify
    assertFalse(result);
    verify(mockSession).execute(mockBoundStatement);
  }

  @Test
  public void testTakeLeadWithCustomTtl() {
    // Setup
    UUID leaderId = UUID.randomUUID();
    int customTtl = 120;
    when(mockSession.execute(mockBoundStatement)).thenReturn(mockResultSet);
    when(mockResultSet.wasApplied()).thenReturn(true);

    // Execute
    boolean result = concurrencyDao.takeLead(leaderId, customTtl);

    // Verify
    assertTrue(result);
    verify(mockSession).execute(mockBoundStatement);
  }

  @Test
  public void testRenewLeadSuccess() {
    // Setup
    UUID leaderId = UUID.randomUUID();
    when(mockSession.execute(mockBoundStatement)).thenReturn(mockResultSet);
    when(mockResultSet.wasApplied()).thenReturn(true);

    // Execute
    boolean result = concurrencyDao.renewLead(leaderId);

    // Verify
    assertTrue(result);
    verify(mockSession).execute(mockBoundStatement);
  }

  @Test(expected = AssertionError.class)
  public void testRenewLeadFailure() {
    // Setup
    UUID leaderId = UUID.randomUUID();
    when(mockSession.execute(mockBoundStatement)).thenReturn(mockResultSet);
    when(mockResultSet.wasApplied()).thenReturn(false);

    // Execute - the method should throw an AssertionError when renewal fails
    // because of the assert statement in the renewLead method
    concurrencyDao.renewLead(leaderId);

    // Note: verify won't be reached due to the expected exception, but the method does execute
  }

  @Test
  public void testRenewLeadWithCustomTtl() {
    // Setup
    UUID leaderId = UUID.randomUUID();
    int customTtl = 180;
    when(mockSession.execute(mockBoundStatement)).thenReturn(mockResultSet);
    when(mockResultSet.wasApplied()).thenReturn(true);

    // Execute
    boolean result = concurrencyDao.renewLead(leaderId, customTtl);

    // Verify
    assertTrue(result);
    verify(mockSession).execute(mockBoundStatement);
  }

  @Test
  public void testGetLeaders() {
    // Setup
    UUID leader1 = UUID.randomUUID();
    UUID leader2 = UUID.randomUUID();

    Row row1 = mock(Row.class);
    Row row2 = mock(Row.class);
    when(row1.getUuid("leader_id")).thenReturn(leader1);
    when(row2.getUuid("leader_id")).thenReturn(leader2);

    when(mockSession.execute(any(SimpleStatement.class))).thenReturn(mockResultSet);
    when(mockResultSet.all()).thenReturn(Arrays.asList(row1, row2));

    // Execute
    List<UUID> leaders = concurrencyDao.getLeaders();

    // Verify
    assertEquals(2, leaders.size());
    assertTrue(leaders.contains(leader1));
    assertTrue(leaders.contains(leader2));
  }

  @Test
  public void testGetLeadersEmpty() {
    // Setup
    when(mockSession.execute(any(SimpleStatement.class))).thenReturn(mockResultSet);
    when(mockResultSet.all()).thenReturn(Collections.emptyList());

    // Execute
    List<UUID> leaders = concurrencyDao.getLeaders();

    // Verify
    assertTrue(leaders.isEmpty());
  }

  @Test
  public void testReleaseLead() {
    // Setup
    UUID leaderId = UUID.randomUUID();
    when(mockSession.execute(mockBoundStatement)).thenReturn(mockResultSet);
    when(mockResultSet.wasApplied()).thenReturn(true);

    // Execute
    concurrencyDao.releaseLead(leaderId);

    // Verify
    verify(mockSession).execute(mockBoundStatement);
  }

  @Test
  public void testHasLeadOnSegmentWithRepairSegment() {
    // Setup
    UUID runId = UUID.randomUUID();
    UUID segmentId = UUID.randomUUID();
    Set<String> replicas = new HashSet<>(Arrays.asList("node1", "node2"));

    when(mockRepairSegment.getRunId()).thenReturn(runId);
    when(mockRepairSegment.getId()).thenReturn(segmentId);
    when(mockRepairSegment.getReplicas()).thenReturn(Collections.singletonMap("node1", "token"));

    when(mockSession.execute(any(BatchStatement.class))).thenReturn(mockResultSet);
    when(mockResultSet.wasApplied()).thenReturn(true);

    // Execute
    boolean result = concurrencyDao.hasLeadOnSegment(mockRepairSegment);

    // Verify
    assertTrue(result);
  }

  @Test
  public void testHasLeadOnSegmentWithLeaderId() {
    // Setup
    UUID leaderId = UUID.randomUUID();
    when(mockSession.execute(mockBoundStatement)).thenReturn(mockResultSet);
    when(mockResultSet.wasApplied()).thenReturn(true);

    // Execute
    boolean result = concurrencyDao.hasLeadOnSegment(leaderId);

    // Verify
    assertTrue(result);
  }

  @Test
  public void testCountRunningReapers() {
    // Setup
    UUID reaper1 = UUID.randomUUID();
    UUID reaper2 = UUID.randomUUID();

    Row row1 = mock(Row.class);
    Row row2 = mock(Row.class);
    when(row1.getUuid("reaper_instance_id")).thenReturn(reaper1);
    when(row2.getUuid("reaper_instance_id")).thenReturn(reaper2);

    when(mockSession.execute(mockBoundStatement)).thenReturn(mockResultSet);
    when(mockResultSet.all()).thenReturn(Arrays.asList(row1, row2));

    // Execute
    int count = concurrencyDao.countRunningReapers();

    // Verify
    assertEquals(2, count);
  }

  @Test
  public void testCountRunningReapersWhenEmpty() {
    // Setup
    when(mockSession.execute(mockBoundStatement)).thenReturn(mockResultSet);
    when(mockResultSet.all()).thenReturn(Collections.emptyList());

    // Execute
    int count = concurrencyDao.countRunningReapers();

    // Verify
    assertEquals(1, count); // Should return 1 when no reapers found
  }

  @Test
  public void testGetRunningReapers() {
    // Setup
    UUID reaper1 = UUID.randomUUID();
    UUID reaper2 = UUID.randomUUID();

    Row row1 = mock(Row.class);
    Row row2 = mock(Row.class);
    when(row1.getUuid("reaper_instance_id")).thenReturn(reaper1);
    when(row2.getUuid("reaper_instance_id")).thenReturn(reaper2);

    when(mockSession.execute(mockBoundStatement)).thenReturn(mockResultSet);
    when(mockResultSet.all()).thenReturn(Arrays.asList(row1, row2));

    // Execute
    List<UUID> reapers = concurrencyDao.getRunningReapers();

    // Verify
    assertEquals(2, reapers.size());
    assertTrue(reapers.contains(reaper1));
    assertTrue(reapers.contains(reaper2));
  }

  @Test
  public void testLockRunningRepairsForNodesSuccess() {
    // Setup
    UUID repairId = UUID.randomUUID();
    UUID segmentId = UUID.randomUUID();
    Set<String> replicas = new HashSet<>(Arrays.asList("node1", "node2"));

    when(mockSession.execute(any(BatchStatement.class))).thenReturn(mockResultSet);
    when(mockResultSet.wasApplied()).thenReturn(true);

    // Execute
    boolean result = concurrencyDao.lockRunningRepairsForNodes(repairId, segmentId, replicas);

    // Verify
    assertTrue(result);
    verify(mockSession).execute(any(BatchStatement.class));
  }

  @Test
  public void testLockRunningRepairsForNodesFailure() {
    // Setup
    UUID repairId = UUID.randomUUID();
    UUID segmentId = UUID.randomUUID();
    Set<String> replicas = new HashSet<>(Arrays.asList("node1", "node2"));

    // Mock the result set for failure case - need to mock the iterator for logFailedLead
    Row mockRow = mock(Row.class);
    when(mockRow.getColumnDefinitions()).thenReturn(mockColumnDefinitions);
    when(mockColumnDefinitions.contains(anyString())).thenReturn(true);
    when(mockRow.getString("node")).thenReturn("node1");
    when(mockRow.getString("reaper_instance_host")).thenReturn("host1");
    when(mockRow.getUuid("reaper_instance_id")).thenReturn(UUID.randomUUID());
    when(mockRow.getUuid("segment_id")).thenReturn(segmentId);

    when(mockSession.execute(any(BatchStatement.class))).thenReturn(mockResultSet);
    when(mockResultSet.wasApplied()).thenReturn(false);
    when(mockResultSet.iterator()).thenReturn(Arrays.asList(mockRow).iterator());

    // Execute
    boolean result = concurrencyDao.lockRunningRepairsForNodes(repairId, segmentId, replicas);

    // Verify
    assertFalse(result);
    verify(mockSession).execute(any(BatchStatement.class));
  }

  @Test
  public void testRenewRunningRepairsForNodesSuccess() {
    // Setup
    UUID repairId = UUID.randomUUID();
    UUID segmentId = UUID.randomUUID();
    Set<String> replicas = new HashSet<>(Arrays.asList("node1", "node2"));

    when(mockSession.execute(any(BatchStatement.class))).thenReturn(mockResultSet);
    when(mockResultSet.wasApplied()).thenReturn(true);

    // Execute
    boolean result = concurrencyDao.renewRunningRepairsForNodes(repairId, segmentId, replicas);

    // Verify
    assertTrue(result);
    verify(mockSession).execute(any(BatchStatement.class));
  }

  @Test
  public void testReleaseRunningRepairsForNodes() {
    // Setup
    UUID repairId = UUID.randomUUID();
    UUID segmentId = UUID.randomUUID();
    Set<String> replicas = new HashSet<>(Arrays.asList("node1", "node2"));

    when(mockSession.execute(any(BatchStatement.class))).thenReturn(mockResultSet);
    when(mockResultSet.wasApplied()).thenReturn(true);

    // Execute
    boolean result = concurrencyDao.releaseRunningRepairsForNodes(repairId, segmentId, replicas);

    // Verify
    assertTrue(result);
    verify(mockSession).execute(any(BatchStatement.class));
  }

  @Test
  public void testGetLockedSegmentsForRun() {
    // Setup
    UUID runId = UUID.randomUUID();
    UUID segment1 = UUID.randomUUID();
    UUID segment2 = UUID.randomUUID();

    Row row1 = mock(Row.class);
    Row row2 = mock(Row.class);
    Row row3 = mock(Row.class); // This one will have null reaper_instance_id

    when(row1.getUuid("reaper_instance_id")).thenReturn(UUID.randomUUID());
    when(row1.getUuid("segment_id")).thenReturn(segment1);

    when(row2.getUuid("reaper_instance_id")).thenReturn(UUID.randomUUID());
    when(row2.getUuid("segment_id")).thenReturn(segment2);

    when(row3.getUuid("reaper_instance_id")).thenReturn(null); // Unlocked segment

    when(mockSession.execute(mockBoundStatement)).thenReturn(mockResultSet);
    when(mockResultSet.all()).thenReturn(Arrays.asList(row1, row2, row3));

    // Execute
    Set<UUID> lockedSegments = concurrencyDao.getLockedSegmentsForRun(runId);

    // Verify
    assertEquals(2, lockedSegments.size());
    assertTrue(lockedSegments.contains(segment1));
    assertTrue(lockedSegments.contains(segment2));
  }

  @Test
  public void testGetLockedNodesForRun() {
    // Setup
    UUID runId = UUID.randomUUID();

    Row row1 = mock(Row.class);
    Row row2 = mock(Row.class);
    Row row3 = mock(Row.class); // This one will have null reaper_instance_id

    when(row1.getUuid("reaper_instance_id")).thenReturn(UUID.randomUUID());
    when(row1.getString("node")).thenReturn("node1");

    when(row2.getUuid("reaper_instance_id")).thenReturn(UUID.randomUUID());
    when(row2.getString("node")).thenReturn("node2");

    when(row3.getUuid("reaper_instance_id")).thenReturn(null); // Unlocked node
    when(row3.getString("node")).thenReturn("node3");

    when(mockSession.execute(mockBoundStatement)).thenReturn(mockResultSet);
    when(mockResultSet.all()).thenReturn(Arrays.asList(row1, row2, row3));

    // Execute
    Set<String> lockedNodes = concurrencyDao.getLockedNodesForRun(runId);

    // Verify
    assertEquals(2, lockedNodes.size());
    assertTrue(lockedNodes.contains("node1"));
    assertTrue(lockedNodes.contains("node2"));
    assertFalse(lockedNodes.contains("node3")); // Should not include unlocked node
  }

  @Test
  public void testLogFailedLead() {
    // Setup
    UUID repairId = UUID.randomUUID();
    UUID segmentId = UUID.randomUUID();

    Row row1 = mock(Row.class);
    when(row1.getColumnDefinitions()).thenReturn(mockColumnDefinitions);
    when(mockColumnDefinitions.contains("node")).thenReturn(true);
    when(mockColumnDefinitions.contains("reaper_instance_host")).thenReturn(true);
    when(mockColumnDefinitions.contains("reaper_instance_id")).thenReturn(true);
    when(mockColumnDefinitions.contains("segment_id")).thenReturn(true);

    when(row1.getString("node")).thenReturn("node1");
    when(row1.getString("reaper_instance_host")).thenReturn("host1");
    when(row1.getUuid("reaper_instance_id")).thenReturn(UUID.randomUUID());
    when(row1.getUuid("segment_id")).thenReturn(segmentId);

    when(mockResultSet.iterator()).thenReturn(Arrays.asList(row1).iterator());

    // Execute - should not throw exception
    concurrencyDao.logFailedLead(mockResultSet, repairId, segmentId);

    // Verify - method should complete without error (logging only)
  }

  @Test
  public void testLogFailedLeadWithMissingColumns() {
    // Setup
    UUID repairId = UUID.randomUUID();
    UUID segmentId = UUID.randomUUID();

    Row row1 = mock(Row.class);
    when(row1.getColumnDefinitions()).thenReturn(mockColumnDefinitions);
    when(mockColumnDefinitions.contains(anyString())).thenReturn(false);

    when(mockResultSet.iterator()).thenReturn(Arrays.asList(row1).iterator());

    // Execute - should not throw exception even with missing columns
    concurrencyDao.logFailedLead(mockResultSet, repairId, segmentId);

    // Verify - method should complete without error (logging only)
  }
}
