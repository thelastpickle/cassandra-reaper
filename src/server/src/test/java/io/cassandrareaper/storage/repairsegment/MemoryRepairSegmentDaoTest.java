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

import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.core.Segment;
import io.cassandrareaper.service.RingRange;
import io.cassandrareaper.storage.MemoryStorageFacade;

import java.math.BigInteger;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

public class MemoryRepairSegmentDaoTest {

  private MemoryStorageFacade memoryStorageFacade;
  private MemoryRepairSegmentDao memoryRepairSegmentDao;
  private UUID repairUnitId;
  private UUID repairRunId;

  @Before
  public void setUp() {
    memoryStorageFacade = new MemoryStorageFacade();
    memoryRepairSegmentDao = new MemoryRepairSegmentDao(memoryStorageFacade);

    // Set up a cluster (required for foreign key constraints)
    Cluster cluster =
        Cluster.builder()
            .withName("testCluster")
            .withSeedHosts(Sets.newHashSet("127.0.0.1"))
            .withState(Cluster.State.ACTIVE)
            .build();
    memoryStorageFacade.getClusterDao().addCluster(cluster);

    // Set up a repair unit
    RepairUnit.Builder repairUnitBuilder =
        RepairUnit.builder()
            .clusterName("testCluster")
            .keyspaceName("testKeyspace")
            .columnFamilies(Sets.newHashSet("testTable"))
            .incrementalRepair(false)
            .subrangeIncrementalRepair(false)
            .nodes(Sets.newHashSet("node1", "node2", "node3"))
            .repairThreadCount(1)
            .timeout(30);

    RepairUnit repairUnit = memoryStorageFacade.getRepairUnitDao().addRepairUnit(repairUnitBuilder);
    repairUnitId = repairUnit.getId();

    // Set up a repair run
    RepairRun.Builder repairRunBuilder =
        RepairRun.builder("testCluster", repairUnitId)
            .intensity(0.5)
            .segmentCount(3)
            .repairParallelism(org.apache.cassandra.repair.RepairParallelism.PARALLEL)
            .tables(Sets.newHashSet("testTable"));

    RepairRun repairRun =
        memoryStorageFacade
            .getRepairRunDao()
            .addRepairRun(repairRunBuilder, java.util.Collections.emptyList());
    repairRunId = repairRun.getId();
  }

  @Test
  public void testGetNextFreeSegments_noLockedNodes() {
    // Create segments with different states and replica sets
    createRepairSegments();

    // No locked nodes
    List<RepairSegment> freeSegments = memoryRepairSegmentDao.getNextFreeSegments(repairRunId);

    // Should return only the NOT_STARTED segments (2 out of 4 total segments)
    assertEquals(2, freeSegments.size());
    assertTrue(
        freeSegments.stream().allMatch(seg -> seg.getState() == RepairSegment.State.NOT_STARTED));
  }

  @Test
  public void testGetNextFreeSegments_withLockedNodes() {
    // Create segments with different states and replica sets
    createRepairSegments();

    // Lock node1 by creating a segment lock - this should filter out segment1 which has node1 as a
    // replica
    UUID dummySegmentId = UUID.randomUUID();
    memoryStorageFacade.lockRunningRepairsForNodes(
        repairRunId, dummySegmentId, Sets.newHashSet("node1"));

    List<RepairSegment> freeSegments = memoryRepairSegmentDao.getNextFreeSegments(repairRunId);

    // Should return only 1 segment (segment2) - segment1 is filtered out due to locked node1
    assertEquals(1, freeSegments.size());
    RepairSegment returnedSegment = freeSegments.get(0);
    assertEquals(RepairSegment.State.NOT_STARTED, returnedSegment.getState());
    // The returned segment should not have node1 in its replicas
    assertTrue(!returnedSegment.getReplicas().containsKey("node1"));
    assertTrue(returnedSegment.getReplicas().containsKey("node2"));
  }

  @Test
  public void testGetNextFreeSegments_allNodesLocked() {
    // Create segments with different states and replica sets
    createRepairSegments();

    // Lock all nodes by creating segment locks
    UUID dummySegmentId1 = UUID.randomUUID();

    // Verify that each locking operation succeeds
    boolean locked =
        memoryStorageFacade.lockRunningRepairsForNodes(
            repairRunId, dummySegmentId1, Sets.newHashSet("node1", "node2", "node3"));
    assertTrue("Failed to lock nodes", locked);

    // Verify that the locked nodes are returned correctly
    Set<String> lockedNodes = memoryStorageFacade.getLockedNodesForRun(repairRunId);
    assertEquals("Expected 3 locked nodes", 3, lockedNodes.size());
    assertTrue("node1 should be locked", lockedNodes.contains("node1" + repairRunId));
    assertTrue("node2 should be locked", lockedNodes.contains("node2" + repairRunId));
    assertTrue("node3 should be locked", lockedNodes.contains("node3" + repairRunId));

    List<RepairSegment> freeSegments = memoryRepairSegmentDao.getNextFreeSegments(repairRunId);

    // Should return no segments since all nodes are locked
    assertEquals(0, freeSegments.size());
  }

  @Test
  public void testGetNextFreeSegments_multipleLockedNodes() {
    // Create segments with different states and replica sets
    createRepairSegments();

    // Lock node1 and node2 by creating segment locks
    UUID dummySegmentId1 = UUID.randomUUID();
    UUID dummySegmentId2 = UUID.randomUUID();
    memoryStorageFacade.lockRunningRepairsForNodes(
        repairRunId, dummySegmentId1, Sets.newHashSet("node1"));
    memoryStorageFacade.lockRunningRepairsForNodes(
        repairRunId, dummySegmentId2, Sets.newHashSet("node2"));

    List<RepairSegment> freeSegments = memoryRepairSegmentDao.getNextFreeSegments(repairRunId);

    // Should return no segments since both NOT_STARTED segments have either node1 or node2
    assertEquals(0, freeSegments.size());
  }

  @Test
  public void testGetNextFreeSegments_onlyFiltersByState() {
    // Create segments with different states but no locked nodes
    createRepairSegments();

    List<RepairSegment> freeSegments = memoryRepairSegmentDao.getNextFreeSegments(repairRunId);

    // Verify that only NOT_STARTED segments are returned
    assertEquals(2, freeSegments.size());
    for (RepairSegment segment : freeSegments) {
      assertEquals(RepairSegment.State.NOT_STARTED, segment.getState());
    }

    // Verify that RUNNING and DONE segments are not returned
    Collection<RepairSegment> allSegments =
        memoryStorageFacade.getRepairSegmentDao().getRepairSegmentsForRun(repairRunId);
    assertEquals(4, allSegments.size()); // Total segments created
  }

  private void createRepairSegments() {
    // Segment 1: NOT_STARTED with node1 and node3 as replicas
    Map<String, String> replicas1 = new HashMap<>();
    replicas1.put("node1", "dc1");
    replicas1.put("node3", "dc1");
    RepairSegment segment1 =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(0), BigInteger.valueOf(100)))
                    .withReplicas(replicas1)
                    .build(),
                repairUnitId)
            .withRunId(repairRunId)
            .withId(Uuids.timeBased())
            .withState(RepairSegment.State.NOT_STARTED)
            .build();
    ((MemoryRepairSegmentDao) memoryStorageFacade.getRepairSegmentDao())
        .addRepairSegmentWithId(segment1);

    // Segment 2: NOT_STARTED with node2 and node3 as replicas
    Map<String, String> replicas2 = new HashMap<>();
    replicas2.put("node2", "dc1");
    replicas2.put("node3", "dc1");
    RepairSegment segment2 =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(100), BigInteger.valueOf(200)))
                    .withReplicas(replicas2)
                    .build(),
                repairUnitId)
            .withRunId(repairRunId)
            .withId(Uuids.timeBased())
            .withState(RepairSegment.State.NOT_STARTED)
            .build();
    ((MemoryRepairSegmentDao) memoryStorageFacade.getRepairSegmentDao())
        .addRepairSegmentWithId(segment2);

    // Segment 3: RUNNING (should be filtered out by state) - needs startTime
    Map<String, String> replicas3 = new HashMap<>();
    replicas3.put("node1", "dc1");
    replicas3.put("node2", "dc1");
    RepairSegment segment3 =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(200), BigInteger.valueOf(300)))
                    .withReplicas(replicas3)
                    .build(),
                repairUnitId)
            .withRunId(repairRunId)
            .withId(Uuids.timeBased())
            .withState(RepairSegment.State.RUNNING)
            .withStartTime(org.joda.time.DateTime.now())
            .build();
    ((MemoryRepairSegmentDao) memoryStorageFacade.getRepairSegmentDao())
        .addRepairSegmentWithId(segment3);

    // Segment 4: DONE (should be filtered out by state) - needs startTime and endTime
    Map<String, String> replicas4 = new HashMap<>();
    replicas4.put("node2", "dc1");
    replicas4.put("node3", "dc1");
    RepairSegment segment4 =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(300), BigInteger.valueOf(400)))
                    .withReplicas(replicas4)
                    .build(),
                repairUnitId)
            .withRunId(repairRunId)
            .withId(Uuids.timeBased())
            .withState(RepairSegment.State.DONE)
            .withStartTime(org.joda.time.DateTime.now().minusMinutes(10))
            .withEndTime(org.joda.time.DateTime.now())
            .build();
    ((MemoryRepairSegmentDao) memoryStorageFacade.getRepairSegmentDao())
        .addRepairSegmentWithId(segment4);
  }

  @Test
  public void testGetRepairSegmentByTokenRange_found() {
    // Create a segment with a specific token range
    Map<String, String> replicas = new HashMap<>();
    replicas.put("node1", "dc1");
    replicas.put("node2", "dc1");

    BigInteger startToken = BigInteger.valueOf(1000);
    BigInteger endToken = BigInteger.valueOf(2000);

    RepairSegment segment =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(startToken, endToken))
                    .withReplicas(replicas)
                    .build(),
                repairUnitId)
            .withRunId(repairRunId)
            .withId(Uuids.timeBased())
            .withState(RepairSegment.State.NOT_STARTED)
            .build();

    memoryRepairSegmentDao.addRepairSegmentWithId(segment);

    // Find the segment by token range
    java.util.Optional<RepairSegment> found =
        memoryRepairSegmentDao.getRepairSegmentByTokenRange(
            repairRunId, repairUnitId, startToken, endToken);

    assertTrue("Segment should be found", found.isPresent());
    assertEquals("Segment ID should match", segment.getId(), found.get().getId());
    assertEquals("Start token should match", startToken, found.get().getStartToken());
    assertEquals("End token should match", endToken, found.get().getEndToken());
  }

  @Test
  public void testGetRepairSegmentByTokenRange_notFound() {
    // Create a segment with a specific token range
    Map<String, String> replicas = new HashMap<>();
    replicas.put("node1", "dc1");

    RepairSegment segment =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(
                        new RingRange(BigInteger.valueOf(1000), BigInteger.valueOf(2000)))
                    .withReplicas(replicas)
                    .build(),
                repairUnitId)
            .withRunId(repairRunId)
            .withId(Uuids.timeBased())
            .withState(RepairSegment.State.NOT_STARTED)
            .build();

    memoryRepairSegmentDao.addRepairSegmentWithId(segment);

    // Try to find a segment with a different token range
    java.util.Optional<RepairSegment> found =
        memoryRepairSegmentDao.getRepairSegmentByTokenRange(
            repairRunId, repairUnitId, BigInteger.valueOf(3000), BigInteger.valueOf(4000));

    assertTrue("Segment should not be found", !found.isPresent());
  }

  @Test
  public void testUpdateRepairSegmentStateConditional_success() {
    // Create a segment in NOT_STARTED state
    Map<String, String> replicas = new HashMap<>();
    replicas.put("node1", "dc1");

    RepairSegment segment =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(
                        new RingRange(BigInteger.valueOf(1000), BigInteger.valueOf(2000)))
                    .withReplicas(replicas)
                    .build(),
                repairUnitId)
            .withRunId(repairRunId)
            .withId(Uuids.timeBased())
            .withState(RepairSegment.State.NOT_STARTED)
            .build();

    memoryRepairSegmentDao.addRepairSegmentWithId(segment);

    // Conditionally update from NOT_STARTED to DONE
    boolean updated =
        memoryRepairSegmentDao.updateRepairSegmentStateConditional(
            segment.getId(), RepairSegment.State.DONE, RepairSegment.State.NOT_STARTED);

    assertTrue("Update should succeed", updated);

    // Verify the state was updated
    java.util.Optional<RepairSegment> updatedSegment =
        memoryRepairSegmentDao.getRepairSegment(repairRunId, segment.getId());

    assertTrue("Segment should exist", updatedSegment.isPresent());
    assertEquals("State should be DONE", RepairSegment.State.DONE, updatedSegment.get().getState());
  }

  @Test
  public void testUpdateRepairSegmentStateConditional_failsWhenStateDoesNotMatch() {
    // Create a segment in RUNNING state
    Map<String, String> replicas = new HashMap<>();
    replicas.put("node1", "dc1");

    RepairSegment segment =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(
                        new RingRange(BigInteger.valueOf(1000), BigInteger.valueOf(2000)))
                    .withReplicas(replicas)
                    .build(),
                repairUnitId)
            .withRunId(repairRunId)
            .withId(Uuids.timeBased())
            .withState(RepairSegment.State.RUNNING)
            .withStartTime(org.joda.time.DateTime.now())
            .build();

    memoryRepairSegmentDao.addRepairSegmentWithId(segment);

    // Try to conditionally update from NOT_STARTED to DONE (but segment is RUNNING)
    boolean updated =
        memoryRepairSegmentDao.updateRepairSegmentStateConditional(
            segment.getId(), RepairSegment.State.DONE, RepairSegment.State.NOT_STARTED);

    assertTrue("Update should fail", !updated);

    // Verify the state was NOT updated
    java.util.Optional<RepairSegment> unchangedSegment =
        memoryRepairSegmentDao.getRepairSegment(repairRunId, segment.getId());

    assertTrue("Segment should exist", unchangedSegment.isPresent());
    assertEquals(
        "State should still be RUNNING",
        RepairSegment.State.RUNNING,
        unchangedSegment.get().getState());
  }

  @Test
  public void testUpdateRepairSegmentStateConditional_idempotent() {
    // Create a segment in NOT_STARTED state
    Map<String, String> replicas = new HashMap<>();
    replicas.put("node1", "dc1");

    RepairSegment segment =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(
                        new RingRange(BigInteger.valueOf(1000), BigInteger.valueOf(2000)))
                    .withReplicas(replicas)
                    .build(),
                repairUnitId)
            .withRunId(repairRunId)
            .withId(Uuids.timeBased())
            .withState(RepairSegment.State.NOT_STARTED)
            .build();

    memoryRepairSegmentDao.addRepairSegmentWithId(segment);

    // First update: NOT_STARTED -> DONE
    boolean firstUpdate =
        memoryRepairSegmentDao.updateRepairSegmentStateConditional(
            segment.getId(), RepairSegment.State.DONE, RepairSegment.State.NOT_STARTED);

    assertTrue("First update should succeed", firstUpdate);

    // Second update: try NOT_STARTED -> DONE again (should fail because state is now DONE)
    boolean secondUpdate =
        memoryRepairSegmentDao.updateRepairSegmentStateConditional(
            segment.getId(), RepairSegment.State.DONE, RepairSegment.State.NOT_STARTED);

    assertTrue("Second update should fail", !secondUpdate);

    // Verify the state is still DONE
    java.util.Optional<RepairSegment> finalSegment =
        memoryRepairSegmentDao.getRepairSegment(repairRunId, segment.getId());

    assertTrue("Segment should exist", finalSegment.isPresent());
    assertEquals("State should be DONE", RepairSegment.State.DONE, finalSegment.get().getState());
  }

  @Test
  public void testUpdateRepairSegmentStateConditional_doneStateSetsBothTimestamps() {
    // Create a segment in NOT_STARTED state (no timestamps)
    Map<String, String> replicas = new HashMap<>();
    replicas.put("node1", "dc1");

    RepairSegment segment =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(
                        new RingRange(BigInteger.valueOf(1000), BigInteger.valueOf(2000)))
                    .withReplicas(replicas)
                    .build(),
                repairUnitId)
            .withRunId(repairRunId)
            .withId(Uuids.timeBased())
            .withState(RepairSegment.State.NOT_STARTED)
            .build();

    memoryRepairSegmentDao.addRepairSegmentWithId(segment);

    // Conditionally update from NOT_STARTED to DONE
    boolean updated =
        memoryRepairSegmentDao.updateRepairSegmentStateConditional(
            segment.getId(), RepairSegment.State.DONE, RepairSegment.State.NOT_STARTED);

    assertTrue("Update should succeed", updated);

    // Verify the state was updated AND both timestamps are set
    java.util.Optional<RepairSegment> updatedSegment =
        memoryRepairSegmentDao.getRepairSegment(repairRunId, segment.getId());

    assertTrue("Segment should exist", updatedSegment.isPresent());
    assertEquals("State should be DONE", RepairSegment.State.DONE, updatedSegment.get().getState());
    assertTrue("Start time should be set", updatedSegment.get().hasStartTime());
    assertTrue("End time should be set", updatedSegment.get().hasEndTime());
    assertNotNull("Start time should not be null", updatedSegment.get().getStartTime());
    assertNotNull("End time should not be null", updatedSegment.get().getEndTime());
  }

  @Test
  public void testUpdateRepairSegmentStateConditional_runningStateSetsStartTime() {
    // Create a segment in NOT_STARTED state (no timestamps)
    Map<String, String> replicas = new HashMap<>();
    replicas.put("node1", "dc1");

    RepairSegment segment =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(
                        new RingRange(BigInteger.valueOf(1000), BigInteger.valueOf(2000)))
                    .withReplicas(replicas)
                    .build(),
                repairUnitId)
            .withRunId(repairRunId)
            .withId(Uuids.timeBased())
            .withState(RepairSegment.State.NOT_STARTED)
            .build();

    memoryRepairSegmentDao.addRepairSegmentWithId(segment);

    // Conditionally update from NOT_STARTED to RUNNING
    boolean updated =
        memoryRepairSegmentDao.updateRepairSegmentStateConditional(
            segment.getId(), RepairSegment.State.RUNNING, RepairSegment.State.NOT_STARTED);

    assertTrue("Update should succeed", updated);

    // Verify the state was updated AND start timestamp is set
    java.util.Optional<RepairSegment> updatedSegment =
        memoryRepairSegmentDao.getRepairSegment(repairRunId, segment.getId());

    assertTrue("Segment should exist", updatedSegment.isPresent());
    assertEquals(
        "State should be RUNNING", RepairSegment.State.RUNNING, updatedSegment.get().getState());
    assertTrue("Start time should be set", updatedSegment.get().hasStartTime());
    assertNotNull("Start time should not be null", updatedSegment.get().getStartTime());
  }

  @Test
  public void testUpdateRepairSegmentStateConditional_withRunId_success() {
    // Create a segment in NOT_STARTED state
    Map<String, String> replicas = new HashMap<>();
    replicas.put("node1", "dc1");

    RepairSegment segment =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(
                        new RingRange(BigInteger.valueOf(1000), BigInteger.valueOf(2000)))
                    .withReplicas(replicas)
                    .build(),
                repairUnitId)
            .withRunId(repairRunId)
            .withId(Uuids.timeBased())
            .withState(RepairSegment.State.NOT_STARTED)
            .build();

    memoryRepairSegmentDao.addRepairSegmentWithId(segment);

    // Update using the 4-parameter overload (with runId)
    boolean updated =
        memoryRepairSegmentDao.updateRepairSegmentStateConditional(
            repairRunId,
            segment.getId(),
            RepairSegment.State.RUNNING,
            RepairSegment.State.NOT_STARTED);

    assertTrue("Conditional update with runId should succeed", updated);

    // Verify the segment was updated
    java.util.Optional<RepairSegment> updatedSegment =
        memoryRepairSegmentDao.getRepairSegment(repairRunId, segment.getId());

    assertTrue("Segment should exist", updatedSegment.isPresent());
    assertEquals(
        "State should be RUNNING", RepairSegment.State.RUNNING, updatedSegment.get().getState());
  }

  @Test
  public void testUpdateRepairSegmentStateConditional_withRunId_stateMismatch() {
    // Create a segment in RUNNING state
    Map<String, String> replicas = new HashMap<>();
    replicas.put("node1", "dc1");

    RepairSegment segment =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(
                        new RingRange(BigInteger.valueOf(1000), BigInteger.valueOf(2000)))
                    .withReplicas(replicas)
                    .build(),
                repairUnitId)
            .withRunId(repairRunId)
            .withId(Uuids.timeBased())
            .withState(RepairSegment.State.RUNNING)
            .withStartTime(org.joda.time.DateTime.now())
            .build();

    memoryRepairSegmentDao.addRepairSegmentWithId(segment);

    // Try to update using the 4-parameter overload, expecting NOT_STARTED (wrong)
    boolean updated =
        memoryRepairSegmentDao.updateRepairSegmentStateConditional(
            repairRunId,
            segment.getId(),
            RepairSegment.State.DONE,
            RepairSegment.State.NOT_STARTED);

    assertTrue("Conditional update with runId should fail on state mismatch", !updated);

    // Verify the segment was NOT updated
    java.util.Optional<RepairSegment> unchangedSegment =
        memoryRepairSegmentDao.getRepairSegment(repairRunId, segment.getId());

    assertTrue("Segment should exist", unchangedSegment.isPresent());
    assertEquals(
        "State should still be RUNNING",
        RepairSegment.State.RUNNING,
        unchangedSegment.get().getState());
  }

  @Test
  public void testUpdateRepairSegmentStateConditional_withRunId_segmentNotFound() {
    // Use a non-existent segment ID
    UUID nonExistentSegmentId = UUID.randomUUID();

    // Try to update using the 4-parameter overload
    boolean updated =
        memoryRepairSegmentDao.updateRepairSegmentStateConditional(
            repairRunId,
            nonExistentSegmentId,
            RepairSegment.State.DONE,
            RepairSegment.State.NOT_STARTED);

    assertTrue("Conditional update with runId should fail for non-existent segment", !updated);
  }

  @Test
  public void testGetRepairSegmentByTokenRange_exceptionHandling() {
    // This test covers the exception handling path in getRepairSegmentByTokenRange
    // Create a segment first
    Map<String, String> replicas = new HashMap<>();
    replicas.put("node1", "dc1");

    RepairSegment segment =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(
                        new RingRange(BigInteger.valueOf(1000), BigInteger.valueOf(2000)))
                    .withReplicas(replicas)
                    .build(),
                repairUnitId)
            .withRunId(repairRunId)
            .withId(Uuids.timeBased())
            .withState(RepairSegment.State.NOT_STARTED)
            .build();

    memoryRepairSegmentDao.addRepairSegmentWithId(segment);

    // Query with valid parameters should work
    java.util.Optional<RepairSegment> found =
        memoryRepairSegmentDao.getRepairSegmentByTokenRange(
            repairRunId, repairUnitId, BigInteger.valueOf(1000), BigInteger.valueOf(2000));

    assertTrue("Segment should be found", found.isPresent());
  }
}
