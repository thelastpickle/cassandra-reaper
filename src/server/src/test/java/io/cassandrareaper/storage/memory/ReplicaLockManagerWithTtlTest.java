/*
 * Copyright 2014-2017 Spotify AB
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

package io.cassandrareaper.storage.memory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ReplicaLockManagerWithTtlTest {

  private ReplicaLockManagerWithTtl replicaLockManager;
  private UUID runId;
  private UUID segmentId;
  private Set<String> replicas;
  private Set<String> replicasOverlap;

  @BeforeEach
  public void setUp() {
    replicaLockManager = new ReplicaLockManagerWithTtl(1000);
    runId = UUID.randomUUID();
    segmentId = UUID.randomUUID();
    replicas = new HashSet<>(Arrays.asList("replica1", "replica2", "replica3"));
    replicasOverlap = new HashSet<>(Arrays.asList("replica4", "replica2", "replica5"));
  }

  @Test
  public void testLockRunningRepairsForNodes() {
    assertTrue(replicaLockManager.lockRunningRepairsForNodes(runId, segmentId, replicas));
  }

  @Test
  public void testLockRunningRepairsForNodesAlreadyLocked() {
    UUID anotherRunId = UUID.randomUUID();
    assertTrue(replicaLockManager.lockRunningRepairsForNodes(runId, segmentId, replicas));
    assertFalse(replicaLockManager.lockRunningRepairsForNodes(runId, segmentId, replicas));
  }

  @Test
  public void testRenewRunningRepairsForNodes() {
    assertTrue(replicaLockManager.lockRunningRepairsForNodes(runId, segmentId, replicas));
    assertTrue(replicaLockManager.renewRunningRepairsForNodes(runId, segmentId, replicas));
  }

  @Test
  public void testRenewRunningRepairsForNodesNotLocked() {
    assertFalse(replicaLockManager.renewRunningRepairsForNodes(runId, segmentId, replicas));
  }

  @Test
  public void testReleaseRunningRepairsForNodes() {
    assertTrue(replicaLockManager.lockRunningRepairsForNodes(runId, segmentId, replicas));
    // Same replicas can't be locked twice
    assertFalse(replicaLockManager.lockRunningRepairsForNodes(runId, UUID.randomUUID(), replicas));
    assertTrue(replicaLockManager.releaseRunningRepairsForNodes(runId, segmentId, replicas));
    // After unlocking, we can lock again
    assertTrue(replicaLockManager.lockRunningRepairsForNodes(runId, UUID.randomUUID(), replicas));
  }

  @Test
  public void testGetLockedSegmentsForRun() {
    assertTrue(replicaLockManager.lockRunningRepairsForNodes(runId, segmentId, replicas));
    Set<UUID> lockedSegments = replicaLockManager.getLockedSegmentsForRun(runId);
    assertTrue(lockedSegments.contains(segmentId));
  }

  @Test
  public void testCleanupExpiredLocks() throws InterruptedException {
    assertTrue(replicaLockManager.lockRunningRepairsForNodes(runId, segmentId, replicas));
    // We can lock the replica for a different run id
    assertTrue(
        replicaLockManager.lockRunningRepairsForNodes(
            UUID.randomUUID(), UUID.randomUUID(), replicas));
    // The following lock should fail because overlapping replicas are already locked
    assertFalse(
        replicaLockManager.lockRunningRepairsForNodes(runId, UUID.randomUUID(), replicasOverlap));
    Thread.sleep(1000); // Wait for TTL to expire
    replicaLockManager.cleanupExpiredLocks();
    // The following lock should succeed as the lock expired
    assertTrue(replicaLockManager.lockRunningRepairsForNodes(runId, segmentId, replicasOverlap));
  }
}
