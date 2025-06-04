/*
 * Copyright 2024-2024 DataStax, Inc.
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

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicaLockManagerWithTtl {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicaLockManagerWithTtl.class);
  private final ConcurrentHashMap<String, LockInfo> replicaLocks = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<UUID, Set<UUID>> repairRunToSegmentLocks =
      new ConcurrentHashMap<>();
  private final Lock lock = new ReentrantLock();

  private final long ttlMilliSeconds;

  public ReplicaLockManagerWithTtl(long ttlMilliSeconds) {
    this.ttlMilliSeconds = ttlMilliSeconds;
    // Schedule cleanup of expired locks
    ScheduledExecutorService lockCleanupScheduler = Executors.newScheduledThreadPool(1);
    lockCleanupScheduler.scheduleAtFixedRate(this::cleanupExpiredLocks, 1, 1, TimeUnit.SECONDS);
  }

  private String getReplicaLockKey(String replica, UUID runId) {
    return replica + runId;
  }

  public boolean lockRunningRepairsForNodes(UUID runId, UUID segmentId, Set<String> replicas) {
    lock.lock();
    try {
      long currentTime = System.currentTimeMillis();
      // Check if any replica is already locked by another runId
      boolean anyReplicaLocked =
          replicas.stream()
              .map(replica -> replicaLocks.get(getReplicaLockKey(replica, runId)))
              .anyMatch(
                  lockInfo ->
                      lockInfo != null
                          && lockInfo.expirationTime > currentTime
                          && lockInfo.runId.equals(runId));

      if (anyReplicaLocked) {
        LOG.debug("One of the replicas is already locked by another segment for runId: {}", runId);
        return false; // Replica is locked by another runId and not expired
      }

      // Lock the replicas for the given runId and segmentId
      long expirationTime = currentTime + ttlMilliSeconds;
      replicas.forEach(
          replica ->
              replicaLocks.put(
                  getReplicaLockKey(replica, runId), new LockInfo(runId, expirationTime)));

      // Update runId to segmentId mapping
      repairRunToSegmentLocks
          .computeIfAbsent(runId, k -> ConcurrentHashMap.newKeySet())
          .add(segmentId);
      return true;
    } finally {
      lock.unlock();
    }
  }

  public boolean renewRunningRepairsForNodes(UUID runId, UUID segmentId, Set<String> replicas) {
    lock.lock();
    try {
      long currentTime = System.currentTimeMillis();

      // Check if all replicas are already locked by this runId
      boolean allReplicasLocked =
          replicas.stream()
              .map(replica -> replicaLocks.get(getReplicaLockKey(replica, runId)))
              .allMatch(
                  lockInfo ->
                      lockInfo != null
                          && lockInfo.runId.equals(runId)
                          && lockInfo.expirationTime > currentTime);

      if (!allReplicasLocked) {
        return false; // Some replica is not validly locked by this runId
      }

      // Renew the lock by extending the expiration time
      long newExpirationTime = currentTime + ttlMilliSeconds;
      replicas.forEach(
          replica ->
              replicaLocks.put(
                  getReplicaLockKey(replica, runId), new LockInfo(runId, newExpirationTime)));

      // Ensure the segmentId is linked to the runId
      repairRunToSegmentLocks
          .computeIfAbsent(runId, k -> ConcurrentHashMap.newKeySet())
          .add(segmentId);
      return true;
    } finally {
      lock.unlock();
    }
  }

  public boolean releaseRunningRepairsForNodes(UUID runId, UUID segmentId, Set<String> replicas) {
    lock.lock();
    try {
      // Remove the lock for replicas
      replicas.stream()
          .map(replica -> getReplicaLockKey(replica, runId))
          .forEach(replica -> LOG.debug("releasing lock for replica: {}", replica));

      replicas.stream()
          .map(replica -> getReplicaLockKey(replica, runId))
          .forEach(replicaLocks::remove);

      LOG.debug("Locked replicas after release: {}", replicaLocks.keySet());
      // Remove the segmentId from the runId mapping
      Set<UUID> segments = repairRunToSegmentLocks.get(runId);
      if (segments != null) {
        segments.remove(segmentId);
        if (segments.isEmpty()) {
          repairRunToSegmentLocks.remove(runId);
        } else {
          repairRunToSegmentLocks.put(runId, segments);
        }
      }
      return true;
    } finally {
      lock.unlock();
    }
  }

  public Set<UUID> getLockedSegmentsForRun(UUID runId) {
    return repairRunToSegmentLocks.getOrDefault(runId, Collections.emptySet());
  }

  @VisibleForTesting
  public void cleanupExpiredLocks() {
    lock.lock();
    try {
      long currentTime = System.currentTimeMillis();

      // Remove expired locks from replicaLocks
      replicaLocks.entrySet().removeIf(entry -> entry.getValue().expirationTime <= currentTime);

      // Clean up runToSegmentLocks by removing segments with no active replicas
      repairRunToSegmentLocks
          .entrySet()
          .removeIf(
              entry -> {
                UUID runId = entry.getKey();
                Set<UUID> segments = entry.getValue();

                // Retain only active segments
                segments.removeIf(
                    segmentId -> {
                      boolean active =
                          replicaLocks.values().stream().anyMatch(info -> info.runId.equals(runId));
                      return !active;
                    });
                return segments.isEmpty();
              });
    } finally {
      lock.unlock();
    }
  }

  // Class to store lock information
  private static class LockInfo {
    UUID runId;
    long expirationTime;

    LockInfo(UUID runId, long expirationTime) {
      this.runId = runId;
      this.expirationTime = expirationTime;
    }
  }
}
