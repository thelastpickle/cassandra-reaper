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

package io.cassandrareaper.storage;

import io.cassandrareaper.core.GenericMetric;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.service.RingRange;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;


/**
 * Definition for a storage that can run in distributed (peer-to-peer) mode. For example Cassandra.
 */
public interface IDistributedStorage {

  boolean takeLead(UUID leaderId);

  boolean takeLead(UUID leaderId, int ttl);

  boolean renewLead(UUID leaderId);

  boolean renewLead(UUID leaderId, int ttl);

  List<UUID> getLeaders();

  void releaseLead(UUID leaderId);

  boolean lockRunningRepairsForNodes(
      UUID repairId,
      UUID segmentId,
      Set<String> replicas);

  boolean renewRunningRepairsForNodes(
      UUID repairId,
      UUID segmentId,
      Set<String> replicas);

  boolean releaseRunningRepairsForNodes(
      UUID repairId,
      UUID segmentId,
      Set<String> replicas);

  Set<UUID> getLockedSegmentsForRun(UUID runId);

  int countRunningReapers();

  List<UUID> getRunningReapers();

  void saveHeartbeat();

  /**
   * Gets the next free segment from the backend that is both within the parallel range and the local node ranges.
   *
   * @param runId  id of the repair run
   * @param ranges list of ranges we're looking a segment for
   * @return an optional repair segment to process
   */
  List<RepairSegment> getNextFreeSegmentsForRanges(
      UUID runId, List<RingRange> ranges);

  List<GenericMetric> getMetrics(
      String clusterName,
      Optional<String> host,
      String metricDomain,
      String metricType,
      long since);

  void storeMetrics(List<GenericMetric> metric);

  void storeOperations(String clusterName, OpType operationType, String host, String operationsJson);

  String listOperations(String clusterName, OpType operationType, String host);

  /**
   * Purges old metrics from the database (no-op for databases w/ TTL)
   */
  void purgeMetrics();

  /**
   * Purges old node operation info from the database (no-op for databases w/ TTL)
   */
  void purgeNodeOperations();

  /**
   * Update the repair segment without a lock as it couldn't be grabbed.
   *
   * @param newRepairSegment repair segment to update
   * @return true if the segment was updated, false otherwise
   */
  boolean updateRepairSegmentUnsafe(RepairSegment newRepairSegment);
}
