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
import io.cassandrareaper.core.NodeMetrics;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.service.RingRange;

import java.util.Collection;
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
      Set<String> replicas);

  boolean renewRunningRepairsForNodes(
      UUID repairId,
      Set<String> replicas);

  boolean releaseRunningRepairsForNodes(
      UUID repairId,
      Set<String> replicas);

  Set<String> getLockedNodesForRun(UUID runId);

  int countRunningReapers();

  void saveHeartbeat();

  Collection<NodeMetrics> getNodeMetrics(UUID runId);

  Optional<NodeMetrics> getNodeMetrics(UUID runId, String node);

  void deleteNodeMetrics(UUID runId, String node);

  void storeNodeMetrics(UUID runId, NodeMetrics nodeMetrics);

  /**
   * Gets the next free segment from the backend that is both within the parallel range and the local node ranges.
   *
   * @param runId id of the repair run
   * @param parallelRange list of ranges that can run in parallel
   * @param ranges list of ranges we're looking a segment for
   * @return an optional repair segment to process
   */
  Optional<RepairSegment> getNextFreeSegmentForRanges(
      UUID runId, Optional<RingRange> parallelRange, List<RingRange> ranges);

  List<GenericMetric> getMetrics(
      String clusterName,
      Optional<String> host,
      String metricDomain,
      String metricType,
      long since);

  void storeMetric(GenericMetric metric);

  void storeOperations(String clusterName, OpType operationType, String host, String operationsJson);

  String listOperations(String clusterName, OpType operationType, String host);

  /**
   * Purges old node metrics from the database (no-op for databases with TTL)
   */
  void purgeNodeMetrics();

  /**
   * Purges old metrics from the database (no-op for databases w/ TTL)
   */
  void purgeMetrics();

  /**
   * Purges old node operation info from the database (no-op for databases w/ TTL)
   */
  void purgeNodeOperations();
}
