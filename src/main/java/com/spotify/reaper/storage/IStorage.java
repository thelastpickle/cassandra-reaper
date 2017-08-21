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
package com.spotify.reaper.storage;

import com.google.common.base.Optional;

import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.NodeMetrics;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSchedule;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.core.RepairUnit;
import com.spotify.reaper.resources.view.RepairRunStatus;
import com.spotify.reaper.resources.view.RepairScheduleStatus;
import com.spotify.reaper.service.RepairParameters;
import com.spotify.reaper.service.RingRange;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * API definition for cassandra-reaper.
 */
public interface IStorage {
  
  boolean isStorageConnected();

  Collection<Cluster> getClusters();

  boolean addCluster(Cluster cluster);

  boolean updateCluster(Cluster newCluster);

  Optional<Cluster> getCluster(String clusterName);

  /**
   * Delete the Cluster instance identified by the given cluster name. Delete succeeds
   * only if there are no repair runs for the targeted cluster.
   *
   * @param clusterName The name of the Cluster instance to delete.
   * @return The deleted Cluster instance if delete succeeds, with state set to DELETED.
   */
  Optional<Cluster> deleteCluster(String clusterName);

  RepairRun addRepairRun(RepairRun.Builder repairRun, Collection<RepairSegment.Builder> newSegments);

  boolean updateRepairRun(RepairRun repairRun);

  Optional<RepairRun> getRepairRun(UUID id);

  Collection<RepairRun> getRepairRunsForCluster(String clusterName);

  Collection<RepairRun> getRepairRunsForUnit(UUID repairUnitId);

  Collection<RepairRun> getRepairRunsWithState(RepairRun.RunState runState);

  /**
   * Delete the RepairRun instance identified by the given id, and delete also
   * all the related repair segments.
   *
   * @param id The id of the RepairRun instance to delete, and all segments for it.
   * @return The deleted RepairRun instance, if delete succeeds, with state set to DELETED.
   */
  Optional<RepairRun> deleteRepairRun(UUID id);

  RepairUnit addRepairUnit(RepairUnit.Builder newRepairUnit);

  Optional<RepairUnit> getRepairUnit(UUID id);

  /**
   * Get a stored RepairUnit targeting the given tables in the given keyspace.
   *
   * @param cluster           Cluster name for the RepairUnit.
   * @param keyspace          Keyspace name for the RepairUnit.
   * @param columnFamilyNames Set of column families targeted by the RepairUnit.
   * @return Instance of a RepairUnit matching the parameters, or null if not found.
   */
  Optional<RepairUnit> getRepairUnit(String cluster, String keyspace,
      Set<String> columnFamilyNames);


  boolean updateRepairSegment(RepairSegment newRepairSegment);

  Optional<RepairSegment> getRepairSegment(UUID runId, UUID segmentId);

  Collection<RepairSegment> getRepairSegmentsForRun(UUID runId);

  /**
   * @param runId the run id that the segment belongs to.
   * @param range a ring range. The start of the range may be greater than or equal to the end.
   *              This case has to be handled. When start = end, consider that as a range
   *              that covers the whole ring.
   * @return a segment enclosed by the range with state NOT_STARTED, or nothing.
   */
  Optional<RepairSegment> getNextFreeSegmentInRange(UUID runId, Optional<RingRange> range);

  Collection<RepairSegment> getSegmentsWithState(UUID runId, RepairSegment.State segmentState);

  Collection<RepairParameters> getOngoingRepairsInCluster(String clusterName);

  Collection<UUID> getRepairRunIdsForCluster(String clusterName);

  int getSegmentAmountForRepairRun(UUID runId);

  int getSegmentAmountForRepairRunWithState(UUID runId, RepairSegment.State state);

  RepairSchedule addRepairSchedule(RepairSchedule.Builder repairSchedule);

  Optional<RepairSchedule> getRepairSchedule(UUID repairScheduleId);

  Collection<RepairSchedule> getRepairSchedulesForCluster(String clusterName);

  Collection<RepairSchedule> getRepairSchedulesForKeyspace(String keyspaceName);

  Collection<RepairSchedule> getRepairSchedulesForClusterAndKeyspace(String clusterName,
      String keyspaceName);

  Collection<RepairSchedule> getAllRepairSchedules();

  boolean updateRepairSchedule(RepairSchedule newRepairSchedule);

  /**
   * Delete the RepairSchedule instance identified by the given id. Related repair runs
   * or other resources tied to the schedule will not be deleted.
   *
   * @param id The id of the RepairSchedule instance to delete.
   * @return The deleted RepairSchedule instance, if delete succeeds, with state set to DELETED.
   */
  Optional<RepairSchedule> deleteRepairSchedule(UUID id);

  Collection<RepairRunStatus> getClusterRunStatuses(String clusterName, int limit);

  Collection<RepairScheduleStatus> getClusterScheduleStatuses(String clusterName);
  
}
