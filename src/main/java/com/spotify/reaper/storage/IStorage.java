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
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSchedule;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.core.RepairUnit;
import com.spotify.reaper.service.RingRange;
import com.spotify.reaper.service.SchedulingManager;

import java.util.Collection;
import java.util.Set;

/**
 * API definition for cassandra-reaper.
 */
public interface IStorage {

  boolean isStorageConnected();

  Collection<Cluster> getClusters();

  boolean addCluster(Cluster cluster);

  boolean updateCluster(Cluster newCluster);

  Optional<Cluster> getCluster(String clusterName);

  RepairRun addRepairRun(RepairRun.Builder repairRun);

  boolean updateRepairRun(RepairRun repairRun);

  Optional<RepairRun> getRepairRun(long id);

  Collection<RepairRun> getRepairRunsForCluster(String clusterName);

  Collection<RepairRun> getRepairRunsForUnit(RepairUnit repairUnit);

  Collection<RepairRun> getRepairRunsWithState(RepairRun.RunState runState);

  RepairUnit addRepairUnit(RepairUnit.Builder newRepairUnit);

  Optional<RepairUnit> getRepairUnit(long id);

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

  void addRepairSegments(Collection<RepairSegment.Builder> newSegments, long runId);

  boolean updateRepairSegment(RepairSegment newRepairSegment);

  Optional<RepairSegment> getRepairSegment(long id);

  Optional<RepairSegment> getNextFreeSegment(long runId);

  Optional<RepairSegment> getNextFreeSegmentInRange(long runId, RingRange range);

  Collection<RepairSegment> getSegmentsWithState(long runId, RepairSegment.State segmentState);

  Collection<Long> getRepairRunIdsForCluster(String clusterName);

  int getSegmentAmountForRepairRun(long runId, RepairSegment.State state);

  RepairSchedule addRepairSchedule(RepairSchedule.Builder repairSchedule);

  Optional<RepairSchedule> getRepairSchedule(long repairScheduleId);

  Collection<RepairSchedule> getAllRepairSchedules();

  boolean updateRepairSchedule(RepairSchedule newRepairSchedule);

}
