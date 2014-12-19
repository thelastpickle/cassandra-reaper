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

import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.ColumnFamily;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.service.RingRange;

import java.util.Collection;

import javax.annotation.Nullable;

/**
 * API definition for cassandra-reaper.
 */
public interface IStorage {

  boolean isStorageConnected();

  Collection<Cluster> getClusters();

  Cluster addCluster(Cluster cluster);

  boolean updateCluster(Cluster newCluster);

  Cluster getCluster(String clusterName);

  RepairRun addRepairRun(RepairRun.Builder repairRun);

  boolean updateRepairRun(RepairRun repairRun);

  RepairRun getRepairRun(long id);

  Collection<RepairRun> getRepairRunsForCluster(String clusterName);

  Collection<RepairRun> getAllRunningRepairRuns();

  ColumnFamily addColumnFamily(ColumnFamily.Builder newTable);

  ColumnFamily getColumnFamily(long id);

  ColumnFamily getColumnFamily(String cluster, String keyspace, String table);

  void addRepairSegments(Collection<RepairSegment.Builder> newSegments);

  boolean updateRepairSegment(RepairSegment newRepairSegment);

  RepairSegment getRepairSegment(long id);

  RepairSegment getNextFreeSegment(long runId);

  RepairSegment getNextFreeSegmentInRange(long runId, RingRange range);

  /**
   * If RepairRun is running, there should always be only one running segment at a time.
   * TODO: what if we have parallel repair run on one ring?
   *
   * @param runId The RepairRun id that should be running to have one running segment.
   * @return The running segment, or null in case there is none.
   */
  @Nullable
  RepairSegment getTheRunningSegment(long runId);

  Collection<Long> getRepairRunIdsForCluster(String clusterName);

  int getSegmentAmountForRepairRun(long runId, RepairSegment.State state);
}
