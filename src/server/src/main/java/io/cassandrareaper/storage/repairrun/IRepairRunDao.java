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

package io.cassandrareaper.storage.repairrun;

import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.resources.view.RepairRunStatus;
import java.util.Collection;
import java.util.Optional;
import java.util.SortedSet;
import java.util.UUID;

public interface IRepairRunDao {
  RepairRun addRepairRun(
      RepairRun.Builder repairRun, Collection<RepairSegment.Builder> newSegments);

  boolean updateRepairRun(RepairRun repairRun, Optional<Boolean> updateRepairState);

  boolean updateRepairRun(RepairRun repairRun);

  Optional<RepairRun> getRepairRun(UUID id);

  /**
   * return all the repair runs in a cluster, in reverse chronological order, with default limit is
   * 1000
   */
  Collection<RepairRun> getRepairRunsForCluster(String clusterName, Optional<Integer> limit);

  Collection<RepairRun> getRepairRunsForClusterPrioritiseRunning(
      String clusterName, Optional<Integer> limit);

  Collection<RepairRun> getRepairRunsForUnit(UUID repairUnitId);

  Collection<RepairRun> getRepairRunsWithState(RepairRun.RunState runState);

  SortedSet<UUID> getRepairRunIdsForCluster(String clusterName, Optional<Integer> limit);

  /**
   * Delete the RepairRun instance identified by the given id, and delete also all the related
   * repair segments.
   *
   * @param id The id of the RepairRun instance to delete, and all segments for it.
   * @return The deleted RepairRun instance, if delete succeeds, with state set to DELETED.
   */
  Optional<RepairRun> deleteRepairRun(UUID id);

  Collection<RepairRunStatus> getClusterRunStatuses(String clusterName, int limit);
}
