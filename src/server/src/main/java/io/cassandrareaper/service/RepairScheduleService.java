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

package io.cassandrareaper.service;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairUnit;

import java.util.Collection;
import java.util.Optional;

import com.google.common.base.Preconditions;
import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;


public final class RepairScheduleService {

  private final AppContext context;
  private final RepairUnitService repairUnitService;

  private RepairScheduleService(AppContext context) {
    this.context = context;
    this.repairUnitService = RepairUnitService.create(context);
  }

  public static RepairScheduleService create(AppContext context) {
    return new RepairScheduleService(context);
  }

  public Optional<RepairSchedule> conflictingRepairSchedule(Cluster cluster, RepairUnit.Builder repairUnit) {

    Collection<RepairSchedule> repairSchedules = context.storage
        .getRepairSchedulesForClusterAndKeyspace(repairUnit.clusterName, repairUnit.keyspaceName);

    for (RepairSchedule sched : repairSchedules) {
      RepairUnit repairUnitForSched = context.storage.getRepairUnit(sched.getRepairUnitId());
      Preconditions.checkState(repairUnitForSched.getClusterName().equals(repairUnit.clusterName));
      Preconditions.checkState(repairUnitForSched.getKeyspaceName().equals(repairUnit.keyspaceName));

      if (repairUnitService.conflictingUnits(cluster, repairUnitForSched, repairUnit)) {
        return Optional.of(sched);
      }
    }
    return Optional.empty();
  }

  /**
   * Instantiates a RepairSchedule and stores it in the storage backend.
   *
   *<p>
   * Expected to have called first  conflictingRepairSchedule(Cluster, RepairUnit)
   *
   * @return the new, just stored RepairSchedule instance
   */
  public RepairSchedule storeNewRepairSchedule(
      Cluster cluster,
      RepairUnit repairUnit,
      int daysBetween,
      DateTime nextActivation,
      String owner,
      int segmentCountPerNode,
      RepairParallelism repairParallelism,
      Double intensity) {

    Preconditions.checkArgument(
        !conflictingRepairSchedule(cluster, repairUnit.with()).isPresent(),
        "A repair schedule already exists for cluster \"%s\", keyspace \"%s\", and column families: %s",
        cluster.getName(),
        repairUnit.getKeyspaceName(),
        repairUnit.getColumnFamilies());

    RepairSchedule.Builder scheduleBuilder = RepairSchedule.builder(repairUnit.getId())
        .daysBetween(daysBetween)
        .nextActivation(nextActivation)
        .repairParallelism(repairParallelism)
        .intensity(intensity)
        .segmentCountPerNode(segmentCountPerNode)
        .owner(owner);

    return context.storage.addRepairSchedule(scheduleBuilder);
  }
}
