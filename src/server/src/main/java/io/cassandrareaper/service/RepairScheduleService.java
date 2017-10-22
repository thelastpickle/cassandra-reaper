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
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairUnit;

import java.util.Collection;
import java.util.UUID;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class RepairScheduleService {

  private static final Logger LOG = LoggerFactory.getLogger(RepairScheduleService.class);

  private final AppContext context;

  private RepairScheduleService(AppContext context) {
    this.context = context;
  }

  public static RepairScheduleService create(AppContext context) {
    return new RepairScheduleService(context);
  }

  /**
   * Instantiates a RepairSchedule and stores it in the storage backend.
   *
   * @return the new, just stored RepairSchedule instance
   * @throws ReaperException when fails to store the RepairSchedule.
   */
  public RepairSchedule storeNewRepairSchedule(
      Cluster cluster,
      RepairUnit repairUnit,
      int daysBetween,
      DateTime nextActivation,
      String owner,
      int segmentCountPerNode,
      RepairParallelism repairParallelism,
      Double intensity)
      throws ReaperException {

    RepairSchedule.Builder scheduleBuilder =
        new RepairSchedule.Builder(
            repairUnit.getId(),
            RepairSchedule.State.ACTIVE,
            daysBetween,
            nextActivation,
            ImmutableList.<UUID>of(),
            0,
            repairParallelism,
            intensity,
            DateTime.now(),
            segmentCountPerNode);

    scheduleBuilder.owner(owner);

    Collection<RepairSchedule> repairSchedules = context.storage
        .getRepairSchedulesForClusterAndKeyspace(repairUnit.getClusterName(), repairUnit.getKeyspaceName());

    for (RepairSchedule sched : repairSchedules) {
      Optional<RepairUnit> repairUnitForSched = context.storage.getRepairUnit(sched.getRepairUnitId());
      if (repairUnitForSched.isPresent()
          && repairUnitForSched.get().getClusterName().equals(repairUnit.getClusterName())
          && repairUnitForSched.get().getKeyspaceName().equals(repairUnit.getKeyspaceName())
          && repairUnitForSched.get().getIncrementalRepair().equals(repairUnit.getIncrementalRepair())) {

        if (isConflictingSchedules(repairUnitForSched.get(), repairUnit)) {
          String errMsg = String.format(
              "A repair schedule already exists for cluster \"%s\", " + "keyspace \"%s\", and column families: %s",
              cluster.getName(),
              repairUnit.getKeyspaceName(),
              Sets.intersection(repairUnit.getColumnFamilies(), repairUnitForSched.get().getColumnFamilies()));

          LOG.error(errMsg);
          throw new ReaperException(errMsg);
        }
      }
    }

    RepairSchedule newRepairSchedule = context.storage.addRepairSchedule(scheduleBuilder);
    if (newRepairSchedule == null) {

      String errMsg = String.format(
          "failed storing repair schedule for cluster \"%s\", " + "keyspace \"%s\", and column families: %s",
          cluster.getName(), repairUnit.getKeyspaceName(), repairUnit.getColumnFamilies());

      LOG.error(errMsg);
      throw new ReaperException(errMsg);
    }
    return newRepairSchedule;
  }

  private static boolean isConflictingSchedules(RepairUnit newRepairUnit, RepairUnit existingRepairUnit) {

    return (newRepairUnit.getColumnFamilies().isEmpty()
            && existingRepairUnit.getColumnFamilies().isEmpty())
        || (!Sets.intersection(
                existingRepairUnit.getColumnFamilies(), newRepairUnit.getColumnFamilies())
            .isEmpty())
        || (!existingRepairUnit.getBlacklistedTables().isEmpty()
            && !newRepairUnit.getBlacklistedTables().isEmpty());
  }

}
