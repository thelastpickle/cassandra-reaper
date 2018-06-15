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
import io.cassandrareaper.jmx.JmxProxy;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
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

  public Optional<RepairSchedule> conflictingRepairSchedule(Cluster cluster, RepairUnit repairUnit) {

    Collection<RepairSchedule> repairSchedules = context.storage
        .getRepairSchedulesForClusterAndKeyspace(repairUnit.getClusterName(), repairUnit.getKeyspaceName());

    for (RepairSchedule sched : repairSchedules) {
      RepairUnit repairUnitForSched = context.storage.getRepairUnit(sched.getRepairUnitId());
      Preconditions.checkState(repairUnitForSched.getClusterName().equals(repairUnit.getClusterName()));
      Preconditions.checkState(repairUnitForSched.getKeyspaceName().equals(repairUnit.getKeyspaceName()));

      if (isConflictingSchedules(cluster, repairUnitForSched, repairUnit)) {
        return Optional.of(sched);
      }
    }
    return Optional.absent();
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
        !conflictingRepairSchedule(cluster, repairUnit).isPresent(),
        "A repair schedule already exists for cluster \"%s\", keyspace \"%s\", and column families: %s",
        cluster.getName(),
        repairUnit.getKeyspaceName(),
        repairUnit.getColumnFamilies());

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
    return context.storage.addRepairSchedule(scheduleBuilder);
  }

  private boolean isConflictingSchedules(Cluster cluster, RepairUnit unit0, RepairUnit unit1) {
    Preconditions.checkState(unit0.getKeyspaceName().equals(unit1.getKeyspaceName()));

    Set<String> tables = unit0.getColumnFamilies().isEmpty() || unit1.getColumnFamilies().isEmpty()
        ? getTableNamesForKeyspace(cluster, unit0.getKeyspaceName())
        : Collections.emptySet();

    // a conflict exists if any table is listed to be repaired by both repair units
    return !Sets.intersection(listRepairTables(unit0, tables), listRepairTables(unit1, tables)).isEmpty();
  }

  private Set<String> getTableNamesForKeyspace(Cluster cluster, String keyspace) {
    try {
      JmxProxy jmxProxy
          = context.jmxConnectionFactory.connectAny(cluster, context.config.getJmxConnectionTimeoutInSeconds());

      return jmxProxy.getTableNamesForKeyspace(keyspace);
    } catch (ReaperException e) {
      LOG.warn("unknown table list to cluster {} keyspace", cluster.getName(), keyspace, e);
      return Collections.emptySet();
    }
  }

  private static Set<String> listRepairTables(RepairUnit unit, Set<String> allTables) {
    // subtract blacklisted tables from all tables (or those explicitly listed)
    Set<String> tables = Sets.newHashSet(unit.getColumnFamilies().isEmpty() ? allTables : unit.getColumnFamilies());
    tables.removeAll(unit.getBlacklistedTables());
    return tables;
  }
}
