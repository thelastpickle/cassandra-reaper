/*
 * Copyright 2017-2017 Spotify AB
 * Copyright 2017-2018 The Last Pickle Ltd
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

package io.cassandrareaper.service;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.jmx.JmxProxy;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class RepairUnitService {

  private static final Logger LOG = LoggerFactory.getLogger(RepairUnitService.class);

  private final AppContext context;

  private RepairUnitService(AppContext context) {
    this.context = context;
  }

  public static RepairUnitService create(AppContext context) {
    return new RepairUnitService(context);
  }

  public RepairUnit getOrCreateRepairUnit(Cluster cluster, RepairUnit.Builder params) {
    if (params.incrementalRepair) {
      try {
        JmxProxy jmxProxy
            = context.jmxConnectionFactory.connectAny(cluster, context.config.getJmxConnectionTimeoutInSeconds());

        String version = jmxProxy.getCassandraVersion();
        if (null != version && version.startsWith("2.0")) {
          throw new IllegalArgumentException("Incremental repair does not work with Cassandra versions before 2.1");
        }
      } catch (ReaperException e) {
        LOG.warn("unknown version to cluster {}, maybe enabling incremental on 2.0...", cluster.getName(), e);
      }
    }
    Optional<RepairUnit> repairUnit = context.storage.getRepairUnit(params);
    return repairUnit.isPresent() ? repairUnit.get() : createRepairUnit(cluster, params);
  }

  private RepairUnit createRepairUnit(Cluster cluster, RepairUnit.Builder builder) {
    Preconditions.checkArgument(
        !unitConflicts(cluster, builder),
        "unit conflicts with existing in " + builder.clusterName + ":" + builder.keyspaceName);

    return context.storage.addRepairUnit(builder);
  }

  private boolean unitConflicts(Cluster cluster, RepairUnit.Builder builder) {

    Collection<RepairSchedule> repairSchedules = context.storage
        .getRepairSchedulesForClusterAndKeyspace(builder.clusterName, builder.keyspaceName);

    for (RepairSchedule sched : repairSchedules) {
      RepairUnit repairUnitForSched = context.storage.getRepairUnit(sched.getRepairUnitId());
      Preconditions.checkState(repairUnitForSched.getClusterName().equals(builder.clusterName));
      Preconditions.checkState(repairUnitForSched.getKeyspaceName().equals(builder.keyspaceName));

      if (conflictingUnits(cluster, repairUnitForSched, builder)) {
        return true;
      }
    }
    return false;
  }

  boolean conflictingUnits(Cluster cluster, RepairUnit unit, RepairUnit.Builder builder) {
    if (unit.with().equals(builder)) {
      return true;
    }

    Preconditions.checkState(unit.getKeyspaceName().equals(builder.keyspaceName));

    Set<String> tables = unit.getColumnFamilies().isEmpty() || builder.columnFamilies.isEmpty()
        ? getTableNamesForKeyspace(cluster, unit.getKeyspaceName())
        : Collections.emptySet();

    // a conflict exists if any table is listed to be repaired by both repair units
    return !Sets.intersection(listRepairTables(unit.with(), tables), listRepairTables(builder, tables)).isEmpty();
  }

  private Set<String> getTableNamesForKeyspace(Cluster cluster, String keyspace) {
    try {
      return context
          .jmxConnectionFactory.connectAny(cluster, context.config.getJmxConnectionTimeoutInSeconds())
          .getTableNamesForKeyspace(keyspace);

    } catch (ReaperException e) {
      LOG.warn("unknown table list to cluster {} keyspace", cluster.getName(), keyspace, e);
      return Collections.emptySet();
    }
  }

  private static Set<String> listRepairTables(RepairUnit.Builder builder, Set<String> allTables) {
    // subtract blacklisted tables from all tables (or those explicitly listed)
    Set<String> tables = Sets.newHashSet(builder.columnFamilies.isEmpty() ? allTables : builder.columnFamilies);
    tables.removeAll(builder.blacklistedTables);
    return tables;
  }
}
