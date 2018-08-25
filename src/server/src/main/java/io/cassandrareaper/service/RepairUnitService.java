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

package io.cassandrareaper.service;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.core.Table;
import io.cassandrareaper.jmx.ClusterFacade;
import io.cassandrareaper.jmx.JmxProxy;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class RepairUnitService {

  private static final Logger LOG = LoggerFactory.getLogger(RepairUnitService.class);


  private static final Set<String> BLACKLISTED_STRATEGEIS
      = ImmutableSet.of("TimeWindowCompactionStrategy", "DateTieredCompactionStrategy");

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
        String version = ClusterFacade.create(context).getCassandraVersion(cluster);
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

  /**
   * Applies blacklist filter on tables for the given repair unit.
   *
   * @param proxy : a JMX proxy instance
   * @param unit : the repair unit for the current run
   * @return the list of tables to repair for the keyspace without the blacklisted ones
   * @throws ReaperException, IllegalStateException
   */
  Set<String> getTablesToRepair(JmxProxy proxy, Cluster cluster, RepairUnit repairUnit)
      throws ReaperException, IllegalStateException {

    String keyspace = repairUnit.getKeyspaceName();
    Set<String> tables;

    if (repairUnit.getColumnFamilies().isEmpty()) {
      Set<String> twcsBlacklisted = findBlacklistedCompactionStrategyTables(cluster, keyspace);

      tables = proxy.getTablesForKeyspace(keyspace).stream()
          .map(Table::getName)
          .filter(tableName -> !repairUnit.getBlacklistedTables().contains(tableName))
          .filter(tableName -> !twcsBlacklisted.contains(tableName))
          .collect(Collectors.toSet());
    } else {
      // if tables have been specified then don't apply the twcsBlacklisting
      tables = repairUnit.getColumnFamilies().stream()
            .filter(tableName -> !repairUnit.getBlacklistedTables().contains(tableName))
            .collect(Collectors.toSet());
    }

    Preconditions.checkState(
        repairUnit.getBlacklistedTables().isEmpty() || !tables.isEmpty(),
        "Invalid blacklist definition. It filtered out all tables in the keyspace.");

    return tables;
  }

  public Set<String> findBlacklistedCompactionStrategyTables(Cluster cluster, String keyspace) {
    if (context.config.getBlacklistTwcsTables()) {
      try {
        return ClusterFacade.create(context)
            .getTablesForKeyspace(cluster, keyspace)
            .stream()
            .filter(RepairUnitService::isBlackListedCompactionStrategy)
            .map(Table::getName)
            .collect(Collectors.toSet());

      } catch (ReaperException e) {
        LOG.error("unknown table list to cluster {} keyspace", cluster.getName(), keyspace, e);
      }
    }
    return Collections.emptySet();
  }

  private static boolean isBlackListedCompactionStrategy(Table table) {
    return BLACKLISTED_STRATEGEIS.stream()
        .anyMatch(s -> table.getCompactionStrategy().toLowerCase().contains(s.toLowerCase()));
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

  public Set<String> getTableNamesForKeyspace(Cluster cluster, String keyspace) {
    try {
      return ClusterFacade
          .create(context)
          .getTablesForKeyspace(cluster, keyspace)
          .stream()
          .map(Table::getName)
          .collect(Collectors.toSet());
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
