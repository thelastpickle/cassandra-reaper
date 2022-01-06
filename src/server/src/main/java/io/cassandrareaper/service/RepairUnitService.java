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
import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.core.Table;
import io.cassandrareaper.jmx.ClusterFacade;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.datastax.driver.core.VersionNumber;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class RepairUnitService {

  private static final Logger LOG = LoggerFactory.getLogger(RepairUnitService.class);


  private static final Set<String> BLACKLISTED_STRATEGEIS
      = ImmutableSet.of("TimeWindowCompactionStrategy", "DateTieredCompactionStrategy");

  private final AppContext context;
  private final ClusterFacade clusterFacade;

  private RepairUnitService(AppContext context, Supplier<ClusterFacade> clusterFacadeSupplier) {
    this.context = context;
    this.clusterFacade = clusterFacadeSupplier.get();
  }

  @VisibleForTesting
  static RepairUnitService create(AppContext context, Supplier<ClusterFacade> supplier) throws ReaperException {
    return new RepairUnitService(context, supplier);
  }

  public static RepairUnitService create(AppContext context) {
    return new RepairUnitService(context, () -> ClusterFacade.create(context));
  }

  public Optional<RepairUnit> getOrCreateRepairUnit(Cluster cluster, RepairUnit.Builder params) {
    return getOrCreateRepairUnit(cluster, params, false);
  }

  public Optional<RepairUnit> getOrCreateRepairUnit(Cluster cluster, RepairUnit.Builder params, boolean force) {
    if (params.incrementalRepair) {
      try {
        String version = clusterFacade.getCassandraVersion(cluster);
        if (null != version && version.startsWith("2.0")) {
          throw new IllegalArgumentException("Incremental repair does not work with Cassandra versions before 2.1");
        }
      } catch (ReaperException e) {
        LOG.warn("unknown version to cluster {}, maybe enabling incremental on 2.0...", cluster.getName(), e);
      }
    }
    Optional<RepairUnit> repairUnit = context.storage.getRepairUnit(params);
    if (repairUnit.isPresent()) {
      return repairUnit;
    }

    try {
      return Optional.of(createRepairUnit(cluster, params, force));
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }


  }

  /**
   * Applies blacklist filter on tables for the given repair unit.
   *
   * @param proxy : a JMX proxy instance
   * @param unit : the repair unit for the current run
   * @return the list of tables to repair for the keyspace without the blacklisted ones
   */
  Set<String> getTablesToRepair(Cluster cluster, RepairUnit repairUnit) throws ReaperException {
    String keyspace = repairUnit.getKeyspaceName();
    Set<String> result;

    if (repairUnit.getColumnFamilies().isEmpty()) {
      Set<Table> tables = clusterFacade.getTablesForKeyspace(cluster, keyspace);
      Set<String> twcsBlacklisted = findBlacklistedCompactionStrategyTables(cluster, tables);

      result = tables.stream()
          .map(Table::getName)
          .filter(tableName -> !repairUnit.getBlacklistedTables().contains(tableName))
          .filter(tableName -> !twcsBlacklisted.contains(tableName))
          .collect(Collectors.toSet());
    } else {
      // if tables have been specified then don't apply the twcsBlacklisting
      result = repairUnit.getColumnFamilies().stream()
            .filter(tableName -> !repairUnit.getBlacklistedTables().contains(tableName))
            .collect(Collectors.toSet());
    }

    Preconditions.checkState(
        repairUnit.getBlacklistedTables().isEmpty() || !result.isEmpty(),
        "Invalid blacklist definition. It filtered out all tables in the keyspace.");

    return result;
  }

  public Set<String> findBlacklistedCompactionStrategyTables(Cluster clstr, Set<Table> tables) throws ReaperException {
    if (context.config.getBlacklistTwcsTables()
        && versionCompare(clusterFacade.getCassandraVersion(clstr), "2.1") >= 0) {

      return tables
          .stream()
          .filter(RepairUnitService::isBlackListedCompactionStrategy)
          .map(Table::getName)
          .collect(Collectors.toSet());
    }
    return Collections.emptySet();
  }

  private static Integer versionCompare(String str1, String str2) {
    VersionNumber version1 = VersionNumber.parse(str1);
    VersionNumber version2 = VersionNumber.parse(str2);
    return version1.compareTo(version2);
  }

  private static boolean isBlackListedCompactionStrategy(Table table) {
    return BLACKLISTED_STRATEGEIS.stream()
        .anyMatch(s -> table.getCompactionStrategy().toLowerCase().contains(s.toLowerCase()));
  }

  private RepairUnit createRepairUnit(Cluster cluster, RepairUnit.Builder builder, boolean force) {
    Preconditions.checkArgument(
        force || !unitConflicts(cluster, builder),
        "unit conflicts with existing in " + builder.clusterName + ":" + builder.keyspaceName);

    return context.storage.addRepairUnit(builder);
  }

  @VisibleForTesting
  boolean unitConflicts(Cluster cluster, RepairUnit.Builder builder) {

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

  boolean identicalUnits(Cluster cluster, RepairUnit unit, RepairUnit.Builder builder) {
    // if the Builders are equal, everything is the same
    if (unit.with().equals(builder)) {
      return true;
    }

    // if incremental repair is not the same, the units are not identical
    if (unit.getIncrementalRepair() != builder.incrementalRepair.booleanValue()) {
      // incremental reapir is not the same
      return false;
    }

    // check the set of tables to be repaired
    Preconditions.checkState(unit.getKeyspaceName().equals(builder.keyspaceName));

    Set<String> tables = unit.getColumnFamilies().isEmpty() || builder.columnFamilies.isEmpty()
        ? getTableNamesForKeyspace(cluster, unit.getKeyspaceName())
        : Collections.emptySet();

    // if the set of tables to repair is not the same, the units are not identical
    if (!Objects.equals(listRepairTables(unit.with(), tables), listRepairTables(builder, tables))) {
      // repair tables not the same
      return false;
    }

    // if the set of nodes isn't the same, the units are not identical
    Set<String> unitNodes = getRepairUnitNodes(cluster, unit.with());
    Set<String> builderNodes = getRepairUnitNodes(cluster, builder);
    if (!Objects.equals(unitNodes, builderNodes)) {
      // repair unit nodes not the same
      return false;
    }

    // if the set of datacenetrrs isn't the same, the units are not identical
    Set<String> unitDatacenters = getRepairUnitDatacenters(cluster, unit.with(), unitNodes);
    Set<String> builderDatacenters = getRepairUnitDatacenters(cluster, builder, builderNodes);
    if (!Objects.equals(unitDatacenters, builderDatacenters)) {
      // repair datacenters not the same
      return false;
    }

    // units are effectively identical
    return true;
  }

  public Set<String> getTableNamesForKeyspace(Cluster cluster, String keyspace) {
    try {
      return clusterFacade
          .getTablesForKeyspace(cluster, keyspace)
          .stream()
          .map(Table::getName)
          .collect(Collectors.toSet());
    } catch (ReaperException e) {
      LOG.warn("unknown table list to cluster {} keyspace", cluster.getName(), keyspace, e);
      return Collections.emptySet();
    }
  }

  private Set<String> getRepairUnitNodes(Cluster cluster, RepairUnit.Builder builder) {
    if (!builder.nodes.isEmpty()) {
      return builder.nodes;
    }
    try {
      return clusterFacade
          .getLiveNodes(cluster)
          .stream()
          .collect(Collectors.toSet());
    } catch (ReaperException e) {
      LOG.warn("Unable to get list of live nodes for cluster {}", cluster.getName());
      return Collections.emptySet();
    }
  }

  private Set<String> getRepairUnitDatacenters(Cluster cluster, RepairUnit.Builder builder, Set<String> nodes) {
    if (!builder.datacenters.isEmpty()) {
      return builder.datacenters;
    }
    Set<String> datacenters = Sets.newHashSet();
    try {
      for (String node : nodes) {
        datacenters.add(clusterFacade.getDatacenter(Node.builder().withHostname(node).build()));
      }
    } catch (ReaperException | InterruptedException e) {
      LOG.warn("Unable to get the list of datacenters for cluster {}", cluster.getName(), e);
    }
    return datacenters;
  }

  private static Set<String> listRepairTables(RepairUnit.Builder builder, Set<String> allTables) {
    // subtract blacklisted tables from all tables (or those explicitly listed)
    Set<String> tables = Sets.newHashSet(builder.columnFamilies.isEmpty() ? allTables : builder.columnFamilies);
    tables.removeAll(builder.blacklistedTables);
    return tables;
  }
}
