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

package io.cassandrareaper.storage.cassandra;

import io.cassandrareaper.core.RepairUnit;

import java.util.Optional;
import java.util.UUID;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepairUnitDao {
  private static final Logger LOG = LoggerFactory.getLogger(RepairUnitDao.class);
  PreparedStatement insertRepairUnitPrepStmt;
  PreparedStatement getRepairUnitPrepStmt;
  PreparedStatement deleteRepairUnitPrepStmt;
  final LoadingCache<UUID, RepairUnit> repairUnits = CacheBuilder
      .newBuilder()
      .build(new CacheLoader<UUID, RepairUnit>() {
        public RepairUnit load(UUID repairUnitId) throws Exception {
          return getRepairUnitImpl(repairUnitId);
        }
      });

  private final CassandraStorage cassandraStorage;
  private final Session session;

  public RepairUnitDao(CassandraStorage cassandraStorage, Session session) {
    this.cassandraStorage = cassandraStorage;
    this.session = session;
    prepareStatements();
  }

  private void prepareStatements() {
    insertRepairUnitPrepStmt = session
        .prepare(
            "INSERT INTO repair_unit_v1(id, cluster_name, keyspace_name, column_families, "
                + "incremental_repair, nodes, \"datacenters\", blacklisted_tables, repair_thread_count, timeout) "
                + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .setConsistencyLevel(ConsistencyLevel.QUORUM);
    getRepairUnitPrepStmt = session
        .prepare("SELECT * FROM repair_unit_v1 WHERE id = ?")
        .setConsistencyLevel(ConsistencyLevel.QUORUM);
    deleteRepairUnitPrepStmt = session.prepare("DELETE FROM repair_unit_v1 WHERE id = ?");
  }

  public RepairUnit addRepairUnit(RepairUnit.Builder newRepairUnit) {
    RepairUnit repairUnit = newRepairUnit.build(UUIDs.timeBased());
    updateRepairUnit(repairUnit);

    repairUnits.put(repairUnit.getId(), repairUnit);
    return repairUnit;
  }

  public void updateRepairUnit(RepairUnit updatedRepairUnit) {
    session.execute(
        insertRepairUnitPrepStmt.bind(
            updatedRepairUnit.getId(),
            updatedRepairUnit.getClusterName(),
            updatedRepairUnit.getKeyspaceName(),
            updatedRepairUnit.getColumnFamilies(),
            updatedRepairUnit.getIncrementalRepair(),
            updatedRepairUnit.getNodes(),
            updatedRepairUnit.getDatacenters(),
            updatedRepairUnit.getBlacklistedTables(),
            updatedRepairUnit.getRepairThreadCount(),
            updatedRepairUnit.getTimeout()));
  }

  RepairUnit getRepairUnitImpl(UUID id) {
    Row repairUnitRow = session.execute(getRepairUnitPrepStmt.bind(id)).one();
    if (repairUnitRow != null) {
      return RepairUnit.builder()
          .clusterName(repairUnitRow.getString("cluster_name"))
          .keyspaceName(repairUnitRow.getString("keyspace_name"))
          .columnFamilies(repairUnitRow.getSet("column_families", String.class))
          .incrementalRepair(repairUnitRow.getBool("incremental_repair"))
          .nodes(repairUnitRow.getSet("nodes", String.class))
          .datacenters(repairUnitRow.getSet("datacenters", String.class))
          .blacklistedTables(repairUnitRow.getSet("blacklisted_tables", String.class))
          .repairThreadCount(repairUnitRow.getInt("repair_thread_count"))
          .timeout(repairUnitRow.isNull("timeout") ? cassandraStorage.defaultTimeout : repairUnitRow.getInt("timeout"))
          .build(id);
    }
    throw new IllegalArgumentException("No repair unit exists for " + id);
  }


  public RepairUnit getRepairUnit(UUID id) {
    return repairUnits.getUnchecked(id);
  }

  public Optional<RepairUnit> getRepairUnit(RepairUnit.Builder params) {
    // brute force again
    RepairUnit repairUnit = null;
    Statement stmt = new SimpleStatement(CassandraStorage.SELECT_REPAIR_UNIT);
    stmt.setIdempotent(Boolean.TRUE);
    ResultSet results = session.execute(stmt);
    for (Row repairUnitRow : results) {
      RepairUnit existingRepairUnit = RepairUnit.builder()
          .clusterName(repairUnitRow.getString("cluster_name"))
          .keyspaceName(repairUnitRow.getString("keyspace_name"))
          .columnFamilies(repairUnitRow.getSet("column_families", String.class))
          .incrementalRepair(repairUnitRow.getBool("incremental_repair"))
          .nodes(repairUnitRow.getSet("nodes", String.class))
          .datacenters(repairUnitRow.getSet("datacenters", String.class))
          .blacklistedTables(repairUnitRow.getSet("blacklisted_tables", String.class))
          .repairThreadCount(repairUnitRow.getInt("repair_thread_count"))
          .timeout(repairUnitRow.isNull("timeout") ? cassandraStorage.defaultTimeout : repairUnitRow.getInt("timeout"))
          .build(repairUnitRow.getUUID("id"));
      if (existingRepairUnit.with().equals(params)) {
        repairUnit = existingRepairUnit;
        LOG.info("Found matching repair unit: {}", repairUnitRow.getUUID("id"));
        // exit the loop once we find a match
        break;
      }
    }

    return Optional.ofNullable(repairUnit);
  }
}