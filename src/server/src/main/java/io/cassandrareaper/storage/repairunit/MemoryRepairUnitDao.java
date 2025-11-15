/*
 * Copyright 2014-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
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

package io.cassandrareaper.storage.repairunit;

import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.storage.MemoryStorageFacade;
import io.cassandrareaper.storage.sqlite.SqliteHelper;
import io.cassandrareaper.storage.sqlite.UuidUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryRepairUnitDao implements IRepairUnitDao {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryRepairUnitDao.class);

  private final Connection connection;
  private final PreparedStatement insertRepairUnitStmt;
  private final PreparedStatement getRepairUnitByIdStmt;
  private final PreparedStatement getRepairUnitByKeyStmt;
  private final PreparedStatement getRepairUnitsForClusterStmt;
  private final PreparedStatement deleteRepairUnitStmt;

  public MemoryRepairUnitDao(MemoryStorageFacade storage) {
    this.connection = storage.getSqliteConnection();

    try {
      this.insertRepairUnitStmt =
          connection.prepareStatement(
              "INSERT OR REPLACE INTO repair_unit (id, cluster_name, keyspace_name, "
                  + "column_families, incremental_repair, subrange_incremental, nodes, datacenters, "
                  + "blacklisted_tables, repair_thread_count, timeout) "
                  + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

      this.getRepairUnitByIdStmt =
          connection.prepareStatement("SELECT * FROM repair_unit WHERE id = ?");

      this.getRepairUnitByKeyStmt =
          connection.prepareStatement(
              "SELECT * FROM repair_unit WHERE cluster_name = ? AND keyspace_name = ? "
                  + "AND column_families = ? AND nodes = ? AND datacenters = ?");

      this.getRepairUnitsForClusterStmt =
          connection.prepareStatement("SELECT * FROM repair_unit WHERE cluster_name = ?");

      this.deleteRepairUnitStmt =
          connection.prepareStatement("DELETE FROM repair_unit WHERE id = ?");

    } catch (SQLException e) {
      LOG.error("Failed to prepare statements for MemoryRepairUnitDao", e);
      throw new RuntimeException("Failed to initialize MemoryRepairUnitDao", e);
    }
  }

  @Override
  public RepairUnit addRepairUnit(RepairUnit.Builder repairUnitBuilder) {
    Optional<RepairUnit> existing = getRepairUnit(repairUnitBuilder);
    if (existing.isPresent()
        && repairUnitBuilder.incrementalRepair == existing.get().getIncrementalRepair()
        && repairUnitBuilder.subrangeIncrementalRepair
            == existing.get().getSubrangeIncrementalRepair()) {
      return existing.get();
    } else {
      RepairUnit newRepairUnit = repairUnitBuilder.build(Uuids.timeBased());
      insertRepairUnit(newRepairUnit);
      return newRepairUnit;
    }
  }

  @Override
  public void updateRepairUnit(RepairUnit updatedRepairUnit) {
    insertRepairUnit(updatedRepairUnit); // INSERT OR REPLACE handles updates
  }

  @Override
  public RepairUnit getRepairUnit(UUID id) {
    synchronized (connection) {
      try {
        getRepairUnitByIdStmt.setBytes(1, UuidUtil.toBytes(id));
        try (ResultSet rs = getRepairUnitByIdStmt.executeQuery()) {
          if (rs.next()) {
            return mapRowToRepairUnit(rs);
          } else {
            throw new IllegalArgumentException("No repair unit found with id: " + id);
          }
        }
      } catch (SQLException e) {
        LOG.error("Failed to get repair unit by id: {}", id, e);
        throw new RuntimeException("Failed to get repair unit", e);
      } finally {
        try {
          getRepairUnitByIdStmt.clearParameters();
        } catch (SQLException e) {
          LOG.warn("Failed to clear parameters", e);
        }
      }
    }
  }

  @Override
  public Optional<RepairUnit> getRepairUnit(RepairUnit.Builder repairUnitBuilder) {
    synchronized (connection) {
      try {
        getRepairUnitByKeyStmt.setString(1, repairUnitBuilder.clusterName);
        getRepairUnitByKeyStmt.setString(2, repairUnitBuilder.keyspaceName);
        getRepairUnitByKeyStmt.setString(3, SqliteHelper.toJson(repairUnitBuilder.columnFamilies));
        getRepairUnitByKeyStmt.setString(4, SqliteHelper.toJson(repairUnitBuilder.nodes));
        getRepairUnitByKeyStmt.setString(5, SqliteHelper.toJson(repairUnitBuilder.datacenters));

        try (ResultSet rs = getRepairUnitByKeyStmt.executeQuery()) {
          if (rs.next()) {
            return Optional.of(mapRowToRepairUnit(rs));
          } else {
            return Optional.empty();
          }
        }
      } catch (SQLException e) {
        LOG.error("Failed to get repair unit by key", e);
        return Optional.empty();
      } finally {
        try {
          getRepairUnitByKeyStmt.clearParameters();
        } catch (SQLException e) {
          LOG.warn("Failed to clear parameters", e);
        }
      }
    }
  }

  /**
   * Get all repair units for a specific cluster.
   *
   * @param clusterName The cluster name
   * @return Collection of repair units
   */
  public Collection<RepairUnit> getRepairUnitsForCluster(String clusterName) {
    synchronized (connection) {
      try {
        getRepairUnitsForClusterStmt.setString(1, clusterName);
        try (ResultSet rs = getRepairUnitsForClusterStmt.executeQuery()) {
          Collection<RepairUnit> units = new ArrayList<>();
          while (rs.next()) {
            units.add(mapRowToRepairUnit(rs));
          }
          return units;
        }
      } catch (SQLException e) {
        LOG.error("Failed to get repair units for cluster: {}", clusterName, e);
        throw new RuntimeException("Failed to get repair units", e);
      } finally {
        try {
          getRepairUnitsForClusterStmt.clearParameters();
        } catch (SQLException e) {
          LOG.warn("Failed to clear parameters", e);
        }
      }
    }
  }

  /**
   * Delete a repair unit.
   *
   * @param id The repair unit ID
   */
  public void deleteRepairUnit(UUID id) {
    synchronized (connection) {
      try {
        deleteRepairUnitStmt.setBytes(1, UuidUtil.toBytes(id));
        deleteRepairUnitStmt.executeUpdate();
      } catch (SQLException e) {
        LOG.error("Failed to delete repair unit: {}", id, e);
        throw new RuntimeException("Failed to delete repair unit", e);
      } finally {
        try {
          deleteRepairUnitStmt.clearParameters();
        } catch (SQLException e) {
          LOG.warn("Failed to clear parameters", e);
        }
      }
    }
  }

  /**
   * Insert a repair unit into the database.
   *
   * @param repairUnit The repair unit to insert
   */
  private void insertRepairUnit(RepairUnit repairUnit) {
    synchronized (connection) {
      try {
        insertRepairUnitStmt.setBytes(1, UuidUtil.toBytes(repairUnit.getId()));
        insertRepairUnitStmt.setString(2, repairUnit.getClusterName());
        insertRepairUnitStmt.setString(3, repairUnit.getKeyspaceName());
        insertRepairUnitStmt.setString(4, SqliteHelper.toJson(repairUnit.getColumnFamilies()));
        insertRepairUnitStmt.setInt(5, repairUnit.getIncrementalRepair() ? 1 : 0);
        insertRepairUnitStmt.setInt(6, repairUnit.getSubrangeIncrementalRepair() ? 1 : 0);
        insertRepairUnitStmt.setString(7, SqliteHelper.toJson(repairUnit.getNodes()));
        insertRepairUnitStmt.setString(8, SqliteHelper.toJson(repairUnit.getDatacenters()));
        insertRepairUnitStmt.setString(9, SqliteHelper.toJson(repairUnit.getBlacklistedTables()));
        insertRepairUnitStmt.setInt(10, repairUnit.getRepairThreadCount());
        insertRepairUnitStmt.setInt(11, repairUnit.getTimeout());

        insertRepairUnitStmt.executeUpdate();
      } catch (SQLException e) {
        LOG.error("Failed to insert repair unit: {}", repairUnit.getId(), e);
        throw new RuntimeException("Failed to insert repair unit", e);
      } finally {
        try {
          insertRepairUnitStmt.clearParameters();
        } catch (SQLException e) {
          LOG.warn("Failed to clear parameters", e);
        }
      }
    }
  }

  /**
   * Map a SQL ResultSet row to a RepairUnit object.
   *
   * @param rs The ResultSet positioned at a row
   * @return The RepairUnit object
   */
  private RepairUnit mapRowToRepairUnit(ResultSet rs) throws SQLException {
    UUID id = UuidUtil.fromBytes(rs.getBytes("id"));
    String clusterName = rs.getString("cluster_name");
    String keyspaceName = rs.getString("keyspace_name");
    String columnFamiliesJson = rs.getString("column_families");
    boolean incrementalRepair = rs.getInt("incremental_repair") == 1;
    boolean subrangeIncremental = rs.getInt("subrange_incremental") == 1;
    String nodesJson = rs.getString("nodes");
    String datacentersJson = rs.getString("datacenters");
    String blacklistedTablesJson = rs.getString("blacklisted_tables");
    int repairThreadCount = rs.getInt("repair_thread_count");
    int timeout = rs.getInt("timeout");

    Set<String> columnFamilies =
        SqliteHelper.fromJson(columnFamiliesJson, new TypeReference<Set<String>>() {});
    Set<String> nodes = SqliteHelper.fromJson(nodesJson, new TypeReference<Set<String>>() {});
    Set<String> datacenters =
        SqliteHelper.fromJson(datacentersJson, new TypeReference<Set<String>>() {});
    Set<String> blacklistedTables =
        SqliteHelper.fromJson(blacklistedTablesJson, new TypeReference<Set<String>>() {});

    return RepairUnit.builder()
        .clusterName(clusterName)
        .keyspaceName(keyspaceName)
        .columnFamilies(columnFamilies)
        .incrementalRepair(incrementalRepair)
        .subrangeIncrementalRepair(subrangeIncremental)
        .nodes(nodes)
        .datacenters(datacenters)
        .blacklistedTables(blacklistedTables)
        .repairThreadCount(repairThreadCount)
        .timeout(timeout)
        .build(id);
  }
}
