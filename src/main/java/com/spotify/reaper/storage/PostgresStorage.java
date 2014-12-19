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

import com.spotify.reaper.ReaperApplicationConfiguration;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.ColumnFamily;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.service.RingRange;
import com.spotify.reaper.storage.postgresql.BigIntegerArgumentFactory;
import com.spotify.reaper.storage.postgresql.IStoragePostgreSQL;
import com.spotify.reaper.storage.postgresql.PostgresArrayArgumentFactory;
import com.spotify.reaper.storage.postgresql.RunStateArgumentFactory;
import com.spotify.reaper.storage.postgresql.StateArgumentFactory;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nullable;

import io.dropwizard.jdbi.DBIFactory;
import io.dropwizard.setup.Environment;

/**
 * Implements the StorageAPI using PostgreSQL database.
 */
public class PostgresStorage implements IStorage {

  private static final Logger LOG = LoggerFactory.getLogger(PostgresStorage.class);

  private final DBI jdbi;

  public PostgresStorage(ReaperApplicationConfiguration config, Environment environment)
      throws ReaperException {
    try {
      final DBIFactory factory = new DBIFactory();
      jdbi = factory.build(environment, config.getDataSourceFactory(), "postgresql");
    } catch (ClassNotFoundException ex) {
      LOG.error("failed creating database connection: {}", ex);
      throw new ReaperException(ex);
    }
  }

  private static IStoragePostgreSQL getPostgresStorage(Handle h) {
    h.registerArgumentFactory(new PostgresArrayArgumentFactory());
    h.registerArgumentFactory(new RunStateArgumentFactory());
    h.registerArgumentFactory(new StateArgumentFactory());
    h.registerArgumentFactory(new BigIntegerArgumentFactory());
    return h.attach(IStoragePostgreSQL.class);
  }

  @Override
  public Cluster getCluster(String clusterName) {
    Cluster result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getCluster(clusterName);
    }
    return result;
  }

  @Override
  public boolean isStorageConnected() {
    String postgresVersion = null;
    if (null != jdbi) {
      try (Handle h = jdbi.open()) {
        postgresVersion = getPostgresStorage(h).getVersion();
        LOG.debug("connected postgresql version: {}", postgresVersion);
      }
    }
    return null != postgresVersion && postgresVersion.trim().length() > 0;
  }

  @Override
  public Collection<Cluster> getClusters() {
    Collection<Cluster> result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getClusters();
    }
    return result;
  }

  @Override
  public Cluster addCluster(Cluster newCluster) {
    Cluster result = null;
    try (Handle h = jdbi.open()) {
      int rowsAdded = getPostgresStorage(h).insertCluster(newCluster);
      if (rowsAdded < 1) {
        LOG.warn("failed inserting cluster with name: {}", newCluster.getName());
      } else {
        result = newCluster; // no created id, as cluster name used for primary key
      }
    }
    return result;
  }

  @Override
  public boolean updateCluster(Cluster cluster) {
    boolean result = false;
    try (Handle h = jdbi.open()) {
      int rowsAdded = getPostgresStorage(h).updateCluster(cluster);
      if (rowsAdded < 1) {
        LOG.warn("failed updating cluster with name: {}", cluster.getName());
      } else {
        result = true;
      }
    }
    return result;
  }

  @Override
  public RepairRun getRepairRun(long id) {
    RepairRun result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getRepairRun(id);
    }
    return result;
  }

  @Override
  public Collection<RepairRun> getRepairRunsForCluster(String clusterName) {
    Collection<RepairRun> result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getRepairRunsForCluster(clusterName);
    }
    return result;
  }

  @Override
  public Collection<RepairRun> getAllRunningRepairRuns() {
    Collection<RepairRun> result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getRepairRunsWithState(RepairRun.RunState.RUNNING);
    }
    return result;
  }

  @Override
  public RepairRun addRepairRun(RepairRun.Builder newRepairRun) {
    RepairRun result;
    try (Handle h = jdbi.open()) {
      long insertedId = getPostgresStorage(h).insertRepairRun(newRepairRun.build(-1));
      result = newRepairRun.build(insertedId);
    }
    return result;
  }

  @Override
  public boolean updateRepairRun(RepairRun repairRun) {
    boolean result = false;
    try (Handle h = jdbi.open()) {
      int rowsAdded = getPostgresStorage(h).updateRepairRun(repairRun);
      if (rowsAdded < 1) {
        LOG.warn("failed updating repair run with id: {}", repairRun.getId());
      } else {
        result = true;
      }
    }
    return result;
  }

  @Override
  public ColumnFamily addColumnFamily(ColumnFamily.Builder newColumnFamily) {
    ColumnFamily result;
    try (Handle h = jdbi.open()) {
      long insertedId = getPostgresStorage(h).insertColumnFamily(newColumnFamily.build(-1));
      result = newColumnFamily.build(insertedId);
    }
    return result;
  }

  @Override
  public ColumnFamily getColumnFamily(long id) {
    ColumnFamily result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getColumnFamily(id);
    }
    return result;
  }

  @Override
  public ColumnFamily getColumnFamily(String clusterName, String keyspaceName, String tableName) {
    ColumnFamily result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getColumnFamilyByClusterAndName(clusterName, keyspaceName,
                                                                     tableName);
    }
    return result;
  }

  @Override
  public void addRepairSegments(Collection<RepairSegment.Builder> newSegments) {
    List<RepairSegment> insertableSegments = new ArrayList<>();
    for (RepairSegment.Builder segment : newSegments) {
      insertableSegments.add(segment.build(-1));
    }
    try (Handle h = jdbi.open()) {
      getPostgresStorage(h).insertRepairSegments(insertableSegments.iterator());
    }
  }

  @Override
  public boolean updateRepairSegment(RepairSegment repairSegment) {
    boolean result = false;
    try (Handle h = jdbi.open()) {
      int rowsAdded = getPostgresStorage(h).updateRepairSegment(repairSegment);
      if (rowsAdded < 1) {
        LOG.warn("failed updating repair segment with id: {}", repairSegment.getId());
      } else {
        result = true;
      }
    }
    return result;
  }

  @Override
  public RepairSegment getRepairSegment(long id) {
    RepairSegment result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getRepairSegment(id);
    }
    return result;
  }

  @Override
  public RepairSegment getNextFreeSegment(long runId) {
    RepairSegment result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getNextFreeRepairSegment(runId);
    }
    return result;
  }

  @Override
  public RepairSegment getNextFreeSegmentInRange(long runId, RingRange range) {
    RepairSegment result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getNextFreeRepairSegmentOnRange(runId, range.getStart(),
                                                                     range.getEnd());
    }
    return result;
  }

  @Nullable
  @Override
  public RepairSegment getTheRunningSegment(long runId) {
    RepairSegment result = null;
    try (Handle h = jdbi.open()) {
      Collection<RepairSegment> segments =
          getPostgresStorage(h).getRepairSegmentForRunWithState(runId, RepairSegment.State.RUNNING);
      if (null != segments) {
        assert segments.size() < 2 : "there are more than one RUNNING segment on run: " + runId;
        if (segments.size() == 1) {
          result = segments.iterator().next();
        }
      }
    }
    return result;
  }

  @Override
  public Collection<Long> getRepairRunIdsForCluster(String clusterName) {
    Collection<Long> result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getRepairRunIdsForCluster(clusterName);
    }
    return result;
  }

  @Override
  public int getSegmentAmountForRepairRun(long runId, RepairSegment.State state) {
    int result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getSegmentAmountForRepairRun(runId, state);
    }
    return result;
  }
}
