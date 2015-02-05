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

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import com.spotify.reaper.ReaperApplicationConfiguration;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.core.RepairUnit;
import com.spotify.reaper.service.RingRange;
import com.spotify.reaper.storage.postgresql.BigIntegerArgumentFactory;
import com.spotify.reaper.storage.postgresql.IStoragePostgreSQL;
import com.spotify.reaper.storage.postgresql.PostgresArrayArgumentFactory;
import com.spotify.reaper.storage.postgresql.RepairParallelismArgumentFactory;
import com.spotify.reaper.storage.postgresql.RunStateArgumentFactory;
import com.spotify.reaper.storage.postgresql.StateArgumentFactory;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

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
    h.registerArgumentFactory(new RepairParallelismArgumentFactory());
    h.registerArgumentFactory(new StateArgumentFactory());
    h.registerArgumentFactory(new BigIntegerArgumentFactory());
    return h.attach(IStoragePostgreSQL.class);
  }

  @Override
  public Optional<Cluster> getCluster(String clusterName) {
    Cluster result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getCluster(clusterName);
    }
    return result == null ? Optional.<Cluster>absent() : Optional.of(result);
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
    return result != null ? result : Lists.<Cluster>newArrayList();
  }

  @Override
  public boolean addCluster(Cluster newCluster) {
    Cluster result = null;
    try (Handle h = jdbi.open()) {
      int rowsAdded = getPostgresStorage(h).insertCluster(newCluster);
      if (rowsAdded < 1) {
        LOG.warn("failed inserting cluster with name: {}", newCluster.getName());
      } else {
        result = newCluster; // no created id, as cluster name used for primary key
      }
    }
    return result != null;
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
  public Optional<RepairRun> getRepairRun(long id) {
    RepairRun result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getRepairRun(id);
    }
    return result == null ? Optional.<RepairRun>absent() : Optional.of(result);
  }

  @Override
  public Collection<RepairRun> getRepairRunsForCluster(String clusterName) {
    Collection<RepairRun> result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getRepairRunsForCluster(clusterName);
    }
    return result == null ? Lists.<RepairRun>newArrayList() : result;
  }

  @Override
  public Collection<RepairRun> getRepairRunsWithState(RepairRun.RunState runState) {
    Collection<RepairRun> result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getRepairRunsWithState(runState);
    }
    return result == null ? Lists.<RepairRun>newArrayList() : result;
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
  public RepairUnit addRepairUnit(RepairUnit.Builder newRepairUnit) {
    long insertedId;
    try (Handle h = jdbi.open()) {
      insertedId = getPostgresStorage(h).insertRepairUnit(newRepairUnit.build(-1));
    }
    return newRepairUnit.build(insertedId);
  }

  @Override
  public Optional<RepairUnit> getRepairUnit(long id) {
    RepairUnit result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getRepairUnit(id);
    }
    return result == null ? Optional.<RepairUnit>absent() : Optional.of(result);
  }

  @Override
  public Optional<RepairUnit> getRepairUnit(String clusterName, String keyspaceName,
                                            Set<String> columnFamilies) {
    RepairUnit result;
    try (Handle h = jdbi.open()) {
      IStoragePostgreSQL storage = getPostgresStorage(h);
      result = storage.getRepairUnitByClusterAndTables(clusterName, keyspaceName, columnFamilies);
    }
    return result == null ? Optional.<RepairUnit>absent() : Optional.of(result);
  }

  @Override
  public void addRepairSegments(Collection<RepairSegment.Builder> newSegments, long runId) {
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
  public Optional<RepairSegment> getRepairSegment(long id) {
    RepairSegment result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getRepairSegment(id);
    }
    return result == null ? Optional.<RepairSegment>absent() : Optional.of(result);
  }

  @Override
  public Optional<RepairSegment> getNextFreeSegment(long runId) {
    RepairSegment result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getNextFreeRepairSegment(runId);
    }
    return result == null ? Optional.<RepairSegment>absent() : Optional.of(result);
  }

  @Override
  public Optional<RepairSegment> getNextFreeSegmentInRange(long runId, RingRange range) {
    RepairSegment result;
    try (Handle h = jdbi.open()) {
      IStoragePostgreSQL storage = getPostgresStorage(h);
      result = storage.getNextFreeRepairSegmentOnRange(runId, range.getStart(), range.getEnd());
    }
    return result == null ? Optional.<RepairSegment>absent() : Optional.of(result);
  }

  @Override
  public Collection<RepairSegment> getSegmentsWithState(long runId,
                                                        RepairSegment.State segmentState) {
    Collection<RepairSegment> result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getRepairSegmentForRunWithState(runId, segmentState);
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
