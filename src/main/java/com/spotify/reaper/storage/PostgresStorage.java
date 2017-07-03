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

import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSchedule;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.core.RepairUnit;
import com.spotify.reaper.resources.view.RepairRunStatus;
import com.spotify.reaper.resources.view.RepairScheduleStatus;
import com.spotify.reaper.service.RepairParameters;
import com.spotify.reaper.service.RingRange;
import com.spotify.reaper.storage.postgresql.BigIntegerArgumentFactory;
import com.spotify.reaper.storage.postgresql.IStoragePostgreSQL;
import com.spotify.reaper.storage.postgresql.LongCollectionSQLTypeArgumentFactory;
import com.spotify.reaper.storage.postgresql.PostgresArrayArgumentFactory;
import com.spotify.reaper.storage.postgresql.RepairParallelismArgumentFactory;
import com.spotify.reaper.storage.postgresql.RunStateArgumentFactory;
import com.spotify.reaper.storage.postgresql.ScheduleStateArgumentFactory;
import com.spotify.reaper.storage.postgresql.StateArgumentFactory;
import com.spotify.reaper.storage.postgresql.UuidArgumentFactory;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.DBIException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;


/**
 * Implements the StorageAPI using PostgreSQL database.
 */
public final class PostgresStorage implements IStorage {

  private static final Logger LOG = LoggerFactory.getLogger(PostgresStorage.class);

  private final DBI jdbi;

  public PostgresStorage(DBI jdbi) {
    this.jdbi = jdbi;
  }

  private static IStoragePostgreSQL getPostgresStorage(Handle h) {
    h.registerArgumentFactory(new LongCollectionSQLTypeArgumentFactory());
    h.registerArgumentFactory(new PostgresArrayArgumentFactory());
    h.registerArgumentFactory(new RunStateArgumentFactory());
    h.registerArgumentFactory(new RepairParallelismArgumentFactory());
    h.registerArgumentFactory(new StateArgumentFactory());
    h.registerArgumentFactory(new BigIntegerArgumentFactory());
    h.registerArgumentFactory(new ScheduleStateArgumentFactory());
    h.registerArgumentFactory(new UuidArgumentFactory());
    return h.attach(IStoragePostgreSQL.class);
  }

  @Override
  public Optional<Cluster> getCluster(String clusterName) {
    Cluster result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getCluster(clusterName);
    }
    return Optional.fromNullable(result);
  }

  @Override
  public Optional<Cluster> deleteCluster(String clusterName) {
    Cluster result = null;
    try (Handle h = jdbi.open()) {
      IStoragePostgreSQL pg = getPostgresStorage(h);
      Cluster clusterToDel = pg.getCluster(clusterName);
      if (clusterToDel != null) {
        int rowsDeleted = pg.deleteCluster(clusterName);
        if (rowsDeleted > 0) {
          result = clusterToDel;
        }
      }
    }
    return Optional.fromNullable(result);
  }

  @Override
  public boolean isStorageConnected() {
    String currentDate = null;
    if (null != jdbi) {
      try (Handle h = jdbi.open()) {
        currentDate = getPostgresStorage(h).getCurrentDate();        
      }
    }
    return null != currentDate && !currentDate.trim().isEmpty();
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
  public Optional<RepairRun> getRepairRun(UUID id) {
    RepairRun result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getRepairRun(toSequenceId(id));
    }
    return Optional.fromNullable(result);
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
  public Collection<RepairRun> getRepairRunsForUnit(UUID repairUnitId) {
    Collection<RepairRun> result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getRepairRunsForUnit(toSequenceId(repairUnitId));
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
  public Optional<RepairRun> deleteRepairRun(UUID id) {
    RepairRun result = null;
    Handle h = null;
    try {
      h = jdbi.open();
      h.begin();
      IStoragePostgreSQL pg = getPostgresStorage(h);
      RepairRun runToDelete = pg.getRepairRun(toSequenceId(id));
      if (runToDelete != null) {
        int segmentsRunning = pg.getSegmentAmountForRepairRunWithState(toSequenceId(id),
            RepairSegment.State.RUNNING);
        if (segmentsRunning == 0) {
          pg.deleteRepairSegmentsForRun(toSequenceId(runToDelete.getId()));
          pg.deleteRepairRun(toSequenceId(id));
          result = runToDelete.with().runState(RepairRun.RunState.DELETED).build(id);
        } else {
          LOG.warn("not deleting RepairRun \"{}\" as it has segments running: {}",
              id, segmentsRunning);
        }
      }
      h.commit();
    } catch (DBIException ex) {
      LOG.warn("DELETE failed", ex);
      ex.printStackTrace();
      if (h != null) {
        h.rollback();
      }
    } finally {
      if (h != null) {
        h.close();
      }
    }
    if (result != null) {
      tryDeletingRepairUnit(result.getRepairUnitId());
    }
    return Optional.fromNullable(result);
  }

  private void tryDeletingRepairUnit(UUID id) {
    try (Handle h = jdbi.open()) {
      IStoragePostgreSQL pg = getPostgresStorage(h);
      pg.deleteRepairUnit(toSequenceId(id));
    } catch (DBIException ex) {
      LOG.info("cannot delete RepairUnit with id " + id);
    }
  }

  @Override
  public RepairRun addRepairRun(RepairRun.Builder newRepairRun, Collection<RepairSegment.Builder> newSegments) {
    RepairRun result;
    try (Handle h = jdbi.open()) {
      long insertedId = getPostgresStorage(h).insertRepairRun(newRepairRun.build(null));
      result = newRepairRun.build(fromSequenceId(insertedId));
    }
    addRepairSegments(newSegments, result.getId());
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
      insertedId = getPostgresStorage(h).insertRepairUnit(newRepairUnit.build(null));
    }
    return newRepairUnit.build(fromSequenceId(insertedId));
  }

  @Override
  public Optional<RepairUnit> getRepairUnit(UUID id) {
    RepairUnit result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getRepairUnit(toSequenceId(id));
    }
    return Optional.fromNullable(result);
  }

  @Override
  public Optional<RepairUnit> getRepairUnit(String clusterName, String keyspaceName,
      Set<String> columnFamilies) {
    RepairUnit result;
    try (Handle h = jdbi.open()) {
      IStoragePostgreSQL storage = getPostgresStorage(h);
      result = storage.getRepairUnitByClusterAndTables(clusterName, keyspaceName, columnFamilies);
    }
    return Optional.fromNullable(result);
  }

  private void addRepairSegments(Collection<RepairSegment.Builder> newSegments, UUID runId) {
    List<RepairSegment> insertableSegments = new ArrayList<>();
    for (RepairSegment.Builder segment : newSegments) {
      insertableSegments.add(segment.withRunId(runId).build(null));
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
  public Optional<RepairSegment> getRepairSegment(UUID runId, UUID segmentId) {
    RepairSegment result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getRepairSegment(toSequenceId(segmentId));
    }
    return Optional.fromNullable(result);
  }

  @Override
  public Collection<RepairSegment> getRepairSegmentsForRun(UUID runId) {
    try (Handle h = jdbi.open()) {
      return getPostgresStorage(h).getRepairSegmentsForRun(toSequenceId(runId));
    }
  }

  @Override
  public Optional<RepairSegment> getNextFreeSegment(UUID runId) {
    RepairSegment result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getNextFreeRepairSegment(toSequenceId(runId));
    }
    return Optional.fromNullable(result);
  }

  @Override
  public Optional<RepairSegment> getNextFreeSegmentInRange(UUID runId, RingRange range) {
    RepairSegment result;
    try (Handle h = jdbi.open()) {
      IStoragePostgreSQL storage = getPostgresStorage(h);
      if (!range.isWrapping()) {
        result = storage.getNextFreeRepairSegmentInNonWrappingRange(toSequenceId(runId), range.getStart(),
            range.getEnd());
      } else {
        result = storage.getNextFreeRepairSegmentInWrappingRange(toSequenceId(runId), range.getStart(),
            range.getEnd());
      }
    }
    return Optional.fromNullable(result);
  }

  @Override
  public Collection<RepairSegment> getSegmentsWithState(UUID runId, RepairSegment.State segmentState) {
    Collection<RepairSegment> result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getRepairSegmentsForRunWithState(toSequenceId(runId), segmentState);
    }
    return result;
  }

  @Override
  public Collection<RepairParameters> getOngoingRepairsInCluster(String clusterName) {
    try (Handle h = jdbi.open()) {
      return getPostgresStorage(h).getRunningRepairsForCluster(clusterName);
    }
  }

  @Override
  public Collection<UUID> getRepairRunIdsForCluster(String clusterName) {
    Collection<UUID> result = Lists.newArrayList();
    try (Handle h = jdbi.open()) {
      for(Long l: getPostgresStorage(h).getRepairRunIdsForCluster(clusterName)){
          result.add(fromSequenceId(l));
      }
    }
    return result;
  }

  @Override
  public int getSegmentAmountForRepairRun(UUID runId) {
    try (Handle h = jdbi.open()) {
      return getPostgresStorage(h).getSegmentAmountForRepairRun(toSequenceId(runId));
    }
  }

  @Override
  public int getSegmentAmountForRepairRunWithState(UUID runId, RepairSegment.State state) {
    int result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getSegmentAmountForRepairRunWithState(toSequenceId(runId), state);
    }
    return result;
  }

  @Override
  public RepairSchedule addRepairSchedule(RepairSchedule.Builder repairSchedule) {
    long insertedId;
    try (Handle h = jdbi.open()) {
      insertedId = getPostgresStorage(h).insertRepairSchedule(repairSchedule.build(null));
    }
    return repairSchedule.build(fromSequenceId(insertedId));
  }

  @Override
  public Optional<RepairSchedule> getRepairSchedule(UUID repairScheduleId) {
    RepairSchedule result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getRepairSchedule(toSequenceId(repairScheduleId));
    }
    return Optional.fromNullable(result);
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForCluster(String clusterName) {
    Collection<RepairSchedule> result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getRepairSchedulesForCluster(clusterName);
    }
    return result;
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForKeyspace(String keyspaceName) {
    Collection<RepairSchedule> result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getRepairSchedulesForKeyspace(keyspaceName);
    }
    return result;
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForClusterAndKeyspace(String clusterName,
      String keyspaceName) {
    Collection<RepairSchedule> result;
    try (Handle h = jdbi.open()) {
      result =
          getPostgresStorage(h).getRepairSchedulesForClusterAndKeySpace(clusterName, keyspaceName);
    }
    return result;
  }

  @Override
  public Collection<RepairSchedule> getAllRepairSchedules() {
    Collection<RepairSchedule> result;
    try (Handle h = jdbi.open()) {
      result = getPostgresStorage(h).getAllRepairSchedules();
    }
    return result;
  }

  @Override
  public boolean updateRepairSchedule(RepairSchedule newRepairSchedule) {
    boolean result = false;
    try (Handle h = jdbi.open()) {
      int rowsAdded = getPostgresStorage(h).updateRepairSchedule(newRepairSchedule);
      if (rowsAdded < 1) {
        LOG.warn("failed updating repair schedule with id: {}", newRepairSchedule.getId());
      } else {
        result = true;
      }
    }
    return result;
  }

  @Override
  public Optional<RepairSchedule> deleteRepairSchedule(UUID id) {
    RepairSchedule result = null;
    try (Handle h = jdbi.open()) {
      IStoragePostgreSQL pg = getPostgresStorage(h);
      RepairSchedule scheduleToDel = pg.getRepairSchedule(toSequenceId(id));
      if (scheduleToDel != null) {
        int rowsDeleted = pg.deleteRepairSchedule(toSequenceId(scheduleToDel.getId()));
        if (rowsDeleted > 0) {
          result = scheduleToDel.with().state(RepairSchedule.State.DELETED).build(id);
        }
      }
    }
    if (result != null) {
      tryDeletingRepairUnit(result.getRepairUnitId());
    }
    return Optional.fromNullable(result);
  }

  @Override
  public Collection<RepairRunStatus> getClusterRunStatuses(String clusterName, int limit) {
    try (Handle h = jdbi.open()) {
      return getPostgresStorage(h).getClusterRunOverview(clusterName, limit);
    }
  }

  @Override
  public Collection<RepairScheduleStatus> getClusterScheduleStatuses(String clusterName) {
    try (Handle h = jdbi.open()) {
      return getPostgresStorage(h).getClusterScheduleOverview(clusterName);
    }
  }

  public static UUID fromSequenceId(long insertedId) {
    return new UUID(insertedId, 0L);
  }

  public static long toSequenceId(UUID id) {
    return id.getMostSignificantBits();
  }
}
