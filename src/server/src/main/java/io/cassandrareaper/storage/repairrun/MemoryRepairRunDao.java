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

package io.cassandrareaper.storage.repairrun;

import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.resources.view.RepairRunStatus;
import io.cassandrareaper.service.RepairRunService;
import io.cassandrareaper.storage.MemoryStorageFacade;
import io.cassandrareaper.storage.repairsegment.MemoryRepairSegmentDao;
import io.cassandrareaper.storage.repairunit.MemoryRepairUnitDao;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.joda.time.DateTime;

public class MemoryRepairRunDao implements IRepairRunDao {
  private final MemoryRepairSegmentDao memRepairSegment;
  private final MemoryRepairUnitDao memoryRepairUnitDao;
  private final MemoryStorageFacade storage;


  public MemoryRepairRunDao(
      MemoryStorageFacade storage,
      MemoryRepairSegmentDao memRepairSegment,
       MemoryRepairUnitDao memoryRepairUnitDao) {
    this.memRepairSegment = memRepairSegment;
    this.memoryRepairUnitDao = memoryRepairUnitDao;
    this.storage = storage;
  }

  @Override
  public Collection<RepairRunStatus> getClusterRunStatuses(String clusterName, int limit) {
    List<RepairRunStatus> runStatuses = Lists.newArrayList();
    for (RepairRun run : getRepairRunsForCluster(clusterName, Optional.of(limit))) {
      RepairUnit unit = memoryRepairUnitDao.getRepairUnit(run.getRepairUnitId());
      int segmentsRepaired = memRepairSegment
            .getSegmentAmountForRepairRunWithState(run.getId(), RepairSegment.State.DONE);
      int totalSegments = memRepairSegment.getSegmentAmountForRepairRun(run.getId());
      runStatuses.add(
            new RepairRunStatus(
                  run.getId(),
                  clusterName,
                  unit.getKeyspaceName(),
                  run.getTables(),
                  segmentsRepaired,
                  totalSegments,
                  run.getRunState(),
                  run.getStartTime(),
                  run.getEndTime(),
                  run.getCause(),
                  run.getOwner(),
                  run.getLastEvent(),
                  run.getCreationTime(),
                  run.getPauseTime(),
                  run.getIntensity(),
                  unit.getIncrementalRepair(),
                  unit.getSubrangeIncrementalRepair(),
                  run.getRepairParallelism(),
                  unit.getNodes(),
                  unit.getDatacenters(),
                  unit.getBlacklistedTables(),
                  unit.getRepairThreadCount(),
                  unit.getId(),
                  unit.getTimeout(),
                  run.getAdaptiveSchedule()));
    }
    return runStatuses;
  }

  @Override
  public RepairRun addRepairRun(RepairRun.Builder repairRun, Collection<RepairSegment.Builder> newSegments) {
    RepairRun newRepairRun = repairRun.build(Uuids.timeBased());
    storage.addRepairRun(newRepairRun);
    memRepairSegment.addRepairSegments(newSegments, newRepairRun.getId());
    return newRepairRun;
  }


  public boolean updateRepairRun(RepairRun repairRun) {
    return updateRepairRun(repairRun, Optional.of(true));
  }


  public boolean updateRepairRun(RepairRun repairRun, Optional<Boolean> updateRepairState) {
    if (!getRepairRun(repairRun.getId()).isPresent()) {
      return false;
    } else {
      storage.addRepairRun(repairRun);
      return true;
    }
  }


  public Optional<RepairRun> getRepairRun(UUID id) {
    return storage.getRepairRunById(id);
  }


  public List<RepairRun> getRepairRunsForCluster(String clusterName, Optional<Integer> limit) {
    List<RepairRun> foundRepairRuns = new ArrayList<RepairRun>();
    TreeMap<UUID, RepairRun> reverseOrder = new TreeMap<UUID, RepairRun>(Collections.reverseOrder());
    storage.getRepairRuns().forEach(repairRun -> {
      reverseOrder.put(repairRun.getId(), repairRun);
    });
    for (RepairRun repairRun : reverseOrder.values()) {
      if (repairRun.getClusterName().equalsIgnoreCase(clusterName)) {
        foundRepairRuns.add(repairRun);
        if (foundRepairRuns.size() == limit.orElse(1000)) {
          break;
        }
      }
    }
    return foundRepairRuns;
  }


  public List<RepairRun> getRepairRunsForClusterPrioritiseRunning(String clusterName, Optional<Integer> limit) {
    List<RepairRun> foundRepairRuns = storage.getRepairRuns()
        .stream()
        .filter(
            row -> row.getClusterName().equals(clusterName.toLowerCase(Locale.ROOT))).collect(Collectors.toList()
        );
    RepairRunService.sortByRunState(foundRepairRuns);
    return foundRepairRuns.subList(0, Math.min(foundRepairRuns.size(), limit.orElse(1000)));
  }


  public Collection<RepairRun> getRepairRunsForUnit(UUID repairUnitId) {
    List<RepairRun> foundRepairRuns = new ArrayList<RepairRun>();
    for (RepairRun repairRun : storage.getRepairRuns()) {
      if (repairRun.getRepairUnitId().equals(repairUnitId)) {
        foundRepairRuns.add(repairRun);
      }
    }
    return foundRepairRuns;
  }


  public Collection<RepairRun> getRepairRunsWithState(RepairRun.RunState runState) {
    List<RepairRun> foundRepairRuns = new ArrayList<RepairRun>();
    for (RepairRun repairRun : storage.getRepairRuns()) {
      if (repairRun.getRunState() == runState) {
        foundRepairRuns.add(repairRun);
      }
    }
    return foundRepairRuns;
  }


  public Optional<RepairRun> deleteRepairRun(UUID id) {
    RepairRun deletedRun = storage.removeRepairRun(id);
    if (deletedRun != null) {
      if (memRepairSegment.getSegmentAmountForRepairRunWithState(id, RepairSegment.State.RUNNING) == 0) {
        memRepairSegment.deleteRepairSegmentsForRun(id);

        deletedRun = deletedRun.with()
            .runState(RepairRun.RunState.DELETED)
            .endTime(DateTime.now())
            .build(id);
      }
    }
    return Optional.ofNullable(deletedRun);
  }


  public SortedSet<UUID> getRepairRunIdsForCluster(String clusterName, Optional<Integer> limit) {
    SortedSet<UUID> repairRunIds = Sets.newTreeSet((u0, u1) -> (int) (u0.timestamp() - u1.timestamp()));
    for (RepairRun repairRun : storage.getRepairRuns()) {
      if (repairRun.getClusterName().equalsIgnoreCase(clusterName)) {
        repairRunIds.add(repairRun.getId());
      }
    }
    return repairRunIds;
  }
}