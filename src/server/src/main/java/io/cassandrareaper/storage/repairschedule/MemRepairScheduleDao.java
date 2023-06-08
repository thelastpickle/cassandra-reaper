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

package io.cassandrareaper.storage.repairschedule;

import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairUnit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.Maps;

public class MemRepairScheduleDao implements IRepairSchedule {
  public final ConcurrentMap<UUID, RepairSchedule> repairSchedules = Maps.newConcurrentMap();

  public MemRepairScheduleDao() {
  }

  @Override
  public RepairSchedule addRepairSchedule(RepairSchedule.Builder repairSchedule) {
    RepairSchedule newRepairSchedule = repairSchedule.build(UUIDs.timeBased());
    repairSchedules.put(newRepairSchedule.getId(), newRepairSchedule);
    return newRepairSchedule;
  }

  @Override
  public Optional<RepairSchedule> getRepairSchedule(UUID id) {
    return Optional.ofNullable(repairSchedules.get(id));
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForCluster(String clusterName) {
    Collection<RepairSchedule> foundRepairSchedules = new ArrayList<RepairSchedule>();
    for (RepairSchedule repairSchedule : repairSchedules.values()) {
      RepairUnit repairUnit = memoryStorageFacade.getMemRepairUnit().getRepairUnit(repairSchedule.getRepairUnitId());
      if (repairUnit.getClusterName().equals(clusterName)) {
        foundRepairSchedules.add(repairSchedule);
      }
    }
    return foundRepairSchedules;
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForCluster(String clusterName, boolean incremental) {
    return memoryStorageFacade.getRepairSchedulesForCluster(clusterName).stream()
        .filter(schedule -> memoryStorageFacade
              .getMemRepairUnit()
              .getRepairUnit(schedule.getRepairUnitId())
              .getIncrementalRepair() == incremental)
        .collect(Collectors.toList());
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForKeyspace(String keyspaceName) {
    Collection<RepairSchedule> foundRepairSchedules = new ArrayList<RepairSchedule>();
    for (RepairSchedule repairSchedule : repairSchedules.values()) {
      RepairUnit repairUnit = memoryStorageFacade.getMemRepairUnit().getRepairUnit(repairSchedule.getRepairUnitId());
      if (repairUnit.getKeyspaceName().equals(keyspaceName)) {
        foundRepairSchedules.add(repairSchedule);
      }
    }
    return foundRepairSchedules;
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForClusterAndKeyspace(String clusterName, String keyspaceName) {
    Collection<RepairSchedule> foundRepairSchedules = new ArrayList<RepairSchedule>();
    for (RepairSchedule repairSchedule : repairSchedules.values()) {
      RepairUnit repairUnit = memoryStorageFacade.getMemRepairUnit().getRepairUnit(repairSchedule.getRepairUnitId());
      if (repairUnit.getClusterName().equals(clusterName) && repairUnit.getKeyspaceName().equals(keyspaceName)) {
        foundRepairSchedules.add(repairSchedule);
      }
    }
    return foundRepairSchedules;
  }

  @Override
  public Collection<RepairSchedule> getAllRepairSchedules() {
    return repairSchedules.values();
  }

  @Override
  public boolean updateRepairSchedule(RepairSchedule newRepairSchedule) {
    if (repairSchedules.get(newRepairSchedule.getId()) == null) {
      return false;
    } else {
      repairSchedules.put(newRepairSchedule.getId(), newRepairSchedule);
      return true;
    }
  }

  @Override
  public Optional<RepairSchedule> deleteRepairSchedule(UUID id) {
    RepairSchedule deletedSchedule = repairSchedules.remove(id);
    if (deletedSchedule != null) {
      deletedSchedule = deletedSchedule.with().state(RepairSchedule.State.DELETED).build(id);
    }
    return Optional.ofNullable(deletedSchedule);
  }
}