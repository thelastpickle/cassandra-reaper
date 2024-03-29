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

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class MemoryRepairUnitDao implements IRepairUnitDao {
  public final ConcurrentMap<UUID, RepairUnit> repairUnits = Maps.newConcurrentMap();
  public final ConcurrentMap<RepairUnit.Builder, RepairUnit> repairUnitsByKey = Maps.newConcurrentMap();

  public MemoryRepairUnitDao() {
  }

  /**
   * Delete a RepairUnit instance from Storage, but only if no run or schedule is referencing it.
   *
   * @param repairUnitId The RepairUnit instance id to delete.
   * @return The deleted RepairUnit instance, if delete succeeded.
   */

  @Override
  public RepairUnit addRepairUnit(RepairUnit.Builder repairUnit) {
    Optional<RepairUnit> existing = getRepairUnit(repairUnit);
    if (existing.isPresent() && repairUnit.incrementalRepair == existing.get().getIncrementalRepair()) {
      return existing.get();
    } else {
      RepairUnit newRepairUnit = repairUnit.build(UUIDs.timeBased());
      repairUnits.put(newRepairUnit.getId(), newRepairUnit);
      repairUnitsByKey.put(repairUnit, newRepairUnit);
      return newRepairUnit;
    }
  }

  @Override
  public void updateRepairUnit(RepairUnit updatedRepairUnit) {
    repairUnits.put(updatedRepairUnit.getId(), updatedRepairUnit);
    repairUnitsByKey.put(updatedRepairUnit.with(), updatedRepairUnit);
  }

  @Override
  public RepairUnit getRepairUnit(UUID id) {
    RepairUnit unit = repairUnits.get(id);
    Preconditions.checkArgument(null != unit);
    return unit;
  }

  @Override
  public Optional<RepairUnit> getRepairUnit(RepairUnit.Builder params) {
    return Optional.ofNullable(repairUnitsByKey.get(params));
  }
}