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

import java.util.Optional;
import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.base.Preconditions;

public class MemoryRepairUnitDao implements IRepairUnitDao {
  private final MemoryStorageFacade storage;

  public MemoryRepairUnitDao(MemoryStorageFacade storage) {
    this.storage = storage;
  }

  /**
   * Delete a RepairUnit instance from Storage, but only if no run or schedule is referencing it.
   *
   * @param repairUnitId The RepairUnit instance id to delete.
   * @return The deleted RepairUnit instance, if delete succeeded.
   */

  @Override
  public RepairUnit addRepairUnit(RepairUnit.Builder repairUnitBuilder) {
    Optional<RepairUnit> existing = getRepairUnit(repairUnitBuilder);
    if (existing.isPresent() && repairUnitBuilder.incrementalRepair == existing.get().getIncrementalRepair()
        && repairUnitBuilder.subrangeIncrementalRepair == existing.get().getSubrangeIncrementalRepair()) {
      return existing.get();
    } else {
      RepairUnit newRepairUnit = repairUnitBuilder.build(UUIDs.timeBased());
      storage.addRepairUnit(Optional.ofNullable(repairUnitBuilder), newRepairUnit);
      return newRepairUnit;
    }
  }

  @Override
  public void updateRepairUnit(RepairUnit updatedRepairUnit) {
    storage.addRepairUnit(Optional.ofNullable(updatedRepairUnit.with()), updatedRepairUnit);
  }

  @Override
  public RepairUnit getRepairUnit(UUID id) {
    RepairUnit unit = storage.getRepairUnitById(id);
    Preconditions.checkArgument(null != unit);
    return unit;
  }

  @Override
  public Optional<RepairUnit> getRepairUnit(RepairUnit.Builder repairUnitBuilder) {
    return Optional.ofNullable(storage.getRepairUnitByKey(repairUnitBuilder));
  }
}