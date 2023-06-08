package io.cassandrareaper.storage.repairunit;

import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairUnit;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

public class MemRepairUnitDao implements IRepairUnit {
    public final ConcurrentMap<UUID, RepairUnit> repairUnits = Maps.newConcurrentMap();
    public final ConcurrentMap<RepairUnit.Builder, RepairUnit> repairUnitsByKey = Maps.newConcurrentMap();

    public MemRepairUnitDao() {}

    /**
     * Delete a RepairUnit instance from Storage, but only if no run or schedule is referencing it.
     *
     * @param repairUnitId The RepairUnit instance id to delete.
     * @return The deleted RepairUnit instance, if delete succeeded.
     */
    public Optional<RepairUnit> deleteRepairUnit(UUID repairUnitId) {
        RepairUnit deletedUnit = null;
        boolean canDelete = true;
        for (RepairRun repairRun : this.memRepairRunDao.repairRuns.values()) {
            if (repairRun.getRepairUnitId().equals(repairUnitId)) {
                canDelete = false;
                break;
            }
        }
        if (canDelete) {
            for (RepairSchedule schedule : this.memRepairSchedules.getRepairSchedules().values()) {
                if (schedule.getRepairUnitId().equals(repairUnitId)) {
                    canDelete = false;
                    break;
                }
            }
        }
        if (canDelete) {
            deletedUnit = repairUnits.remove(repairUnitId);
            repairUnitsByKey.remove(deletedUnit.with());
        }
        return Optional.ofNullable(deletedUnit);
    }

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