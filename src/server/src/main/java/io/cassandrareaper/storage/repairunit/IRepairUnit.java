package io.cassandrareaper.storage.repairunit;

import io.cassandrareaper.core.RepairUnit;

import java.util.Optional;
import java.util.UUID;

public interface IRepairUnit {
    RepairUnit addRepairUnit(RepairUnit.Builder newRepairUnit);

    RepairUnit getRepairUnit(UUID id);

    void updateRepairUnit(RepairUnit updatedRepairUnit);

    Optional<RepairUnit> getRepairUnit(RepairUnit.Builder repairUnit);
}
