package io.cassandrareaper.storage.repairschedule;

import io.cassandrareaper.core.RepairSchedule;

import java.util.Collection;
import java.util.Optional;
import java.util.UUID;

public interface IRepairSchedule {
    RepairSchedule addRepairSchedule(RepairSchedule.Builder repairSchedule);

    Optional<RepairSchedule> getRepairSchedule(UUID repairScheduleId);

    Collection<RepairSchedule> getRepairSchedulesForCluster(String clusterName);

    Collection<RepairSchedule> getRepairSchedulesForCluster(String clusterName, boolean incremental);

    Collection<RepairSchedule> getRepairSchedulesForKeyspace(String keyspaceName);

    Collection<RepairSchedule> getRepairSchedulesForClusterAndKeyspace(String clusterName, String keyspaceName);

    Collection<RepairSchedule> getAllRepairSchedules();

    boolean updateRepairSchedule(RepairSchedule newRepairSchedule);

    /**
     * Delete the RepairSchedule instance identified by the given id. Related repair runs or other resources tied to the
     * schedule will not be deleted.
     *
     * @param id The id of the RepairSchedule instance to delete.
     * @return The deleted RepairSchedule instance, if delete succeeds, with state set to DELETED.
     */
    Optional<RepairSchedule> deleteRepairSchedule(UUID id);
}
