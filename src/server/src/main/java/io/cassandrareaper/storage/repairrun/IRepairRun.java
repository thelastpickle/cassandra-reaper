package io.cassandrareaper.storage.repairrun;

import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSegment;

import java.util.Collection;
import java.util.Optional;
import java.util.SortedSet;
import java.util.UUID;

public interface IRepairRun {
    RepairRun addRepairRun(RepairRun.Builder repairRun, Collection<RepairSegment.Builder> newSegments);

    boolean updateRepairRun(RepairRun repairRun, Optional<Boolean> updateRepairState);

    boolean updateRepairRun(RepairRun repairRun);

    Optional<RepairRun> getRepairRun(UUID id);

    /**
     * return all the repair runs in a cluster, in reverse chronological order, with default limit is 1000
     */
    Collection<RepairRun> getRepairRunsForCluster(String clusterName, Optional<Integer> limit);

    Collection<RepairRun> getRepairRunsForClusterPrioritiseRunning(String clusterName, Optional<Integer> limit);

    Collection<RepairRun> getRepairRunsForUnit(UUID repairUnitId);

    Collection<RepairRun> getRepairRunsWithState(RepairRun.RunState runState);

    SortedSet<UUID> getRepairRunIdsForCluster(String clusterName, Optional<Integer> limit);

    /**
     * Delete the RepairRun instance identified by the given id, and delete also all the related repair segments.
     *
     * @param id The id of the RepairRun instance to delete, and all segments for it.
     * @return The deleted RepairRun instance, if delete succeeds, with state set to DELETED.
     */
    Optional<RepairRun> deleteRepairRun(UUID id);
}
