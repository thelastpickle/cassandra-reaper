package io.cassandrareaper.storage.repairrun;

import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.service.RepairRunService;
import io.cassandrareaper.storage.repairsegment.MemRepairSegment;
import io.cassandrareaper.storage.repairunit.MemRepairUnitDao;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.joda.time.DateTime;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class MemRepairRunDao implements IRepairRun {
    public final ConcurrentMap<UUID, RepairRun> repairRuns = Maps.newConcurrentMap();
    private final MemRepairSegment memRepairSegment;
    private final MemRepairUnitDao memRepairUnitDao;


    public MemRepairRunDao(MemRepairSegment memRepairSegment, MemRepairUnitDao memRepairUnitDao) {
        this.memRepairSegment = memRepairSegment;
        this.memRepairUnitDao = memRepairUnitDao;
    }

    
    public RepairRun addRepairRun(RepairRun.Builder repairRun, Collection<RepairSegment.Builder> newSegments) {
        RepairRun newRepairRun = repairRun.build(UUIDs.timeBased());
        repairRuns.put(newRepairRun.getId(), newRepairRun);
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
            repairRuns.put(repairRun.getId(), repairRun);
            return true;
        }
    }

    
    public Optional<RepairRun> getRepairRun(UUID id) {
        return Optional.ofNullable(repairRuns.get(id));
    }

    
    public List<RepairRun> getRepairRunsForCluster(String clusterName, Optional<Integer> limit) {
        List<RepairRun> foundRepairRuns = new ArrayList<RepairRun>();
        TreeMap<UUID, RepairRun> reverseOrder = new TreeMap<UUID, RepairRun>(Collections.reverseOrder());
        reverseOrder.putAll(repairRuns);
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
        List<RepairRun> foundRepairRuns = repairRuns
                .values()
                .stream()
                .filter(
                        row -> row.getClusterName().equals(clusterName.toLowerCase(Locale.ROOT))).collect(Collectors.toList()
                );
        RepairRunService.sortByRunState(foundRepairRuns);
        return foundRepairRuns.subList(0, Math.min(foundRepairRuns.size(), limit.orElse(1000)));
    }

    
    public Collection<RepairRun> getRepairRunsForUnit(UUID repairUnitId) {
        List<RepairRun> foundRepairRuns = new ArrayList<RepairRun>();
        for (RepairRun repairRun : repairRuns.values()) {
            if (repairRun.getRepairUnitId().equals(repairUnitId)) {
                foundRepairRuns.add(repairRun);
            }
        }
        return foundRepairRuns;
    }

    
    public Collection<RepairRun> getRepairRunsWithState(RepairRun.RunState runState) {
        List<RepairRun> foundRepairRuns = new ArrayList<RepairRun>();
        for (RepairRun repairRun : repairRuns.values()) {
            if (repairRun.getRunState() == runState) {
                foundRepairRuns.add(repairRun);
            }
        }
        return foundRepairRuns;
    }

    
    public Optional<RepairRun> deleteRepairRun(UUID id) {
        RepairRun deletedRun = repairRuns.remove(id);
        if (deletedRun != null) {
            if (memRepairSegment.getSegmentAmountForRepairRunWithState(id, RepairSegment.State.RUNNING) == 0) {
                memRepairUnitDao.deleteRepairUnit(deletedRun.getRepairUnitId());
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
        for (RepairRun repairRun : repairRuns.values()) {
            if (repairRun.getClusterName().equalsIgnoreCase(clusterName)) {
                repairRunIds.add(repairRun.getId());
            }
        }
        return repairRunIds;
    }
}