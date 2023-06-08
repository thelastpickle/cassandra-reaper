package io.cassandrareaper.storage.repairsegment;

import io.cassandrareaper.core.RepairSegment;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface IRepairSegment {
    boolean updateRepairSegment(RepairSegment newRepairSegment);

    Optional<RepairSegment> getRepairSegment(UUID runId, UUID segmentId);

    Collection<RepairSegment> getRepairSegmentsForRun(UUID runId);

    /**
     * @param runId the run id that the segment belongs to.
     * @return a segment enclosed by the range with state NOT_STARTED, or nothing.
     */
    List<RepairSegment> getNextFreeSegments(UUID runId);

    Collection<RepairSegment> getSegmentsWithState(UUID runId, RepairSegment.State segmentState);

    int getSegmentAmountForRepairRun(UUID runId);

    int getSegmentAmountForRepairRunWithState(UUID runId, RepairSegment.State state);
}
