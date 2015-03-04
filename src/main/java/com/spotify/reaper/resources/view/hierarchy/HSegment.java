package com.spotify.reaper.resources.view.hierarchy;

import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.service.RingRange;

import org.joda.time.DateTime;

public class HSegment {

  public final long id;
  public final RingRange tokenRange;
  public final RepairSegment.State state;
  public final DateTime startTime;
  public final DateTime endTime;
  public final String coordinatorHost;
  public final int failCount;
  public final Integer repairCommandId;
  public final long runId;
  public final long repairUnitId;

  public HSegment(RepairSegment segment) {
    id = segment.getId();
    tokenRange = segment.getTokenRange();
    state = segment.getState();
    startTime = segment.getStartTime();
    endTime = segment.getEndTime();
    coordinatorHost = segment.getCoordinatorHost();
    failCount = segment.getFailCount();
    repairCommandId = segment.getRepairCommandId();
    runId = segment.getRunId();
    repairUnitId = segment.getRepairUnitId();
  }
}
