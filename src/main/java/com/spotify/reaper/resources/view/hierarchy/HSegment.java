package com.spotify.reaper.resources.view.hierarchy;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.resources.CommonTools;
import com.spotify.reaper.service.RingRange;

import org.joda.time.DateTime;

public class HSegment {

  public final long id;
  public final RingRange tokenRange;
  public final RepairSegment.State state;
  private final DateTime startTime;
  public String getStartTime() {
    return CommonTools.dateTimeToISO8601(startTime);
  }
  private final DateTime endTime;
  public String getEndTime() {
    return CommonTools.dateTimeToISO8601(endTime);
  }
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
