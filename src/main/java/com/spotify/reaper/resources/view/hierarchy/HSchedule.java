package com.spotify.reaper.resources.view.hierarchy;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.spotify.reaper.core.RepairSchedule;
import com.spotify.reaper.resources.CommonTools;
import com.spotify.reaper.storage.IStorage;

import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;

import java.util.Collection;

import javax.annotation.Nullable;

public class HSchedule {

  public final long id;
  public final long repairUnitId;
  public final RepairSchedule.State state;
  public final int daysBetween;
  private final DateTime nextActivation;
  public String getNextActivation() {
    return CommonTools.dateTimeToISO8601(nextActivation);
  }
  private final DateTime followingActivation;
  public String getFollowingActivation() {
    return CommonTools.dateTimeToISO8601(followingActivation);
  }
  private final DateTime creationTime;
  public String getCreationTime() {
    return CommonTools.dateTimeToISO8601(creationTime);
  }
  private final DateTime pauseTime;
  public String getPauseTime() {
    return CommonTools.dateTimeToISO8601(pauseTime);
  }
  public final int segmentCount;
  public final RepairParallelism repairParallelism;
  public final double intensity;
  public final String owner;

  public final Collection<HRun> runs;

  public HSchedule(RepairSchedule schedule, Collection<HRun> runs) {
    id = schedule.getId();
    repairUnitId = schedule.getRepairUnitId();
    state = schedule.getState();
    nextActivation = schedule.getNextActivation();
    followingActivation = schedule.getFollowingActivation();
    daysBetween = schedule.getDaysBetween();
    creationTime = schedule.getCreationTime();
    pauseTime = schedule.getPauseTime();
    segmentCount = schedule.getSegmentCount();
    repairParallelism = schedule.getRepairParallelism();
    intensity = schedule.getIntensity();
    owner = schedule.getOwner();
    // Skipping: schedule.getRunHistory()
    this.runs = runs;
  }

  public HSchedule(IStorage storage, long scheduleId) {
    this(storage.getRepairSchedule(scheduleId).get(), getRunsForSchedule(storage, scheduleId));
  }

  // Bj0rn: This is a workaround. IStorage should have this function!
  private static Collection<HRun> getRunsForSchedule(final IStorage storage, long scheduleId) {
    return Collections2.transform(
        storage.getRepairSchedule(scheduleId).get().getRunHistory(),
        new Function<Long, HRun>() {
          @Nullable
          @Override
          public HRun apply(Long runId) {
            return new HRun(storage, runId);
          }
        });
  }
}
