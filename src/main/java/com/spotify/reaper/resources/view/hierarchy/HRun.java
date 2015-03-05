package com.spotify.reaper.resources.view.hierarchy;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.resources.CommonTools;
import com.spotify.reaper.storage.IStorage;

import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;

import java.util.Collection;

import javax.annotation.Nullable;

public class HRun {

  public final long id;
  public final String clusterName;
  public final String owner;
  public final String cause;
  private final DateTime creationTime;
  public String getCreationTime() {
    return CommonTools.dateTimeToISO8601(creationTime);
  }
  private final DateTime startTime;
  public String getStartTime() {
    return CommonTools.dateTimeToISO8601(startTime);
  }
  private final DateTime endTime;
  public String getEndTime() {
    return CommonTools.dateTimeToISO8601(endTime);
  }
  private final DateTime pauseTime;
  public String getPauseTime() {
    return CommonTools.dateTimeToISO8601(pauseTime);
  }
  public final RepairRun.RunState runState;
  public final int segmentCount;
  public final RepairParallelism repairParallelism;
  public final double intensity;
  public final String lastEvent;

  public final Collection<HSegment> segments;

  // Extra
  public final int segmentsRepaired;

  public HRun(RepairRun run, Collection<RepairSegment> segments) {
    id = run.getId();
    clusterName = run.getClusterName();
    owner = run.getOwner();
    cause = run.getCause();
    creationTime = run.getCreationTime();
    startTime = run.getStartTime();
    endTime = run.getEndTime();
    pauseTime = run.getPauseTime();
    runState = run.getRunState();
    segmentCount = run.getSegmentCount();
    repairParallelism = run.getRepairParallelism();
    intensity = run.getIntensity();
    lastEvent = run.getLastEvent();
    this.segments = Collections2.transform(segments, new Function<RepairSegment, HSegment>() {
      @Nullable
      @Override
      public HSegment apply(RepairSegment repairSegment) {
        return new HSegment(repairSegment);
      }
    });
    segmentsRepaired = Collections2.filter(segments, new Predicate<RepairSegment>() {
      @Override
      public boolean apply(RepairSegment segment) {
        return segment.getState() == RepairSegment.State.DONE;
      }
    }).size();
  }

  public HRun(IStorage storage, long runId) {
    this(storage.getRepairRun(runId).get(), getSegmentsForRun(storage, runId));
  }

  // Bj0rn: This is a workaround. IStorage should have this function!
  private static Collection<RepairSegment> getSegmentsForRun(IStorage storage, long runId) {
    return Lists.newArrayList(Iterables.unmodifiableIterable(Iterables.concat(
        storage.getSegmentsWithState(runId, RepairSegment.State.NOT_STARTED),
        storage.getSegmentsWithState(runId, RepairSegment.State.RUNNING),
        storage.getSegmentsWithState(runId, RepairSegment.State.DONE))));
  }
}
