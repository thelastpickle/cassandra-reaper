package com.spotify.reaper.resources.view.hierarchy;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.storage.IStorage;

import java.util.Collection;

import javax.annotation.Nullable;

public class HRun {

  public final long id;
  public final Collection<HSegment> segments;

  public HRun(RepairRun run, Collection<RepairSegment> segments) {
    id = run.getId();
    this.segments = Collections2.transform(segments, new Function<RepairSegment, HSegment>() {
      @Nullable
      @Override
      public HSegment apply(RepairSegment repairSegment) {
        return new HSegment(repairSegment);
      }
    });
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
