package com.spotify.reaper.resources.view.hierarchy;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

import com.spotify.reaper.core.RepairSchedule;
import com.spotify.reaper.storage.IStorage;

import java.util.Collection;

import javax.annotation.Nullable;

public class HSchedule {

  public final long id;
  public final Collection<HRun> runs;

  public HSchedule(RepairSchedule schedule, Collection<HRun> runs) {
    id = schedule.getId();
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
