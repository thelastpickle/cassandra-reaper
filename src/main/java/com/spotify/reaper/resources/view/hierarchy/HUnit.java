package com.spotify.reaper.resources.view.hierarchy;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;

import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSchedule;
import com.spotify.reaper.core.RepairUnit;
import com.spotify.reaper.storage.IStorage;

import java.util.Collection;
import java.util.Set;

import javax.annotation.Nullable;

public class HUnit {

  public final long id;
  public final String clusterName;
  public final String keyspaceName;
  public final Set<String> columnFamilies;

  // Bj0rn: Couldn't figure out how to use GuavaOptionalSerializer, so the conversion to Set is
  // just a temporary fix.
  private final Optional<HSchedule> schedule;
  public Set<HSchedule> getSchedule() {
    return schedule.asSet();
  }

  public final Collection<HRun> unscheduled;

  public HUnit(RepairUnit unit, Optional<HSchedule> schedule, Collection<HRun> unscheduled) {
    id = unit.getId();
    clusterName = unit.getClusterName();
    keyspaceName = unit.getKeyspaceName();
    columnFamilies = unit.getColumnFamilies();
    this.schedule = schedule;
    this.unscheduled = unscheduled;
  }

  public HUnit(IStorage storage, long unitId) {
    this(storage.getRepairUnit(unitId).get(), getScheduleForUnit(storage, unitId), getUnscheduledRunsForUnit(
        storage, unitId));
  }

  // Bj0rn: This is a workaround. IStorage should have this function!
  private static Optional<HSchedule> getScheduleForUnit(IStorage storage, final long unitId) {
    Collection<RepairSchedule> cluserSchedules =
        storage.getRepairSchedulesForCluster(storage.getRepairUnit(unitId).get().getClusterName());
    Collection<RepairSchedule> unitSchedules =
        Collections2.filter(cluserSchedules, new Predicate<RepairSchedule>() {
          @Override
          public boolean apply(RepairSchedule schedule) {
            return schedule.getRepairUnitId() == unitId;
          }
        });
    if (unitSchedules.isEmpty()) {
      return Optional.absent();
    } else if (unitSchedules.size() == 1) {
      return Optional.of(new HSchedule(storage, unitSchedules.iterator().next().getId()));
    } else {
      throw new RuntimeException("A singe repair unit should not have multiple schedules!");
    }
  }

  // Bj0rn: This is a workaround. IStorage should have this function!
  private static Collection<HRun> getUnscheduledRunsForUnit(final IStorage storage, long unitId) {
    Collection<HRun> allUnitRuns = Collections2.transform(storage.getRepairRunsForUnit(unitId),
        new Function<RepairRun, HRun>() {
          @Nullable
          @Override
          public HRun apply(RepairRun run) {
            return new HRun(storage, run.getId());
          }
        });

    Optional<HSchedule> schedule = getScheduleForUnit(storage, unitId);
    if (schedule.isPresent()) {
      final Collection<Long> scheduledRuns = Collections2.transform(schedule.get().runs,
          new Function<HRun, Long>() {
            @Nullable
            @Override
            public Long apply(HRun run) {
              return run.id;
            }
          });
      return Collections2.filter(allUnitRuns, new Predicate<HRun>() {
        @Override
        public boolean apply(HRun run) {
          return !scheduledRuns.contains(run.id);
        }
      });
    } else {
      return allUnitRuns;
    }
  }
}
