/*
 * Copyright 2018-2018 The Last Pickle Ltd
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.cassandrareaper.service;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.RepairRun;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PurgeManager {

  private static final Logger LOG = LoggerFactory.getLogger(PurgeManager.class);

  private final AppContext context;

  private PurgeManager(AppContext context) {
    this.context = context;
  }

  public static PurgeManager create(AppContext context) {
    return new PurgeManager(context);
  }

  public Integer purgeDatabase() {
    int purgedRuns = 0;
    if (context.config.getNumberOfRunsToKeepPerUnit() != 0
        || context.config.getPurgeRecordsAfterInDays() != 0) {
      // List clusters
      Collection<Cluster> clusters = context.storage.getClusters();

      // List repair runs
      for (Cluster cluster : clusters) {

        Collection<RepairRun> repairRuns
            = context.storage.getRepairRunsForCluster(cluster.getName(), Optional.empty());

        if (context.config.getPurgeRecordsAfterInDays() > 0) {
          // Purge all runs that are older than threshold
          purgedRuns += purgeRepairRunsByDate(repairRuns);
        }

        if (context.config.getNumberOfRunsToKeepPerUnit() > 0) {
          // Purge units that have more runs than the threshold
          purgedRuns += purgeRepairRunsByHistoryDepth(repairRuns);
        }
      }
    }
    return purgedRuns;
  }

  /**
   * Purges all the repair runs that exceed the required number to keep per repair unit. Runs
   * provided as argument will be grouped by repair unit and the purge will be applied by unit.
   *
   * @param repairRuns the existing repair runs
   * @return the number of purged runs
   */
  private int purgeRepairRunsByHistoryDepth(Collection<RepairRun> repairRuns) {
    int purgedRuns = 0;
    Map<UUID, List<RepairRun>> repairRunsByRepairUnit = repairRuns
            .stream()
            .filter(run -> run.getRunState().isTerminated()) // only delete terminated runs
            .collect(Collectors.groupingBy(RepairRun::getRepairUnitId));
    for (Entry<UUID, List<RepairRun>> repairUnit : repairRunsByRepairUnit.entrySet()) {
      List<RepairRun> repairRunsForUnit = repairUnit.getValue();
      repairRunsForUnit.sort(
          (RepairRun r1, RepairRun r2) -> r2.getEndTime().compareTo(r1.getEndTime()));
      for (int i = context.config.getNumberOfRunsToKeepPerUnit();
          i < repairRunsForUnit.size();
          i++) {
        context.storage.deleteRepairRun(repairRunsForUnit.get(i).getId());
        purgedRuns++;
      }
    }

    return purgedRuns;
  }

  /**
   * Purges all repair runs that are older than the required history depth in days.
   *
   * @param repairRuns the list of existing repair runs
   * @return the number of purged runs
   */
  private int purgeRepairRunsByDate(Collection<RepairRun> repairRuns) {
    AtomicInteger purgedRuns = new AtomicInteger(0);
    repairRuns
        .stream()
        .filter(run -> run.getRunState().isTerminated()) // only delete terminated runs
        .filter(
            run ->
                run.getEndTime()
                    .isBefore(
                        DateTime.now()
                            .minusDays(
                                context.config.getPurgeRecordsAfterInDays()))) // filter by date
        .forEach(
            run -> {
              context.storage.deleteRepairRun(run.getId());
              purgedRuns.incrementAndGet();
            });

    return purgedRuns.get();
  }
}
