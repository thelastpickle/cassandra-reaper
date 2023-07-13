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
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.storage.IDistributedStorage;
import io.cassandrareaper.storage.repairrun.IRepairRun;

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

public final class PurgeService {

  private static final Logger LOG = LoggerFactory.getLogger(PurgeService.class);

  private final AppContext context;

  private final IRepairRun repairRunDao;

  private PurgeService(AppContext context, IRepairRun repairRunDao) {
    this.context = context;
    this.repairRunDao = repairRunDao;
  }

  public static PurgeService create(AppContext context, IRepairRun repairRunDao) {
    return new PurgeService(context, repairRunDao);
  }

  public Integer purgeDatabase() throws ReaperException {
    int purgedRuns = 0;
    if (context.config.getNumberOfRunsToKeepPerUnit() != 0
        || context.config.getPurgeRecordsAfterInDays() != 0) {
      // List clusters
      Collection<Cluster> clusters = context.storage.getClusters();

      // List repair runs
      for (Cluster cluster : clusters) {

        Collection<RepairRun> repairRuns
            = repairRunDao.getRepairRunsForCluster(cluster.getName(), Optional.empty());

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
    purgeMetrics();
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
        repairRunDao.deleteRepairRun(repairRunsForUnit.get(i).getId());
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
              repairRunDao.deleteRepairRun(run.getId());
              purgedRuns.incrementAndGet();
            });

    return purgedRuns.get();
  }

  /**
   * Purges all expired metrics from storage. Expiration time is a property of the storage, stored either in
   * the schema itself for databases with TTL or in the storage instance for databases which must be purged manually
   */
  private void purgeMetrics() {
    if (context.storage instanceof IDistributedStorage) {
      IDistributedStorage storage = ((IDistributedStorage) context.storage);
      if (context.config.isInSidecarMode()) {
        storage.purgeMetrics();
        storage.purgeNodeOperations();
      }
    }
  }
}