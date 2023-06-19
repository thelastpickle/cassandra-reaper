/*
 * Copyright 2014-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
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

package io.cassandrareaper.storage.cluster;

import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.storage.events.MemEventsDao;
import io.cassandrareaper.storage.repairrun.MemRepairRunDao;
import io.cassandrareaper.storage.repairschedule.MemRepairScheduleDao;
import io.cassandrareaper.storage.repairunit.MemRepairUnitDao;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

public class MemClusterDao implements ICluster {
  public final ConcurrentMap<String, Cluster> clusters = Maps.newConcurrentMap();
  private final MemRepairUnitDao memRepairUnitDao;
  private final MemRepairRunDao memRepairRunDao;
  private final MemRepairScheduleDao memRepairScheduleDao;

  private final MemEventsDao memEventsDao;

  public MemClusterDao(MemRepairUnitDao memRepairUnitDao,
                       MemRepairRunDao memRepairRunDao,
                       MemRepairScheduleDao memRepairScheduleDao,
                       MemEventsDao memEventsDao) {
    this.memRepairUnitDao = memRepairUnitDao;
    this.memRepairRunDao = memRepairRunDao;
    this.memRepairScheduleDao = memRepairScheduleDao;
    this.memEventsDao = memEventsDao;
  }

  @Override
  public Collection<Cluster> getClusters() {
    return clusters.values();
  }

  @Override
  public boolean addCluster(Cluster cluster) {
    assert addClusterAssertions(cluster);
    Cluster existing = clusters.put(cluster.getName(), cluster);
    return existing == null;
  }

  @Override
  public boolean updateCluster(Cluster newCluster) {
    addCluster(newCluster);
    return true;
  }

  public boolean addClusterAssertions(Cluster cluster) {
    Preconditions.checkState(
          Cluster.State.UNKNOWN != cluster.getState(),
          "Cluster should not be persisted with UNKNOWN state");

    // TODO – unit tests need to also always set the paritioner
    //Preconditions.checkState(cluster.getPartitioner().isPresent(), "Cannot store cluster with no partitioner.");

    // assert we're not overwriting a cluster with the same name but different node list
    Set<String> previousNodes;
    try {
      previousNodes = getCluster(cluster.getName()).getSeedHosts();
    } catch (IllegalArgumentException ignore) {
      // there is no previous cluster with same name
      previousNodes = cluster.getSeedHosts();
    }
    Set<String> addedNodes = cluster.getSeedHosts();

    Preconditions.checkArgument(
          !Collections.disjoint(previousNodes, addedNodes),
          "Trying to add/update cluster using an existing name: %s. No nodes overlap between %s and %s",
          cluster.getName(), StringUtils.join(previousNodes, ','), StringUtils.join(addedNodes, ','));

    return true;
  }

  @Override
  public Cluster getCluster(String clusterName) {
    Preconditions.checkArgument(clusters.containsKey(clusterName), "no such cluster: %s", clusterName);
    return clusters.get(clusterName);
  }

  @Override
  public Cluster deleteCluster(String clusterName) {
    memRepairScheduleDao.getRepairSchedulesForCluster(clusterName).forEach(
        schedule -> memRepairScheduleDao.deleteRepairSchedule(schedule.getId())
    );
    memRepairRunDao.getRepairRunIdsForCluster(clusterName, Optional.empty())
          .forEach(runId -> memRepairRunDao.deleteRepairRun(runId));

    memEventsDao.getEventSubscriptions(clusterName)
          .stream()
          .filter(subscription -> subscription.getId().isPresent())
          .forEach(subscription -> memEventsDao.deleteEventSubscription(subscription.getId().get()));

    memRepairUnitDao.repairUnits.values().stream()
          .filter((unit) -> unit.getClusterName().equals(clusterName))
          .forEach((unit) -> {
            assert memRepairRunDao.getRepairRunsForUnit(
                  unit.getId()).isEmpty() : StringUtils.join(memRepairRunDao.getRepairRunsForUnit(unit.getId())
            );
            memRepairUnitDao.repairUnits.remove(unit.getId());
            memRepairUnitDao.repairUnitsByKey.remove(unit.with());
          });

    return clusters.remove(clusterName);
  }
}