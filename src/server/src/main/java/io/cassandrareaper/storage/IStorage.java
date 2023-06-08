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

package io.cassandrareaper.storage;

import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.DiagEventSubscription;
import io.cassandrareaper.core.PercentRepairedMetric;
import io.cassandrareaper.core.Snapshot;
import io.cassandrareaper.resources.view.RepairRunStatus;
import io.cassandrareaper.resources.view.RepairScheduleStatus;
import io.cassandrareaper.storage.repairrun.IRepairRun;
import io.cassandrareaper.storage.repairschedule.IRepairSchedule;
import io.cassandrareaper.storage.repairsegment.IRepairSegment;
import io.cassandrareaper.storage.repairunit.IRepairUnit;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

import io.dropwizard.lifecycle.Managed;

/**
 * API definition for cassandra-reaper.
 */
public interface IStorage extends Managed,
      IRepairRun,
      IRepairSegment,
      IRepairUnit,
      IRepairSchedule {

  boolean isStorageConnected();

  Collection<Cluster> getClusters();

  boolean addCluster(Cluster cluster);

  boolean updateCluster(Cluster newCluster);

  Cluster getCluster(String clusterName);

  /**
   * Delete the Cluster instance identified by the given cluster name. Delete succeeds only if there are no repair runs
   * for the targeted cluster.
   *
   * @param clusterName The name of the Cluster instance to delete.
   * @return The deleted Cluster instance if delete succeeds, with state set to DELETED.
   */
  Cluster deleteCluster(String clusterName);

  Collection<RepairRunStatus> getClusterRunStatuses(String clusterName, int limit);

  Collection<RepairScheduleStatus> getClusterScheduleStatuses(String clusterName);

  boolean saveSnapshot(Snapshot snapshot);

  boolean deleteSnapshot(Snapshot snapshot);

  Snapshot getSnapshot(String clusterName, String snapshotName);

  Collection<DiagEventSubscription> getEventSubscriptions();

  Collection<DiagEventSubscription> getEventSubscriptions(String clusterName);

  DiagEventSubscription getEventSubscription(UUID id);

  DiagEventSubscription addEventSubscription(DiagEventSubscription subscription);

  boolean deleteEventSubscription(UUID id);

  List<PercentRepairedMetric> getPercentRepairedMetrics(
      String clusterName,
      UUID repairScheduleId,
      Long since);

  void storePercentRepairedMetric(PercentRepairedMetric metric);
}
