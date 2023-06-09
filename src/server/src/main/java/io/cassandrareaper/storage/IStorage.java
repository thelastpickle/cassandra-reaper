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

import io.cassandrareaper.core.PercentRepairedMetric;
import io.cassandrareaper.core.Snapshot;
import io.cassandrareaper.resources.view.RepairRunStatus;
import io.cassandrareaper.resources.view.RepairScheduleStatus;
import io.cassandrareaper.storage.cluster.ICluster;
import io.cassandrareaper.storage.events.IEvents;
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
      IRepairSchedule,
      ICluster, IEvents {

  boolean isStorageConnected();

  boolean saveSnapshot(Snapshot snapshot);

  boolean deleteSnapshot(Snapshot snapshot);

  Snapshot getSnapshot(String clusterName, String snapshotName);

  Collection<RepairRunStatus> getClusterRunStatuses(String clusterName, int limit);

  Collection<RepairScheduleStatus> getClusterScheduleStatuses(String clusterName);

  List<PercentRepairedMetric> getPercentRepairedMetrics(
        String clusterName,
        UUID repairScheduleId,
        Long since);

  void storePercentRepairedMetric(PercentRepairedMetric metric);

}
