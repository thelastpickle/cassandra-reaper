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

import io.cassandrareaper.storage.cluster.IClusterDao;
import io.cassandrareaper.storage.events.IEventsDao;
import io.cassandrareaper.storage.metrics.IMetricsDao;
import io.cassandrareaper.storage.repairrun.IRepairRunDao;
import io.cassandrareaper.storage.repairschedule.IRepairScheduleDao;
import io.cassandrareaper.storage.repairsegment.IRepairSegmentDao;
import io.cassandrareaper.storage.repairunit.IRepairUnitDao;
import io.cassandrareaper.storage.snapshot.ISnapshotDao;

import java.util.Set;
import java.util.UUID;

import io.dropwizard.lifecycle.Managed;

/**
 * API definition for cassandra-reaper.
 */
public interface IStorageDao extends Managed,
    IMetricsDao {

  boolean lockRunningRepairsForNodes(
      UUID repairId,
      UUID segmentId,
      Set<String> replicas);

  boolean renewRunningRepairsForNodes(
      UUID repairId,
      UUID segmentId,
      Set<String> replicas);

  boolean releaseRunningRepairsForNodes(
      UUID repairId,
      UUID segmentId,
      Set<String> replicas);

  Set<UUID> getLockedSegmentsForRun(UUID runId);

  boolean isStorageConnected();

  IEventsDao getEventsDao();

  ISnapshotDao getSnapshotDao();

  IRepairRunDao getRepairRunDao();

  IRepairSegmentDao getRepairSegmentDao();

  IRepairUnitDao getRepairUnitDao();

  IRepairScheduleDao getRepairScheduleDao();

  IClusterDao getClusterDao();

}