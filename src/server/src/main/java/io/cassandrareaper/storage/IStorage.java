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

import io.cassandrareaper.storage.cluster.ICluster;
import io.cassandrareaper.storage.events.IEvents;
import io.cassandrareaper.storage.metrics.IMetrics;
import io.cassandrareaper.storage.repairrun.IRepairRun;
import io.cassandrareaper.storage.repairschedule.IRepairSchedule;
import io.cassandrareaper.storage.repairsegment.IRepairSegment;
import io.cassandrareaper.storage.repairunit.IRepairUnit;
import io.cassandrareaper.storage.snapshot.ISnapshot;

import io.dropwizard.lifecycle.Managed;

/**
 * API definition for cassandra-reaper.
 */
public interface IStorage extends Managed,
    IRepairUnit,
    IRepairSchedule,
    ICluster,
    IMetrics {

  boolean isStorageConnected();

  IEvents getEventsDao();

  ISnapshot getSnapshotDao();

  IRepairRun getRepairRunDao();

  IRepairSegment getRepairSegmentDao();

}