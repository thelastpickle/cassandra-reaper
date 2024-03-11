/*
 * Copyright 2024-2024 DataStax, Inc.
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

package io.cassandrareaper.storage.memory;

import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.DiagEventSubscription;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairUnit;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Maps;


public final class MemoryStorageRoot {
  public ConcurrentMap<UUID, LinkedHashMap<UUID, RepairSegment>> repairSegmentsByRunId = Maps.newConcurrentMap();
  public ConcurrentMap<UUID, RepairSegment> repairSegments = Maps.newConcurrentMap();
  public ConcurrentMap<UUID, RepairUnit> repairUnits = Maps.newConcurrentMap();
  public ConcurrentMap<RepairUnit.Builder, RepairUnit> repairUnitsByKey = Maps.newConcurrentMap();
  public ConcurrentMap<UUID, RepairRun> repairRuns = Maps.newConcurrentMap();
  public ConcurrentMap<UUID, RepairSchedule> repairSchedules = Maps.newConcurrentMap();
  public ConcurrentMap<UUID, DiagEventSubscription> subscriptionsById = Maps.newConcurrentMap();
  private Map<String, Cluster> clusters = Maps.newHashMap();

  public MemoryStorageRoot() {
    super();
  }

  public Map<String, Cluster> getClusters() {
    return clusters;
  }

  public Cluster addCluster(Cluster cluster) {
    Cluster result = clusters.put(cluster.getName(), cluster);
    return result;
  }

  public Cluster removeCluster(String clusterName) {
    Cluster removed = clusters.remove(clusterName);
    return removed;
  }
}
