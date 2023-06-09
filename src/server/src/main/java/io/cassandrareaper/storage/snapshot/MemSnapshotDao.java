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

package io.cassandrareaper.storage.snapshot;

import io.cassandrareaper.core.Snapshot;

import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Maps;

public class MemSnapshotDao implements ISnapshot {
  public final ConcurrentMap<String, Snapshot> snapshots = Maps.newConcurrentMap();

  public MemSnapshotDao() {
  }

  @Override
  public boolean saveSnapshot(Snapshot snapshot) {
    snapshots.put(snapshot.getClusterName() + "-" + snapshot.getName(), snapshot);
    return true;
  }

  @Override
  public boolean deleteSnapshot(Snapshot snapshot) {
    snapshots.remove(snapshot.getClusterName() + "-" + snapshot.getName());
    return true;
  }

  @Override
  public Snapshot getSnapshot(String clusterName, String snapshotName) {
    Snapshot snapshot = snapshots.get(clusterName + "-" + snapshotName);
    return snapshot;
  }
}