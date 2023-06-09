package io.cassandrareaper.storage.snapshot;

import io.cassandrareaper.core.Snapshot;

import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Maps;

public class MemSnapshotDao implements ISnapshot{
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