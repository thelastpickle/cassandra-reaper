package io.cassandrareaper.storage.snapshot;

import io.cassandrareaper.core.Snapshot;

public interface ISnapshot {
  boolean saveSnapshot(Snapshot snapshot);

  boolean deleteSnapshot(Snapshot snapshot);

  Snapshot getSnapshot(String clusterName, String snapshotName);
}
