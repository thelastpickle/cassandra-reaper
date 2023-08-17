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

package io.cassandrareaper.management;

import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Snapshot;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class SnapshotProxy {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotProxy.class);

  private final ICassandraManagementProxy proxy;

  private SnapshotProxy(ICassandraManagementProxy proxy) {
    this.proxy = proxy;
  }

  public static SnapshotProxy create(ICassandraManagementProxy proxy) {

    return new SnapshotProxy((ICassandraManagementProxy) proxy);
  }

  public void clearSnapshot(String repairId, String keyspaceName) {
    if (null == repairId || repairId.isEmpty()) {
      // Passing in null or empty string will clear all snapshots on the hos
      throw new IllegalArgumentException("repairId cannot be null or empty string");
    }
    try {
      proxy.clearSnapshot(repairId, keyspaceName);
    } catch (AssertionError | IOException e) {
      LOG.error("failed to clear snapshot " + repairId + " in keyspace " + keyspaceName, e);
    }
  }

  public void clearSnapshot(String snapshotName) {
    if (null == snapshotName || snapshotName.isEmpty()) {
      // Passing in null or empty string will clear all snapshots on the hos
      throw new IllegalArgumentException("snapshotName cannot be null or empty string");
    }
    try {
      proxy.clearSnapshot(snapshotName);
    } catch (AssertionError | IOException | RuntimeException e) {
      LOG.error("failed to clear snapshot " + snapshotName, e);
    }
  }

  public List<Snapshot> listSnapshots() throws UnsupportedOperationException {
    return proxy.listSnapshots();
  }

  public String takeSnapshot(String snapshotName, String... keyspaceNames) throws ReaperException {
    try {
      proxy.takeSnapshot(snapshotName, keyspaceNames);
      return snapshotName;
    } catch (IOException e) {
      throw new ReaperException(e);
    }
  }

  public void takeColumnFamilySnapshot(
      String keyspace,
      String table,
      String snapshotName) throws ReaperException {

    try {
      proxy.takeColumnFamilySnapshot(keyspace, table, snapshotName);
    } catch (IOException e) {
      throw new ReaperException(e);
    }
  }

}