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

package io.cassandrareaper.management.jmx;

import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Snapshot;
import io.cassandrareaper.management.ICassandraManagementProxy;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.management.openmbean.TabularData;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class SnapshotProxy {

  private static final long KB_FACTOR = 1000;
  private static final long KIB_FACTOR = 1024;
  private static final long MB_FACTOR = 1000 * KB_FACTOR;
  private static final long GB_FACTOR = 1000 * MB_FACTOR;
  private static final long MIB_FACTOR = 1024 * KIB_FACTOR;
  private static final long GIB_FACTOR = 1024 * MIB_FACTOR;

  private static final Logger LOG = LoggerFactory.getLogger(SnapshotProxy.class);

  private final JmxCassandraManagementProxy proxy;

  private SnapshotProxy(JmxCassandraManagementProxy proxy) {
    this.proxy = proxy;
  }

  public static SnapshotProxy create(ICassandraManagementProxy proxy) {
    Preconditions.checkArgument(proxy instanceof JmxCassandraManagementProxy, "only JmxProxyImpl is supported");
    return new SnapshotProxy((JmxCassandraManagementProxy) proxy);
  }

  public static double parseHumanReadableSize(String readableSize) {
    int spaceNdx = readableSize.indexOf(" ");

    double ret = readableSize.contains(".")
        ? Double.parseDouble(readableSize.substring(0, spaceNdx))
        : Double.parseDouble(readableSize.substring(0, spaceNdx).replace(",", "."));

    switch (readableSize.substring(spaceNdx + 1)) {
      case "GB":
        return ret * GB_FACTOR;
      case "GiB":
        return ret * GIB_FACTOR;
      case "MB":
        return ret * MB_FACTOR;
      case "MiB":
        return ret * MIB_FACTOR;
      case "KB":
        return ret * KB_FACTOR;
      case "KiB":
        return ret * KIB_FACTOR;
      default:
        return 0;
    }
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
    List<Snapshot> snapshots = Lists.newArrayList();

    String cassandraVersion = proxy.getCassandraVersion();
    if (JmxCassandraManagementProxy.versionCompare(cassandraVersion, "2.1.0") < 0) {
      // 2.0 and prior do not allow to list snapshots
      throw new UnsupportedOperationException(
          "Snapshot listing is not supported in Cassandra 2.0 and prior.");
    }

    Map<String, TabularData> snapshotDetails = Collections.emptyMap();
    try {
      snapshotDetails = proxy.getSnapshotDetails();
    } catch (RuntimeException ex) {
      LOG.warn("failed getting snapshots details from " + proxy.getClusterName(), ex);
    }

    if (snapshotDetails.isEmpty()) {
      LOG.debug("There are no snapshots on host {}", proxy.getHost());
      return snapshots;
    }
    // display column names only once
    final List<String> indexNames
        = snapshotDetails.entrySet().iterator().next().getValue().getTabularType().getIndexNames();

    for (final Map.Entry<String, TabularData> snapshotDetail : snapshotDetails.entrySet()) {
      Set<?> values = snapshotDetail.getValue().keySet();
      for (Object eachValue : values) {
        int index = 0;
        Snapshot.Builder snapshotBuilder = Snapshot.builder().withHost(proxy.getHost());
        final List<?> valueList = (List<?>) eachValue;
        for (Object value : valueList) {
          switch (indexNames.get(index)) {
            case "Snapshot name":
              snapshotBuilder.withName((String) value);
              break;
            case "Keyspace name":
              snapshotBuilder.withKeyspace((String) value);
              break;
            case "Column family name":
              snapshotBuilder.withTable((String) value);
              break;
            case "True size":
              snapshotBuilder.withTrueSize(parseHumanReadableSize((String) value));
              break;
            case "Size on disk":
              snapshotBuilder.withSizeOnDisk(parseHumanReadableSize((String) value));
              break;
            default:
              break;
          }
          index++;
        }
        snapshots.add(snapshotBuilder.withClusterName(proxy.getClusterName()).build());
      }
    }
    return snapshots;
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