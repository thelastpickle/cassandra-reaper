/*
 * Copyright 2020-2020 The Last Pickle Ltd
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

package io.cassandrareaper.management.http;

import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Snapshot;
import io.cassandrareaper.core.Table;
import io.cassandrareaper.management.ICassandraManagementProxy;
import io.cassandrareaper.management.RepairStatusHandler;
import io.cassandrareaper.service.RingRange;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.management.JMException;
import javax.management.openmbean.CompositeData;
import javax.validation.constraints.NotNull;

import com.codahale.metrics.MetricRegistry;
import com.datastax.mgmtapi.client.api.DefaultApi;
import com.datastax.mgmtapi.client.invoker.ApiClient;
import com.datastax.mgmtapi.client.invoker.ApiException;
import com.datastax.mgmtapi.client.model.EndpointStates;
import com.datastax.mgmtapi.client.model.SnapshotDetails;
import com.datastax.mgmtapi.client.model.TakeSnapshotRequest;
import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.repair.RepairParallelism;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpCassandraManagementProxy implements ICassandraManagementProxy {
  private static final Logger LOG = LoggerFactory.getLogger(HttpCassandraManagementProxy.class);
  final String host;
  final MetricRegistry metricRegistry;
  final String rootPath;
  final InetSocketAddress endpoint;
  final DefaultApi apiClient;

  public HttpCassandraManagementProxy(MetricRegistry metricRegistry,
                                      String rootPath,
                                      InetSocketAddress endpoint
  ) {
    this.host = endpoint.getHostString();
    this.metricRegistry = metricRegistry;
    this.rootPath = rootPath;
    this.endpoint = endpoint;
    this.apiClient = new DefaultApi(
        new ApiClient().setBasePath("http://" + endpoint.getHostName() + ":" + endpoint.getPort() + rootPath));
  }

  @Override
  public String getHost() {
    return host;
  }

  @Override
  public List<BigInteger> getTokens() {
    return null; // TODO: implement me.
  }

  @Override
  public Map<List<String>, List<String>> getRangeToEndpointMap(String keyspace) throws ReaperException {
    return null; // TODO: implement me.
  }

  @NotNull
  @Override
  public String getLocalEndpoint() throws ReaperException {
    return null; // TODO: implement me.
  }

  @NotNull
  @Override
  public Map<String, String> getEndpointToHostId() {
    return null; // TODO: implement me.
  }

  @Override
  public String getPartitioner() {
    return null; // TODO: implement me.
  }

  @Override
  public String getClusterName() {
    // Cluster name is part of Endpoint States.
    try {
      EndpointStates endpoints = apiClient.getEndpointStates();
      if (endpoints == null) {
        LOG.error("No endpoint state data retrieved.");
        return null;
      }
      List<Map<String, String>> states = endpoints.getEntity();
      if (states == null || states.isEmpty()) {
        LOG.error("Endpoint state data was null or empty:\n" + endpoints.toJson());
        return null;
      }
      // cluster name should be the same for all states
      String clusterName = states.get(0).get("CLUSTER_NAME");
      if (clusterName == null) {
        LOG.error("Endpoint state data did not contain a CLuster Name:\n" + endpoints.toString());
        // no need to return null here, we'll do it on the next line
      }
      return clusterName;
    } catch (ApiException ae) {
      LOG.error("Failed to retrieve Cluster Name from Endpoint states data", ae);
    }
    return null;
  }

  @Override
  public List<String> getKeyspaces() {
    return null; // TODO: implement me.
  }

  @Override
  public Set<Table> getTablesForKeyspace(String keyspace) throws ReaperException {
    return null; // TODO: implement me.
  }

  @Override
  public int getPendingCompactions() throws JMException {
    return 1; // TODO: implement me.
  }

  @Override
  public boolean isRepairRunning() throws JMException {
    return true; // TODO: implement me.
  }


  @Override
  public List<String> getRunningRepairMetricsPost22() {
    return null; // TODO: implement me.
  }

  @Override
  public void cancelAllRepairs() {
    // TODO: implement me.
  }

  @Override
  public Map<String, List<String>> listTablesByKeyspace() {
    return null; // TODO: implement me.
  }

  @Override
  public String getCassandraVersion() {
    try {
      return apiClient.getReleaseVersion();
    } catch (ApiException ae) {
      LOG.error("Failed to get Cassandra version", ae);
    }
    // should not get here
    return "UNKNOWN";
  }

  @Override
  public int triggerRepair(
      BigInteger beginToken,
      BigInteger endToken,
      String keyspace,
      RepairParallelism repairParallelism,
      Collection<String> columnFamilies,
      boolean fullRepair,
      Collection<String> datacenters,
      RepairStatusHandler repairStatusHandler,
      List<RingRange> associatedTokens,
      int repairThreadCount)
      throws ReaperException {
    return 1; //TODO: implement me

  }

  @Override
  public void removeRepairStatusHandler(int repairNo) {
    // TODO: implement me.
  }

  @Override
  public void close() {
    // TODO: implement me.
  }

  @Override
  public List<String> getLiveNodes() throws ReaperException {
    try {
      List<String> liveNodes = new ArrayList<>();
      EndpointStates endpoints = apiClient.getEndpointStates();
      for (Map<String, String> states : endpoints.getEntity()) {
        if (states.containsKey("IS_ALIVE") && Boolean.parseBoolean(states.get("IS_ALIVE"))) {
          liveNodes.add(states.get("ENDPOINT_IP"));
        }
      }
      return liveNodes;
    } catch (ApiException ae) {
      LOG.error("Failed to fetch live nodes", ae);
      throw new ReaperException(ae);
    }
  }

  @Override
  public void clearSnapshot(String snapshotName, String... keyspaces) throws IOException {
    try {
      apiClient.clearSnapshots(Arrays.asList(snapshotName), Arrays.asList(keyspaces));
    } catch (ApiException ae) {
      LOG.error("Failed to clear snapshots", ae);
      throw new IOException(ae);
    }
  }

  @Override
  public List<Snapshot> listSnapshots() throws UnsupportedOperationException {
    try {
      SnapshotDetails snapshotDetails = apiClient.getSnapshotDetails(null, null);
      return convertSnapshots(snapshotDetails);
    } catch (ApiException ae) {
      LOG.error("Failed to retrieve Snapshot details", ae);
      throw new UnsupportedOperationException(ae);
    }
  }

  @VisibleForTesting
  List<Snapshot> convertSnapshots(SnapshotDetails snapshotDetails) {
    assert snapshotDetails != null;
    List<Map<String, String>> details = snapshotDetails.getEntity();
    assert details != null;
    List<Snapshot> snapshots = new ArrayList<>(details.size());
    for (Map<String, String> detail : details) {
      Snapshot.Builder snapshotBuilder = Snapshot.builder().withHost(getHost());
      for (Map.Entry<String, String> detailEntry : detail.entrySet()) {
        String key = detailEntry.getKey();
        String value = detailEntry.getValue();
        switch (key) {
          case "Snapshot name":
            snapshotBuilder.withName(value);
            break;
          case "Keyspace name":
            snapshotBuilder.withKeyspace(value);
            break;
          case "Column family name":
            snapshotBuilder.withTable(value);
            break;
          case "True size":
            snapshotBuilder.withTrueSize(ICassandraManagementProxy.parseHumanReadableSize(value));
            break;
          case "Size on disk":
            snapshotBuilder.withSizeOnDisk(ICassandraManagementProxy.parseHumanReadableSize(value));
            break;
          default:
            break;
        }
      }
      snapshots.add(snapshotBuilder.withClusterName(getClusterName()).build());
    }
    return snapshots;
  }

  @Override
  public void takeSnapshot(String snapshotName, String... keyspaceNames) throws IOException {
    try {
      TakeSnapshotRequest req = new TakeSnapshotRequest();
      req.setSnapshotName(snapshotName);
      req.setKeyspaces(Arrays.asList(keyspaceNames));
      apiClient.takeSnapshot(req);
    } catch (ApiException ae) {
      LOG.error("Failed to take snapshot", ae);
      throw new IOException(ae);
    }
  }

  @Override
  public void takeColumnFamilySnapshot(String var1, String var2, String var3) throws IOException {
    // TODO: implement me.

  }

  @Override
  public Map<String, String> getTokenToEndpointMap() {
    // TODO: implement me.
    return new HashMap<>();
  }

  @Override
  public void forceKeyspaceCompaction(boolean var1, String var2, String... var3) throws IOException,
      ExecutionException, InterruptedException {
    // TODO: implement me.
  }

  // From CompactionManagerMBean
  @Override
  public List<Map<String, String>> getCompactions() {
    return new ArrayList<Map<String, String>>();
  }

  // From FailureDetectorMBean
  @Override
  public String getAllEndpointStates() {
    // TODO: implement me.
    return "";
  }

  @Override
  public Map<String, String> getSimpleStates() {
    // TODO: implement me.
    return new HashMap<String, String>();
  }

  // From EndpointSnitchInfoMBean
  @Override
  public String getDatacenter(String var1) throws UnknownHostException {
    // TODO: implement me.
    return "";
  }

  // From StreamManagerMBean
  @Override
  public Set<CompositeData> getCurrentStreams() {
    return new HashSet<CompositeData>();
  }

  @Override
  public String getUntranslatedHost() {
    //TODO: implement me
    return "";
  }
}
