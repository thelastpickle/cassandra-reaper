/*
 * Copyright 2023-2023 DataStax, Inc.
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
import io.cassandrareaper.core.GenericMetric;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.Snapshot;
import io.cassandrareaper.core.Table;
import io.cassandrareaper.management.ICassandraManagementProxy;
import io.cassandrareaper.management.RepairStatusHandler;
import io.cassandrareaper.management.http.models.JobStatusTracker;
import io.cassandrareaper.resources.view.NodesStatus;
import io.cassandrareaper.service.RingRange;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.management.JMException;
import javax.management.openmbean.CompositeData;
import javax.validation.constraints.NotNull;

import com.codahale.metrics.MetricRegistry;
import com.datastax.mgmtapi.client.api.DefaultApi;
import com.datastax.mgmtapi.client.invoker.ApiException;
import com.datastax.mgmtapi.client.model.CompactRequest;
import com.datastax.mgmtapi.client.model.EndpointStates;
import com.datastax.mgmtapi.client.model.Job;
import com.datastax.mgmtapi.client.model.RepairRequest;
import com.datastax.mgmtapi.client.model.RepairRequestResponse;
import com.datastax.mgmtapi.client.model.SnapshotDetails;
import com.datastax.mgmtapi.client.model.StatusChange;
import com.datastax.mgmtapi.client.model.TakeSnapshotRequest;
import com.datastax.mgmtapi.client.model.TokenRangeToEndpointResponse;
import com.datastax.mgmtapi.client.model.TokenRangeToEndpoints;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpCassandraManagementProxy implements ICassandraManagementProxy {

  public static final int DEFAULT_POLL_INTERVAL_IN_MILLISECONDS = 5000;
  private static final Logger LOG = LoggerFactory.getLogger(HttpCassandraManagementProxy.class);
  final String host;
  final MetricRegistry metricRegistry;
  final String rootPath;
  final InetSocketAddress endpoint;
  final DefaultApi apiClient;
  final int metricsPort;
  final Node node;
  final HttpMetricsProxy metricsProxy;

  final ConcurrentMap<Integer, RepairStatusHandler> repairStatusHandlers = Maps.newConcurrentMap();
  final ConcurrentMap<String, JobStatusTracker> jobTracker = Maps.newConcurrentMap();
  final ConcurrentMap<Integer, ExecutorService> repairStatusExecutors = Maps.newConcurrentMap();


  private ScheduledExecutorService statusTracker;

  public HttpCassandraManagementProxy(MetricRegistry metricRegistry,
                                      String rootPath,
                                      InetSocketAddress endpoint,
                                      ScheduledExecutorService executor,
                                      DefaultApi apiClient,
                                      int metricsPort,
                                      Node node
  ) {
    this.host = endpoint.getHostString();
    this.metricRegistry = metricRegistry;
    this.rootPath = rootPath;
    this.endpoint = endpoint;
    this.apiClient = apiClient;
    this.metricsPort = metricsPort;
    this.statusTracker = executor;
    this.node = node;
    this.metricsProxy = HttpMetricsProxy.create(this, node);

    // TODO Perhaps the poll interval should be configurable through context.config ?
    this.scheduleJobPoller(DEFAULT_POLL_INTERVAL_IN_MILLISECONDS);
  }

  public HttpCassandraManagementProxy(MetricRegistry metricRegistry,
                                      String rootPath,
                                      InetSocketAddress endpoint,
                                      ScheduledExecutorService executor,
                                      DefaultApi apiClient,
                                      int metricsPort,
                                      Node node,
                                      HttpMetricsProxy metricsProxy
  ) {
    this.host = endpoint.getHostString();
    this.metricRegistry = metricRegistry;
    this.rootPath = rootPath;
    this.endpoint = endpoint;
    this.apiClient = apiClient;
    this.metricsPort = metricsPort;
    this.statusTracker = executor;
    this.node = node;
    this.metricsProxy = metricsProxy;

    // TODO Perhaps the poll interval should be configurable through context.config ?
    this.scheduleJobPoller(DEFAULT_POLL_INTERVAL_IN_MILLISECONDS);
  }

  @Override
  public String getHost() {
    return host;
  }

  public int getMetricsPort() {
    return metricsPort;
  }

  @Override
  public List<BigInteger> getTokens() {
    try {
      EndpointStates endpointStates = apiClient.getEndpointStates();
      List<BigInteger> tokenList = new ArrayList<>();
      LOG.info("Retrieved endpoint states: {}", endpointStates.getEntity());
      endpointStates.getEntity().forEach((Map<String, String> states) -> {
        for (String token : states.get("TOKENS").split(",")) {
          tokenList.add(new BigInteger(token));
        }
      });
      // sort the list
      Collections.sort(tokenList);
      return tokenList;
    } catch (ApiException e) {
      LOG.error("Failed to retrieve endpoint states", e);
      return Collections.emptyList();
    } catch (Exception e) {
      LOG.error("Failed to retrieve endpoint states", e);
    }
  }

  @Override
  public Map<List<String>, List<String>> getRangeToEndpointMap(String keyspace) throws ReaperException {
    try {
      TokenRangeToEndpointResponse resp = apiClient.getRangeToEndpointMapV2(keyspace);
      List<TokenRangeToEndpoints> list = resp.getTokenRangeToEndpoints();
      Map<List<String>, List<String>> map = new HashMap<>(list.size());
      list.forEach((TokenRangeToEndpoints entry) -> {
        List<Long> tokens = entry.getTokens();
        List<String> range = new ArrayList<>(tokens.size());
        tokens.forEach((Long token) -> {
          range.add(token.toString());
        });
        map.put(range, entry.getEndpoints());
      });
      return map;
    } catch (ApiException e) {
      LOG.error("Failed to retrieve token range to endpoint mapping", e);
      throw new ReaperException(e);
    }
  }

  @NotNull
  @Override
  public String getLocalEndpoint() throws ReaperException {
    try {
      EndpointStates endpoints = apiClient.getEndpointStates();
      if (endpoints == null) {
        throw new ReaperException("No endpoint state data retrieved");
      }
      for (Map<String, String> state : endpoints.getEntity()) {
        if ("true".equals(state.get("IS_LOCAL"))) {
          String endpoint = state.get("ENDPOINT_IP");
          if (endpoint == null) {
            throw new ReaperException("Missing ENDPOINT_IP field in local endpoint state");
          }
          return endpoint;
        }
      }
      throw new ReaperException("Could not find local endpoint state (IS_LOCAL=true)");
    } catch (ApiException e) {
      throw new ReaperException("Error getting local endpoint", e);
    }
  }

  @NotNull
  @Override
  public Map<String, String> getEndpointToHostId() {
    try {
      return apiClient.getEndpointStates().getEntity().stream()
          .collect(
              Collectors.toMap(
                  i -> i.get("ENDPOINT_IP"),
                  i -> i.get("HOST_ID")
              )
          );
    } catch (ApiException ae) {
      LOG.error("Failed to retrieve endpoint states - does the HTTP proxy have connectivity?", ae);
      return Collections.emptyMap();
    }
  }

  @Override
  public String getPartitioner() throws ReaperException {
    EndpointStates endpointStates;
    try {
      endpointStates = apiClient.getEndpointStates();
      return endpointStates.getEntity().get(0).get("PARTITIONER");
    } catch (ApiException | RuntimeException e) {
      LOG.error("Failed to retrieve partitioner", e);
      throw new ReaperException(e);
    }
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
    try {
      return apiClient.listKeyspaces("");
    } catch (ApiException ae) {
      LOG.error("Failed to list keyspaces", ae);
      return Collections.emptyList();
    }
  }

  @Override
  public Set<Table> getTablesForKeyspace(String keyspace) throws ReaperException {
    try {
      return apiClient.listTablesV1(keyspace).stream()
          .map(t ->
              Table.builder()
                  .withName(t.getName())
                  .withCompactionStrategy(t.getCompaction().get("class"))
                  .build())
          .collect(Collectors.toSet());
    } catch (ApiException e) {
      throw new ReaperException("Error querying table data", e);
    }
  }

  @Override
  public int getPendingCompactions() throws ReaperException {
    List<GenericMetric> tpStatsMetrics = metricsProxy.collectTpPendingTasks();
    for (GenericMetric metric : tpStatsMetrics) {
      if (metric.getMetricName().equals("PendingTasks")
          && metric.getMetricScope().equals("CompactionExecutor")) {
        return (int) metric.getValue();
      }
    }
    throw new ReaperException("Failed to retrieve pending compactions");
  }

  @Override
  public boolean isRepairRunning() throws JMException {
    return true; // TODO: implement me. This is low priority because it is used only in tests now. It should be
    // removed longer term.
  }

  @Override
  public void cancelAllRepairs() {
    try {
      apiClient.deleteRepairsV2();
    } catch (ApiException ae) {
      LOG.error("Failed to cancel all repairs", ae);
    }
  }

  @Override
  public Map<String, List<String>> listTablesByKeyspace() throws ReaperException {
    Map<String, List<String>> tablesByKeyspace = Maps.newHashMap();
    try {
      List<String> keyspaces = apiClient.listKeyspaces("");
      for (String keyspace : keyspaces) {
        List<String> tables = apiClient.listTables(keyspace);
        tablesByKeyspace.put(keyspace, tables);
      }
    } catch (ApiException ae) {
      LOG.warn("Failed to list keyspaces", ae);
      throw new ReaperException(ae);
    }
    return Collections.unmodifiableMap(tablesByKeyspace);
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
      String keyspace,
      RepairParallelism repairParallelism,
      Collection<String> columnFamilies,
      boolean fullRepair,
      Collection<String> datacenters,
      RepairStatusHandler repairStatusHandler,
      List<RingRange> associatedTokens,
      int repairThreadCount)
      throws ReaperException {

    String jobId;
    try {
      RepairRequestResponse resp = apiClient.putRepairV2(
          (new RepairRequest())
              .fullRepair(fullRepair)
              .keyspace(keyspace)
              .tables(new ArrayList<>(columnFamilies))
              .repairParallelism(RepairRequest.RepairParallelismEnum.fromValue(repairParallelism.getName()))
              .repairThreadCount(repairThreadCount)
              .associatedTokens(
                  associatedTokens.stream().map(i ->
                      (new com.datastax.mgmtapi.client.model.RingRange())
                          .start(i.getStart().longValue())
                          .end(i.getEnd().longValue())
                  ).collect(Collectors.toList())
              )
      );
      jobId = resp.getRepairId();
    } catch (ApiException e) {
      throw new ReaperException(e);
    }

    int repairNo = Integer.parseInt(jobId.substring(7));

    repairStatusExecutors.putIfAbsent(repairNo, Executors.newSingleThreadExecutor());
    repairStatusHandlers.putIfAbsent(repairNo, repairStatusHandler);
    jobTracker.put(jobId, new JobStatusTracker());
    return repairNo;
  }

  @Override
  public void removeRepairStatusHandler(int repairNo) {
    repairStatusHandlers.remove(repairNo);
    ExecutorService repairStatusExecutor = repairStatusExecutors.remove(repairNo);
    if (null != repairStatusExecutor) {
      repairStatusExecutor.shutdown();
    }
    String jobId = String.format("repair-%d", repairNo);
    jobTracker.remove(jobId);
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
    // TODO: implement me. This is low priority since it is not called anywhere. It should be removed longer term.

  }

  @Override
  public Map<String, String> getTokenToEndpointMap() {
    try {
      EndpointStates epStates = apiClient.getEndpointStates();
      Map<String, String> tokenMap = new HashMap<>();
      for (Map<String, String> states : epStates.getEntity()) {
        String ip = states.get("ENDPOINT_IP");
        for (String token : states.get("TOKENS").split(",")) {
          tokenMap.put(token, ip);
        }
      }
      return tokenMap;
    } catch (ApiException ae) {
      LOG.error("Failed to get endpoint states", ae);
    }
    return null;
  }

  @Override
  public void forceKeyspaceCompaction(boolean splitOutput, String keyspaceName, String... columnFamilies) throws
      IOException {
    CompactRequest request = new CompactRequest();
    request.setSplitOutput(splitOutput);
    request.setKeyspaceName(keyspaceName);
    for (String columnFamily : columnFamilies) {
      request.addTablesItem(columnFamily);
    }
    try {
      apiClient.compact(request);
    } catch (ApiException ae) {
      LOG.error("Failed to force compaction", ae);
      throw new IOException(ae);
    }
  }

  // From CompactionManagerMBean
  @Override
  public List<Map<String, String>> getCompactions() {
    try {
      return apiClient.getCompactions().stream().map(Compactions::asMap).collect(Collectors.toList());
    } catch (ApiException ae) {
      LOG.error("Failed to get compactions", ae);
      return Collections.emptyList();
    }
  }

  @Override
  public NodesStatus getNodesStatus() throws ReaperException {
    try {
      EndpointStates endpoints = apiClient.getEndpointStates();
      if (endpoints == null) {
        throw new ReaperException("No endpoint state data retrieved.");
      }
      return new NodesStatus(getHost(), endpoints);
    } catch (ApiException ae) {
      throw new ReaperException("Failed to retrieve endpoint state data", ae);
    }
  }

  // From EndpointSnitchInfoMBean
  @Override
  public String getDatacenter(String host) throws UnknownHostException {
    // resolve the host to an IP address
    String hostIP = InetAddress.getByName(host).getHostAddress();
    try {
      EndpointStates epStates = apiClient.getEndpointStates();
      for (Map<String, String> stateMap : epStates.getEntity()) {
        if (hostIP.equals(stateMap.get("ENDPOINT_IP"))) {
          // we found the ENDPOINT_IP that matches the resolved hostIP, return its DC
          return stateMap.get("DC");
        }
      }
    } catch (ApiException e) {
      LOG.error("Failed to get Datacenter from endpoint states", e);
      throw new UnknownHostException(e.getMessage());
    }
    // Endpoint states did not have a mapping for ENDPOINT_IP that matches the resolved host IP
    LOG.error("No endpoint states found for host: " + host + ", resolved IP: " + hostIP);
    return null;
  }

  // From StreamManagerMBean
  @Override
  public Set<CompositeData> getCurrentStreams() {
    // TODO: implement me. This is low priority since it is used only for display of stuff in the UI for
    //  informational purposes. It should be implemented eventually.
    return new HashSet<CompositeData>();
  }

  @Override
  public String getUntranslatedHost() throws ReaperException {
    // TODO getLocalEndpoint returns the "internal" IP, figure out if we have to do any conversion here. If not, this
    //  method can be inlined.
    return host;
  }

  private Job getJobStatus(String id) {
    // Poll with HTTP client the job's status
    try {
      return apiClient.getJobStatus(id);
    } catch (ApiException e) {
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  private void scheduleJobPoller(int pollInterval) {
    statusTracker.scheduleWithFixedDelay(
        notificationsTracker(),
        pollInterval * 2,
        pollInterval,
        TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  Runnable notificationsTracker() {
    return () -> {
      if (jobTracker.size() > 0) {
        for (Map.Entry<String, JobStatusTracker> entry : jobTracker.entrySet()) {
          Job job = getJobStatus(entry.getKey());
          int availableNotifications = job.getStatusChanges().size();
          int currentNotificationCount = entry.getValue().latestNotificationCount.get();

          if (currentNotificationCount < availableNotifications) {
            // We need to process the new ones
            for (int i = currentNotificationCount; i < availableNotifications; i++) {
              StatusChange statusChange = job.getStatusChanges().get(i);
              // remove "repair-" prefix
              int repairNo = Integer.parseInt(job.getId().substring(7));
              ProgressEventType progressType = ProgressEventType.valueOf(statusChange.getStatus());
              repairStatusExecutors.get(repairNo).submit(() -> {
                repairStatusHandlers
                    .get(repairNo)
                    .handle(repairNo, Optional.empty(), Optional.of(progressType),
                        statusChange.getMessage(), this);
              });

              // Update the count as we process them
              entry.getValue().latestNotificationCount.incrementAndGet();
            }
          }
        }
      }
    };
  }
}