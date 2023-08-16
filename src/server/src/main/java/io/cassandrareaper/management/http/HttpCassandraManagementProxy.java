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
import io.cassandrareaper.core.Table;
import io.cassandrareaper.management.ICassandraManagementProxy;
import io.cassandrareaper.management.RepairStatusHandler;
import io.cassandrareaper.service.RingRange;

import java.io.IOException;
import java.io.Serializable;
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
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import javax.management.AttributeList;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.JMException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanInfo;
import javax.management.Notification;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.QueryExp;
import javax.management.ReflectionException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import javax.validation.constraints.NotNull;

import com.codahale.metrics.MetricRegistry;
import com.datastax.mgmtapi.client.api.DefaultApi;
import com.datastax.mgmtapi.client.invoker.ApiClient;
import com.datastax.mgmtapi.client.invoker.ApiException;
import com.datastax.mgmtapi.client.model.EndpointStates;
import com.datastax.mgmtapi.client.model.TakeSnapshotRequest;
import org.apache.cassandra.repair.RepairParallelism;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpCassandraManagementProxy implements ICassandraManagementProxy {
  private static final Logger LOG = LoggerFactory.getLogger(HttpCassandraManagementProxy.class);

  String host;
  MetricRegistry metricRegistry;
  String rootPath;
  InetSocketAddress endpoint;
  DefaultApi apiClient;

  public HttpCassandraManagementProxy(MetricRegistry metricRegistry,
                                      String rootPath,
                                      InetSocketAddress endpoint
  ) {
    this.metricRegistry = metricRegistry;
    this.rootPath = rootPath;
    this.endpoint = endpoint;
    final String basePath = new StringBuilder().append("http://").append(endpoint.getHostName()).append(":").append(endpoint.getPort()).append(rootPath).toString();
    this.apiClient = new DefaultApi(new ApiClient().setBasePath(basePath));
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
    try {
      return apiClient.getClusterName();
    } catch (ApiException ae) {
      LOG.error("Could not fetch Cluster Name", ae);
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
  public void handleNotification(final Notification notification, Object handback) {
    // TODO: implement me.
  }

  @Override
  public boolean isConnectionAlive() {
    return true; // TODO: implement me.
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
    List<String> liveNodes = new ArrayList<>();
    // first get the endpoint states
    try {
      EndpointStates endpoints = apiClient.getEndpointStates();
      for (Map<String, String> states : endpoints.getEntity()) {
        if (states.containsKey("IS_ALIVE") && Boolean.parseBoolean(states.get("IS_ALIVE"))) {
          liveNodes.add(states.get("ENDPOINT_IP"));
        }
      }
    } catch (ApiException ae) {
      LOG.error("Failed to retrieve Live Nodes", ae);
    }
    return liveNodes;
  }

  public void clearSnapshot(String var1, String... var2) throws IOException {
    try {
      apiClient.clearSnapshots(Arrays.asList(var1), Arrays.asList(var2));
    } catch (ApiException ae) {
      LOG.error("Error clearing snapshots", ae);
    }
  }

  public Map<String, TabularData> getSnapshotDetails() {
    // TODO: This API needs to not return TabularData as that is a JMX specific thing
    Map<String, TabularData> snapshotDetails = new HashMap<>();
    try {
      String details = apiClient.getSnapshotDetails(null, null);
      LOG.error("Snapshots retrieved:\n" + details);
    } catch (ApiException ae) {
      LOG.error("Failed to retrieve snapshot details", ae);
    }
    return snapshotDetails;
  }

  public void takeSnapshot(String var1, String... var2) throws IOException {
    TakeSnapshotRequest req = new TakeSnapshotRequest().snapshotName(var1).keyspaces(Arrays.asList(var2));
    try {
      apiClient.takeSnapshot(req);
    } catch (ApiException ae) {
      LOG.error("Failed to take snapshot", ae);
    }
  }

  public void takeColumnFamilySnapshot(String var1, String var2, String var3) throws IOException {
    // TODO: implement me.

  }

  public Map<String, String> getTokenToEndpointMap() {
    // TODO: implement me.
    return new HashMap<>();
  }

  public void forceKeyspaceCompaction(boolean var1, String var2, String... var3) throws IOException,
      ExecutionException, InterruptedException {
    // TODO: implement me.
  }


  // From MBeanServerConnection
  public Set<ObjectName> queryNames(ObjectName name, QueryExp query)
      throws IOException {
    // TODO: implement me.
    return new HashSet<ObjectName>();
  }

  public MBeanInfo getMBeanInfo(ObjectName name)
      throws InstanceNotFoundException, IntrospectionException,
      ReflectionException, IOException {
    // TODO: implement me.
    return new MBeanInfo("", "", null, null, null, null);
  }

  public AttributeList getAttributes(ObjectName name, String[] attributes)
      throws InstanceNotFoundException, ReflectionException,
      IOException {
    // TODO: implement me.
    return new AttributeList();
  }


  // From CompactionManagerMBean
  public List<Map<String, String>> getCompactions() {
    return new ArrayList<Map<String, String>>();
  }

  // From FailureDetectorMBean
  public String getAllEndpointStates() {
    // TODO: implement me.
    return "";
  }

  public Map<String, String> getSimpleStates() {
    // TODO: implement me.
    return new HashMap<String, String>();
  }

  // From EndpointSnitchInfoMBean
  public String getDatacenter(String var1) throws UnknownHostException {
    // TODO: implement me.
    return "";
  }

  // from DiagnosticEventPersistenceMBean
  public SortedMap<Long, Map<String, Serializable>> readEvents(String eventClass, Long lastKey, int limit) {
    //TODO: implement me
    return new TreeMap<>();
  }

  public void enableEventPersistence(String eventClass) {
    //TODO: implement me
  }

  public void disableEventPersistence(String eventClass) {
    //TODO: implement me
  }

  // From LastEventIdBroadcasterMBean
  public Map<String, Comparable> getLastEventIdsIfModified(long lastUpdate) {
    return new HashMap<String, Comparable>();
  }

  // From StreamManagerMBean
  public Set<CompositeData> getCurrentStreams() {
    return new HashSet<CompositeData>();
  }

  public void addConnectionNotificationListener(NotificationListener listener) {
    // TODO - implement me.
  }

  public void removeConnectionNotificationListener(NotificationListener listener) throws ListenerNotFoundException {
    // TODO - implement me.
  }

  public void addNotificationListener(NotificationListener listener, NotificationFilter filter) {
    // TODO - implement me.
  }

  public void removeNotificationListener(NotificationListener listener) throws IOException, JMException {
    // TODO - implement me.
  }

  public String getUntranslatedHost() {
    //TODO: implement me
    return "";
  }


}