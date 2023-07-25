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
import io.cassandrareaper.management.jmx.RepairStatusHandler;
import io.cassandrareaper.service.RingRange;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
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
import javax.management.MBeanInfo;
import javax.management.Notification;
import javax.management.ObjectName;
import javax.management.QueryExp;
import javax.management.ReflectionException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import javax.validation.constraints.NotNull;

import com.codahale.metrics.MetricRegistry;
import org.apache.cassandra.repair.RepairParallelism;

public class HttpCassandraManagementProxy implements ICassandraManagementProxy {
  String host;
  MetricRegistry metricRegistry;
  String rootPath;
  InetSocketAddress endpoint;

  public HttpCassandraManagementProxy(MetricRegistry metricRegistry,
                                      String rootPath,
                                      InetSocketAddress endpoint
  ) {
    this.metricRegistry = metricRegistry;
    this.rootPath = rootPath;
    this.endpoint = endpoint;
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
    return null; // TODO: implement me.
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
    return null; // TODO: implement me.
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
    return null; // TODO: implement me.
  }

  public void clearSnapshot(String var1, String... var2) throws IOException {
    // TODO: implement me.
  }

  public Map<String, TabularData> getSnapshotDetails() {
    // TODO: implement me.
    return new HashMap<>();
  }

  public void takeSnapshot(String var1, String... var2) throws IOException {
    // TODO: implement me.
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

  public void disableEventPersistence(String eventClass){
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



}