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

package io.cassandrareaper.management;

import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Table;
import io.cassandrareaper.management.jmx.RepairStatusHandler;
import io.cassandrareaper.service.RingRange;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ExecutionException;
import javax.management.AttributeList;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.JMException;
import javax.management.MBeanInfo;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.QueryExp;
import javax.management.ReflectionException;
import javax.management.openmbean.TabularData;
import javax.validation.constraints.NotNull;

import org.apache.cassandra.repair.RepairParallelism;


public interface ICassandraManagementProxy extends NotificationListener {

  Duration DEFAULT_JMX_CONNECTION_TIMEOUT = Duration.ofSeconds(5);

  /**
   * Terminates all ongoing repairs on the node this proxy is connected to
   */
  void cancelAllRepairs();

  String getCassandraVersion();

  /**
   * @return Cassandra cluster name.
   */
  String getClusterName();

  /**
   * @return all hosts in the ring with their host id
   */
  @NotNull
  Map<String, String> getEndpointToHostId();

  String getLocalEndpoint() throws ReaperException;

  String getHost();

  /**
   * @return list of available keyspaces
   */
  List<String> getKeyspaces();

  List<String> getLiveNodes() throws ReaperException;

  /**
   * @return full class name of Cassandra's partitioner.
   */
  String getPartitioner();

  /**
   * @return number of pending compactions on the node this proxy is connected to
   */
  int getPendingCompactions() throws JMException;

  Map<List<String>, List<String>> getRangeToEndpointMap(String keyspace) throws ReaperException;

  Set<Table> getTablesForKeyspace(String keyspace) throws ReaperException;

  /**
   * @return list of tokens in the cluster
   */
  List<BigInteger> getTokens();

  boolean isConnectionAlive();

  /**
   * @return true if any repairs are running on the node.
   */
  boolean isRepairRunning() throws JMException;

  /**
   * Checks if table exists in the cluster by instantiating a MBean for that table.
   */
  Map<String, List<String>> listTablesByKeyspace();


  /**
   * Triggers a repair of range (beginToken, endToken] for given keyspace and column family. The
   * repair is triggered by {@link
   * org.apache.cassandra.service.StorageServiceMBean#forceRepairRangeAsync} For time being, we
   * don't allow local nor snapshot repairs.
   *
   * @return Repair command number, or 0 if nothing to repair
   */
  int triggerRepair(
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
      throws ReaperException;

  void close();

  void removeRepairStatusHandler(int repairNo);

  List<String> getRunningRepairMetricsPost22();

  // From StorageServiceMBean
  void clearSnapshot(String var1, String... var2) throws IOException;

  Map<String, TabularData> getSnapshotDetails();

  void takeSnapshot(String var1, String... var2) throws IOException;

  void takeColumnFamilySnapshot(String var1, String var2, String var3) throws IOException;

  Map<String, String> getTokenToEndpointMap();

  void forceKeyspaceCompaction(boolean var1, String var2, String... var3) throws IOException, ExecutionException,
      InterruptedException;


  // From MBeanServerConnection
  Set<ObjectName> queryNames(ObjectName name, QueryExp query)
      throws IOException;

  MBeanInfo getMBeanInfo(ObjectName name)
      throws InstanceNotFoundException, IntrospectionException,
      ReflectionException, IOException;

  AttributeList getAttributes(ObjectName name, String[] attributes)
      throws InstanceNotFoundException, ReflectionException,
      IOException;

  // From CompactionManagerMBean

  List<Map<String, String>> getCompactions();

  // From FailureDetectorMBean
  String getAllEndpointStates();

  Map<String, String> getSimpleStates();

  // From EndpointSnitchInfoMBean
  String getDatacenter(String var1) throws UnknownHostException;

  // from DiagnosticEventPersistenceMBean
  SortedMap<Long, Map<String, Serializable>> readEvents(String eventClass, Long lastKey, int limit);

  void enableEventPersistence(String eventClass);

  void disableEventPersistence(String eventClass);


}