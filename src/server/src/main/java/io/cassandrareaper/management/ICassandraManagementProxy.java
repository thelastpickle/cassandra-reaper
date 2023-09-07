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
import io.cassandrareaper.core.Snapshot;
import io.cassandrareaper.core.Table;
import io.cassandrareaper.service.RingRange;

import java.io.IOException;
import java.math.BigInteger;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.management.JMException;
import javax.management.openmbean.CompositeData;
import javax.validation.constraints.NotNull;

import com.datastax.driver.core.VersionNumber;
import org.apache.cassandra.repair.RepairParallelism;


public interface ICassandraManagementProxy {

  Duration DEFAULT_JMX_CONNECTION_TIMEOUT = Duration.ofSeconds(5);
  long KB_FACTOR = 1000;
  long KIB_FACTOR = 1024;
  long MB_FACTOR = 1000 * KB_FACTOR;
  long GB_FACTOR = 1000 * MB_FACTOR;
  long MIB_FACTOR = 1024 * KIB_FACTOR;
  long GIB_FACTOR = 1024 * MIB_FACTOR;


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
  String getPartitioner() throws ReaperException;

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

  /**
   * @return true if any repairs are running on the node.
   */
  boolean isRepairRunning() throws JMException;

  /**
   * Checks if table exists in the cluster by instantiating a MBean for that table.
   */
  Map<String, List<String>> listTablesByKeyspace() throws ReaperException;


  /**
   * Triggers a repair of range (beginToken, endToken] for given keyspace and column family. The
   * repair is triggered by {@link
   * org.apache.cassandra.service.StorageServiceMBean#forceRepairRangeAsync} For time being, we
   * don't allow local nor snapshot repairs.
   *
   * @return Repair command number, or 0 if nothing to repair
   */
  int triggerRepair(
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

  // From StorageServiceMBean
  void clearSnapshot(String var1, String... var2) throws IOException;

  List<Snapshot> listSnapshots() throws UnsupportedOperationException;

  void takeSnapshot(String var1, String... var2) throws IOException;

  void takeColumnFamilySnapshot(String var1, String var2, String var3) throws IOException;

  Map<String, String> getTokenToEndpointMap();

  void forceKeyspaceCompaction(boolean splitOutput, String keyspaceName, String... columnFamilies) throws IOException,
      ExecutionException,
      InterruptedException;

  // From CompactionManagerMBean

  List<Map<String, String>> getCompactions();

  // From FailureDetectorMBean
  String getAllEndpointStates();

  Map<String, String> getSimpleStates();

  // From EndpointSnitchInfoMBean
  String getDatacenter(String var1) throws UnknownHostException;

  // From StreamManagerMBean
  Set<CompositeData> getCurrentStreams();

  /**
   * Compares two Cassandra versions using classes provided by the Datastax Java Driver.
   *
   * @param str1 a string of ordinal numbers separated by decimal points.
   * @param str2 a string of ordinal numbers separated by decimal points.
   * @return The result is a negative integer if str1 is _numerically_ less than str2. The result is
   *     a positive integer if str1 is _numerically_ greater than str2. The result is zero if the
   *     strings are _numerically_ equal. It does not work if "1.10" is supposed to be equal to
   *     "1.10.0".
   */
  static Integer versionCompare(String str1, String str2) {
    VersionNumber version1 = VersionNumber.parse(str1);
    VersionNumber version2 = VersionNumber.parse(str2);

    return version1.compareTo(version2);
  }

  String getUntranslatedHost() throws ReaperException;

  static double parseHumanReadableSize(String readableSize) {
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
      case "bytes":
        return ret;
      default:
        return 0;
    }
  }


}