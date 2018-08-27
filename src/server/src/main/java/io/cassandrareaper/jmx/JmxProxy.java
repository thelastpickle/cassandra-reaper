/*
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

package io.cassandrareaper.jmx;

import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.JmxStat;
import io.cassandrareaper.core.Segment;
import io.cassandrareaper.service.RingRange;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.JMException;
import javax.management.NotificationListener;
import javax.validation.constraints.NotNull;

import org.apache.cassandra.repair.RepairParallelism;


public interface JmxProxy extends NotificationListener {

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

  List<RingRange> getRangesForLocalEndpoint(String keyspace) throws ReaperException;

  Set<String> getTableNamesForKeyspace(String keyspace) throws ReaperException;

  /**
   * @return list of tokens in the cluster
   */
  List<BigInteger> getTokens();

  boolean isConnectionAlive();

  /**
   * @return true if any repairs are running on the node.
   */
  boolean isRepairRunning() throws JMException;

  /** Checks if table exists in the cluster by instantiating a MBean for that table. */
  Map<String, List<String>> listTablesByKeyspace();

  /**
   * @return all hosts owning a range of tokens
   */
  @NotNull
  List<String> tokenRangeToEndpoint(String keyspace, Segment segment);

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

  Map<String, List<JmxStat>> collectTpStats() throws JMException, IOException;

  Map<String, List<JmxStat>> collectDroppedMessages() throws JMException, IOException;

  Map<String, List<JmxStat>> collectLatencyMetrics() throws JMException, IOException;

}
