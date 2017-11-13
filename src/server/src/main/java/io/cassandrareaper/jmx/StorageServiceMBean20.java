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

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

import javax.management.NotificationEmitter;

import org.apache.cassandra.service.StorageServiceMBean;

public interface StorageServiceMBean20 extends NotificationEmitter, StorageServiceMBean {

  /**
   * Numeric load value.
   *
   * @see org.apache.cassandra.metrics.StorageMetrics#load
   */
  @Deprecated
  double getLoad();

  /** Forces major compaction of a single keyspace */
  void forceKeyspaceCompaction(String keyspaceName, String... columnFamilies)
      throws IOException, ExecutionException, InterruptedException;

  /**
   * Invoke repair asynchronously. You can track repair progress by subscribing JMX notification
   * sent from this StorageServiceMBean. Notification format is: type: "repair" userObject: int
   * array of length 2, [0]=command number, [1]=ordinal of AntiEntropyService.Status
   *
   * @return Repair command number, or 0 if nothing to repair
   */
  int forceRepairAsync(
      String keyspace,
      boolean isSequential,
      Collection<String> dataCenters,
      final Collection<String> hosts,
      boolean primaryRange,
      String... columnFamilies);

  /**
   * Invoke repair asynchronously. You can track repair progress by subscribing JMX notification
   * sent from this StorageServiceMBean. Notification format is: type: "repair" userObject: int
   * array of length 2, [0]=command number, [1]=ordinal of AntiEntropyService.Status
   *
   * @param parallelismDegree 0: sequential, 1: parallel, 2: DC parallel
   * @return Repair command number, or 0 if nothing to repair
   */
  int forceRepairAsync(
      String keyspace,
      int parallelismDegree,
      Collection<String> dataCenters,
      final Collection<String> hosts,
      boolean primaryRange,
      String... columnFamilies);

  /**
   * Invoke repair asynchronously. You can track repair progress by subscribing JMX notification
   * sent from this StorageServiceMBean. Notification format is: type: "repair" userObject: int
   * array of length 2, [0]=command number, [1]=ordinal of AntiEntropyService.Status
   *
   * @return Repair command number, or 0 if nothing to repair
   * @see #forceKeyspaceRepair(String, boolean, boolean, String...)
   */
  int forceRepairAsync(
      String keyspace,
      boolean isSequential,
      boolean isLocal,
      boolean primaryRange,
      String... columnFamilies);

  /** Same as forceRepairAsync, but handles a specified range */
  int forceRepairRangeAsync(
      String beginToken,
      String endToken,
      final String keyspaceName,
      boolean isSequential,
      Collection<String> dataCenters,
      final Collection<String> hosts,
      final String... columnFamilies);

  /**
   * Same as forceRepairAsync, but handles a specified range
   *
   * @param parallelismDegree 0: sequential, 1: parallel, 2: DC parallel
   */
  int forceRepairRangeAsync(
      String beginToken,
      String endToken,
      final String keyspaceName,
      int parallelismDegree,
      Collection<String> dataCenters,
      final Collection<String> hosts,
      final String... columnFamilies);

  /** Same as forceRepairAsync, but handles a specified range */
  int forceRepairRangeAsync(
      String beginToken,
      String endToken,
      final String keyspaceName,
      boolean isSequential,
      boolean isLocal,
      final String... columnFamilies);

  /**
   * Triggers proactive repair for given column families, or all columnfamilies for the given
   * keyspace if none are explicitly listed.
   */
  void forceKeyspaceRepair(
      String keyspaceName, boolean isSequential, boolean isLocal, String... columnFamilies)
      throws IOException;

  /** Triggers proactive repair but only for the node primary range. */
  void forceKeyspaceRepairPrimaryRange(
      String keyspaceName, boolean isSequential, boolean isLocal, String... columnFamilies)
      throws IOException;

  /**
   * Perform repair of a specific range.
   *
   * <p>This allows incremental repair to be performed by having an external controller submitting
   * repair jobs. Note that the provided range much be a subset of one of the node local range.
   */
  void forceKeyspaceRepairRange(
      String beginToken,
      String endToken,
      String keyspaceName,
      boolean isSequential,
      boolean isLocal,
      String... columnFamilies)
      throws IOException;

  /** set the logging level at runtime */
  void setLog4jLevel(String classQualifier, String level);

  @Deprecated
  int getExceptionCount();

}
