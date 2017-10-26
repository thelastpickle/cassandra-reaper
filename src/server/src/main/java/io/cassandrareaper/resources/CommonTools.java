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

package io.cassandrareaper.resources;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.jmx.JmxProxy;
import io.cassandrareaper.service.RingRange;
import io.cassandrareaper.service.SegmentGenerator;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public final class CommonTools {

  public static final Splitter COMMA_SEPARATED_LIST_SPLITTER
      = Splitter.on(',').trimResults(CharMatcher.anyOf(" ()[]\"'")).omitEmptyStrings();

  private static final Logger LOG = LoggerFactory.getLogger(CommonTools.class);

  private CommonTools() {
  }

  /**
   * Creates a repair run but does not start it immediately.
   *
   * <p>Creating a repair run involves: 1) split token range into segments 2) create a RepairRun
   * instance 3) create RepairSegment instances linked to RepairRun.
   *
   * @throws ReaperException if repair run fails to be stored into Reaper's storage.
   */
  public static RepairRun registerRepairRun(
      AppContext context,
      Cluster cluster,
      RepairUnit repairUnit,
      Optional<String> cause,
      String owner,
      int segments,
      int segmentsPerNode,
      RepairParallelism repairParallelism,
      Double intensity)
      throws ReaperException {

    // preparing a repair run involves several steps
    // the first step is to generate token segments
    List<RingRange> tokenSegments =
        repairUnit.getIncrementalRepair()
            ? Lists.newArrayList()
            : generateSegments(context, cluster, segments, segmentsPerNode, repairUnit);

    checkNotNull(tokenSegments, "failed generating repair segments");

    Map<String, RingRange> nodes = getClusterNodes(context, cluster, repairUnit);
    // the next step is to prepare a repair run objec
    segments = repairUnit.getIncrementalRepair() ? nodes.keySet().size() : tokenSegments.size();

    RepairRun.Builder runBuilder
        = createNewRepairRun(cluster, repairUnit, cause, owner, segments, repairParallelism, intensity);

    // the last preparation step is to generate actual repair segments
    List<RepairSegment.Builder> segmentBuilders = repairUnit.getIncrementalRepair()
        ? createRepairSegmentsForIncrementalRepair(nodes, repairUnit)
        : createRepairSegments(tokenSegments, repairUnit);

    RepairRun repairRun = context.storage.addRepairRun(runBuilder, segmentBuilders);

    if (null == repairRun) {
      String errMsg = String.format(
          "failed storing repair run for cluster \"%s\", keyspace \"%s\", and column families: %s",
          cluster.getName(), repairUnit.getKeyspaceName(), repairUnit.getColumnFamilies());

      LOG.error(errMsg);
      throw new ReaperException(errMsg);
    }
    return repairRun;
  }

  /**
   * Splits a token range for given table into segments
   *
   * @return the created segments
   * @throws ReaperException when fails to discover seeds for the cluster or fails to connect to any
   *     of the nodes in the Cluster.
   */
  private static List<RingRange> generateSegments(
      AppContext context,
      Cluster targetCluster,
      int segmentCount,
      int segmentCountPerNode,
      RepairUnit repairUnit)
      throws ReaperException {

    List<RingRange> segments = Lists.newArrayList();

    Preconditions.checkNotNull(
        targetCluster.getPartitioner(),
        "no partitioner for cluster: " + targetCluster.getName());

    SegmentGenerator sg = new SegmentGenerator(targetCluster.getPartitioner());
    Set<String> seedHosts = targetCluster.getSeedHosts();
    if (seedHosts.isEmpty()) {
      String errMsg = String.format("didn't get any seed hosts for cluster \"%s\"", targetCluster.getName());
      LOG.error(errMsg);
      throw new ReaperException(errMsg);
    }

    try (JmxProxy jmxProxy = context.jmxConnectionFactory.connectAny(
        Optional.absent(), seedHosts, context.config.getJmxConnectionTimeoutInSeconds())) {

      List<BigInteger> tokens = jmxProxy.getTokens();
      Map<List<String>, List<String>> rangeToEndpoint = jmxProxy.getRangeToEndpointMap(repairUnit.getKeyspaceName());
      Map<String, List<RingRange>> endpointToRange = buildEndpointToRangeMap(rangeToEndpoint);

      int globalSegmentCount = segmentCount;
      if (globalSegmentCount == 0) {
        globalSegmentCount =
            CommonTools.computeGlobalSegmentCount(
                segmentCountPerNode, rangeToEndpoint, endpointToRange);
      }

      segments =
          filterSegmentsByNodes(
              sg.generateSegments(globalSegmentCount, tokens, repairUnit.getIncrementalRepair()),
              repairUnit,
              endpointToRange);
    } catch (ReaperException e) {
      LOG.warn("couldn't connect to any host: {}, life sucks...", seedHosts, e);
    }

    if (segments == null || (segments.isEmpty() && !repairUnit.getIncrementalRepair())) {
      String errMsg = String.format("failed to generate repair segments for cluster \"%s\"", targetCluster.getName());
      LOG.error(errMsg);
      throw new ReaperException(errMsg);
    }
    return segments;
  }

  @VisibleForTesting
  static int computeGlobalSegmentCount(
      int segmentCountPerNode,
      Map<List<String>, List<String>> rangeToEndpoint,
      Map<String, List<RingRange>> endpointToRange) {
    int nodeCount = Math.max(1, endpointToRange.keySet().size());
    int tokenRangeCount = rangeToEndpoint.keySet().size();

    if (segmentCountPerNode < (tokenRangeCount / nodeCount) && segmentCountPerNode > 0) {
      return tokenRangeCount;
    }

    if (segmentCountPerNode == 0) {
      return Math.max(16 * nodeCount, tokenRangeCount);
    }

    return segmentCountPerNode * nodeCount;
  }

  @VisibleForTesting
  public static List<RingRange> filterSegmentsByNodes(
      List<RingRange> segments,
      RepairUnit repairUnit,
      Map<String, List<RingRange>> endpointToRange)
      throws ReaperException {

    if (repairUnit.getNodes().isEmpty()) {
      return segments;
    } else {
      return segments
          .stream()
          .filter(
              segment -> {
                for (Entry<String, List<RingRange>> entry : endpointToRange.entrySet()) {
                  if (repairUnit.getNodes().contains(entry.getKey())) {
                    for (RingRange range : entry.getValue()) {
                      if (range.encloses(segment)) {
                        return true;
                      }
                    }
                  }
                }
                return false;
              })
          .collect(Collectors.toList());
    }
  }

  @VisibleForTesting
  public static Map<String, List<RingRange>> buildEndpointToRangeMap(Map<List<String>, List<String>> rangeToEndpoint) {
    Map<String, List<RingRange>> endpointToRange = Maps.newHashMap();

    for (Entry<List<String>, List<String>> entry : rangeToEndpoint.entrySet()) {
      RingRange range = new RingRange(entry.getKey().toArray(new String[entry.getKey().size()]));
      for (String endpoint : entry.getValue()) {
        List<RingRange> ranges = endpointToRange.getOrDefault(endpoint, Lists.newArrayList());
        ranges.add(range);
        endpointToRange.put(endpoint, ranges);
      }
    }

    return endpointToRange;
  }

  /**
   * Instantiates a RepairRun and stores it in the storage backend.
   *
   * @return the new, just stored RepairRun instance
   * @throws ReaperException when fails to store the RepairRun.
   */
  private static RepairRun.Builder createNewRepairRun(
      Cluster cluster,
      RepairUnit repairUnit,
      Optional<String> cause,
      String owner,
      int segments,
      RepairParallelism repairParallelism,
      Double intensity)
      throws ReaperException {

    return new RepairRun.Builder(
        cluster.getName(), repairUnit.getId(), DateTime.now(), intensity, segments, repairParallelism)
        .cause(cause.isPresent() ? cause.get() : "no cause specified")
        .owner(owner);
  }

  /**
   * Creates the repair runs linked to given RepairRun and stores them directly in the storage backend.
   */
  private static List<RepairSegment.Builder> createRepairSegments(
      List<RingRange> tokenSegments,
      RepairUnit repairUnit) {

    List<RepairSegment.Builder> repairSegmentBuilders = Lists.newArrayList();
    tokenSegments.forEach(range -> repairSegmentBuilders.add(RepairSegment.builder(range, repairUnit.getId())));
    return repairSegmentBuilders;
  }

  /**
   * Creates the repair runs linked to given RepairRun and stores them directly in the storage backend in case of
   * incrementalRepair
   */
  private static List<RepairSegment.Builder> createRepairSegmentsForIncrementalRepair(
      Map<String, RingRange> nodes,
      RepairUnit repairUnit) {

    List<RepairSegment.Builder> repairSegmentBuilders = Lists.newArrayList();

    nodes
        .entrySet()
        .forEach(
            range
              -> repairSegmentBuilders.add(
                  RepairSegment.builder(range.getValue(), repairUnit.getId()).coordinatorHost(range.getKey())));

    return repairSegmentBuilders;
  }

  private static Map<String, RingRange> getClusterNodes(
      AppContext context,
      Cluster targetCluster,
      RepairUnit repairUnit) throws ReaperException {

    ConcurrentHashMap<String, RingRange> nodesWithRanges = new ConcurrentHashMap<>();
    Set<String> seedHosts = targetCluster.getSeedHosts();
    if (seedHosts.isEmpty()) {
      String errMsg = String.format("didn't get any seed hosts for cluster \"%s\"", targetCluster.getName());
      LOG.error(errMsg);
      throw new ReaperException(errMsg);
    }

    Map<List<String>, List<String>> rangeToEndpoint = Maps.newHashMap();

    try (JmxProxy jmxProxy = context.jmxConnectionFactory.connectAny(
        Optional.absent(), seedHosts, context.config.getJmxConnectionTimeoutInSeconds())) {

      rangeToEndpoint = jmxProxy.getRangeToEndpointMap(repairUnit.getKeyspaceName());
    } catch (ReaperException e) {
      LOG.error("couldn't connect to any host: {}, will try next one", e);
      throw new ReaperException(e);
    }

    for (Entry<List<String>, List<String>> tokenRangeToEndpoint : rangeToEndpoint.entrySet()) {
      String node = tokenRangeToEndpoint.getValue().get(0);
      RingRange range = new RingRange(tokenRangeToEndpoint.getKey().get(0), tokenRangeToEndpoint.getKey().get(1));
      nodesWithRanges.putIfAbsent(node, range);
    }

    return nodesWithRanges;
  }

  /**
   * Instantiates a RepairSchedule and stores it in the storage backend.
   *
   * @return the new, just stored RepairSchedule instance
   * @throws ReaperException when fails to store the RepairSchedule.
   */
  public static RepairSchedule storeNewRepairSchedule(
      AppContext context,
      Cluster cluster,
      RepairUnit repairUnit,
      int daysBetween,
      DateTime nextActivation,
      String owner,
      int segmentCountPerNode,
      RepairParallelism repairParallelism,
      Double intensity)
      throws ReaperException {

    RepairSchedule.Builder scheduleBuilder =
        new RepairSchedule.Builder(
            repairUnit.getId(),
            RepairSchedule.State.ACTIVE,
            daysBetween,
            nextActivation,
            ImmutableList.<UUID>of(),
            0,
            repairParallelism,
            intensity,
            DateTime.now(),
            segmentCountPerNode);

    scheduleBuilder.owner(owner);

    Collection<RepairSchedule> repairSchedules = context.storage
        .getRepairSchedulesForClusterAndKeyspace(repairUnit.getClusterName(), repairUnit.getKeyspaceName());

    for (RepairSchedule sched : repairSchedules) {
      Optional<RepairUnit> repairUnitForSched = context.storage.getRepairUnit(sched.getRepairUnitId());
      if (repairUnitForSched.isPresent()
          && repairUnitForSched.get().getClusterName().equals(repairUnit.getClusterName())
          && repairUnitForSched.get().getKeyspaceName().equals(repairUnit.getKeyspaceName())
          && repairUnitForSched.get().getIncrementalRepair().equals(repairUnit.getIncrementalRepair())) {
        if (CommonTools.isConflictingSchedules(repairUnitForSched.get(), repairUnit)) {
          String errMsg = String.format(
              "A repair schedule already exists for cluster \"%s\", " + "keyspace \"%s\", and column families: %s",
              cluster.getName(),
              repairUnit.getKeyspaceName(),
              Sets.intersection(repairUnit.getColumnFamilies(), repairUnitForSched.get().getColumnFamilies()));
          LOG.error(errMsg);
          throw new ReaperException(errMsg);
        }
      }
    }

    RepairSchedule newRepairSchedule = context.storage.addRepairSchedule(scheduleBuilder);
    if (newRepairSchedule == null) {
      String errMsg = String.format(
          "failed storing repair schedule for cluster \"%s\", " + "keyspace \"%s\", and column families: %s",
          cluster.getName(), repairUnit.getKeyspaceName(), repairUnit.getColumnFamilies());
      LOG.error(errMsg);
      throw new ReaperException(errMsg);
    }
    return newRepairSchedule;
  }

  private static boolean isConflictingSchedules(
      RepairUnit newRepairUnit, RepairUnit existingRepairUnit) {
    return (newRepairUnit.getColumnFamilies().isEmpty()
            && existingRepairUnit.getColumnFamilies().isEmpty())
        || (!Sets.intersection(
                existingRepairUnit.getColumnFamilies(), newRepairUnit.getColumnFamilies())
            .isEmpty())
        || (!existingRepairUnit.getBlacklistedTables().isEmpty()
            && !newRepairUnit.getBlacklistedTables().isEmpty());
  }

  public static Set<String> getTableNamesBasedOnParam(
      AppContext context,
      Cluster cluster,
      String keyspace,
      Optional<String> tableNamesParam) throws ReaperException {

    Set<String> knownTables;
    try (JmxProxy jmxProxy
        = context.jmxConnectionFactory.connectAny(cluster, context.config.getJmxConnectionTimeoutInSeconds())) {
      knownTables = jmxProxy.getTableNamesForKeyspace(keyspace);
      if (knownTables.isEmpty()) {
        LOG.debug("no known tables for keyspace {} in cluster {}", keyspace, cluster.getName());
        throw new IllegalArgumentException("no column families found for keyspace");
      }
    }
    Set<String> tableNames;
    if (tableNamesParam.isPresent() && !tableNamesParam.get().isEmpty()) {
      tableNames = Sets.newHashSet(COMMA_SEPARATED_LIST_SPLITTER.split(tableNamesParam.get()));
      for (String name : tableNames) {
        if (!knownTables.contains(name)) {
          throw new IllegalArgumentException("keyspace doesn't contain a table named \"" + name + "\"");
        }
      }
    } else {
      tableNames = Collections.emptySet();
    }
    return tableNames;
  }

  public static Set<String> getNodesToRepairBasedOnParam(
      AppContext context,
      Cluster cluster,
      Optional<String> nodesToRepairParam) throws ReaperException {

    Set<String> nodesInCluster;
    try (JmxProxy jmxProxy
        = context.jmxConnectionFactory.connectAny(cluster, context.config.getJmxConnectionTimeoutInSeconds())) {
      nodesInCluster = jmxProxy.getEndpointToHostId().keySet();
      if (nodesInCluster.isEmpty()) {
        LOG.debug("no nodes found in cluster {}", cluster.getName());
        throw new IllegalArgumentException("no nodes found in cluster");
      }
    }
    Set<String> nodesToRepair;
    if (nodesToRepairParam.isPresent() && !nodesToRepairParam.get().isEmpty()) {
      nodesToRepair = Sets.newHashSet(COMMA_SEPARATED_LIST_SPLITTER.split(nodesToRepairParam.get()));
      for (String node : nodesToRepair) {
        if (!nodesInCluster.contains(node)) {
          throw new IllegalArgumentException(
              "cluster \"" + cluster.getName() + "\" doesn't contain a node named \"" + node + "\"");
        }
      }
    } else {
      nodesToRepair = Collections.emptySet();
    }
    return nodesToRepair;
  }

  public static Set<String> getDatacentersToRepairBasedOnParam(
      AppContext context,
      Cluster cluster,
      Optional<String> datacenters) throws ReaperException {

    Set<String> datacentersToRepair;
    if (datacenters.isPresent() && !datacenters.get().isEmpty()) {
      datacentersToRepair = Sets.newHashSet(COMMA_SEPARATED_LIST_SPLITTER.split(datacenters.get()));
    } else {
      datacentersToRepair = Collections.emptySet();
    }
    return datacentersToRepair;
  }

  public static RepairUnit getNewOrExistingRepairUnit(
      AppContext context,
      Cluster cluster,
      String keyspace,
      Set<String> tableNames,
      Boolean incrementalRepair,
      Set<String> nodesToRepair,
      Set<String> datacenters,
      Set<String> blacklistedTables)
      throws ReaperException {

    Optional<RepairUnit> storedRepairUnit =
        context.storage.getRepairUnit(
            cluster.getName(), keyspace, tableNames, nodesToRepair, datacenters, blacklistedTables);
    RepairUnit theRepairUnit;

    Optional<String> cassandraVersion = Optional.absent();

    try (JmxProxy jmxProxy = context.jmxConnectionFactory.connectAny(
        Optional.absent(), cluster.getSeedHosts(), context.config.getJmxConnectionTimeoutInSeconds())) {

      cassandraVersion = Optional.fromNullable(jmxProxy.getCassandraVersion());
    } catch (ReaperException e) {
      LOG.warn("couldn't connect to hosts: {}, life sucks...", cluster.getSeedHosts(), e);
    }

    if (cassandraVersion.isPresent() && cassandraVersion.get().startsWith("2.0") && incrementalRepair) {
      String errMsg = "Incremental repair does not work with Cassandra versions before 2.1";
      LOG.error(errMsg);
      throw new ReaperException(errMsg);
    }

    if (storedRepairUnit.isPresent()
        && storedRepairUnit.get().getIncrementalRepair().equals(incrementalRepair)
        && storedRepairUnit.get().getNodes().equals(nodesToRepair)
        && storedRepairUnit.get().getDatacenters().equals(datacenters)
        && storedRepairUnit.get().getBlacklistedTables().equals(blacklistedTables)
        && storedRepairUnit.get().getColumnFamilies().equals(tableNames)) {
      LOG.info(
          "use existing repair unit for cluster '{}', keyspace '{}', "
              + "column families: {}, nodes: {} and datacenters: {}",
          cluster.getName(),
          keyspace,
          tableNames,
          nodesToRepair,
          datacenters);
      theRepairUnit = storedRepairUnit.get();
    } else {
      LOG.info(
          "create new repair unit for cluster '{}', keyspace '{}', column families: {}, nodes: {} and datacenters: {}",
          cluster.getName(),
          keyspace,
          tableNames,
          nodesToRepair,
          datacenters);
      theRepairUnit =
          context.storage.addRepairUnit(
              new RepairUnit.Builder(
                  cluster.getName(),
                  keyspace,
                  tableNames,
                  incrementalRepair,
                  nodesToRepair,
                  datacenters,
                  blacklistedTables));
    }
    return theRepairUnit;
  }

  @Nullable
  public static String dateTimeToIso8601(
      @Nullable DateTime dateTime) {

    if (null == dateTime) {
      return null;
    }
    return ISODateTimeFormat.dateTimeNoMillis().print(dateTime);
  }

  public static double roundDoubleNicely(double intensity) {
    return Math.round(intensity * 10000f) / 10000f;
  }

  public static Set<String> parseSeedHosts(String seedHost) {
    return Arrays.stream(seedHost.split(",")).map(String::trim).collect(Collectors.toSet());
  }

  /**
   * Applies blacklist filter on tables for the given repair unit.
   *
   * @param coordinator : a JMX proxy instance
   * @param unit : the repair unit for the current run
   * @return the list of tables to repair for the keyspace without the blacklisted ones
   * @throws ReaperException, IllegalStateException
   */
  public static Set<String> getTablesToRepair(JmxProxy coordinator, RepairUnit unit)
      throws ReaperException, IllegalStateException {
    Set<String> tables = unit.getColumnFamilies();

    if (!unit.getBlacklistedTables().isEmpty() && unit.getColumnFamilies().isEmpty()) {
      tables =
          coordinator
              .getTableNamesForKeyspace(unit.getKeyspaceName())
              .stream()
              .filter(tableName -> !unit.getBlacklistedTables().contains(tableName))
              .collect(Collectors.toSet());
    }

    if (!unit.getBlacklistedTables().isEmpty() && !unit.getColumnFamilies().isEmpty()) {
      tables =
          unit.getColumnFamilies()
              .stream()
              .filter(tableName -> !unit.getBlacklistedTables().contains(tableName))
              .collect(Collectors.toSet());
    }

    Preconditions.checkState(
        !(!unit.getBlacklistedTables().isEmpty()
            && tables.isEmpty())); // if we have a blacklist, we should have tables in the output.

    return tables;
  }

}
