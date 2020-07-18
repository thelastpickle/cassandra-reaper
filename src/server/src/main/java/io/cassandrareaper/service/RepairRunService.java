/*
 * Copyright 2015-2017 Spotify AB
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

package io.cassandrareaper.service;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.core.Segment;
import io.cassandrareaper.core.Table;
import io.cassandrareaper.jmx.ClusterFacade;
import io.cassandrareaper.jmx.EndpointSnitchInfoProxy;
import io.cassandrareaper.jmx.JmxProxy;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.cassandra.repair.RepairParallelism;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public final class RepairRunService {

  public static final Splitter COMMA_SEPARATED_LIST_SPLITTER
      = Splitter.on(',').trimResults(CharMatcher.anyOf(" ()[]\"'")).omitEmptyStrings();
  public static final int DEFAULT_SEGMENT_COUNT_PER_NODE = 16;

  private static final Logger LOG = LoggerFactory.getLogger(RepairRunService.class);

  private final AppContext context;
  private final RepairUnitService repairUnitService;
  private final ClusterFacade clusterFacade;

  private RepairRunService(AppContext context, Supplier<ClusterFacade> clusterFacadeSupplier) {
    this.context = context;
    this.repairUnitService = RepairUnitService.create(context);
    this.clusterFacade = clusterFacadeSupplier.get();
  }

  @VisibleForTesting
  static RepairRunService create(AppContext context, Supplier<ClusterFacade> supplier) throws ReaperException {
    return new RepairRunService(context, supplier);
  }

  public static RepairRunService create(AppContext context) {
    return new RepairRunService(context, () -> ClusterFacade.create(context));
  }

  /**
   * Creates a repair run but does not start it immediately.
   *
   * <p>Creating a repair run involves: 1) split token range into segments 2) create a RepairRun
   * instance 3) create RepairSegment instances linked to RepairRun.
   *
   * @throws ReaperException if repair run fails to be stored into Reaper's storage.
   */
  public RepairRun registerRepairRun(
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
    List<Segment> tokenSegments = repairUnit.getIncrementalRepair()
            ? Lists.newArrayList()
            : generateSegments(cluster, segments, segmentsPerNode, repairUnit);

    checkNotNull(tokenSegments, "failed generating repair segments");

    Map<String, RingRange> nodes = getClusterNodes(cluster, repairUnit);
    // the next step is to prepare a repair run objec
    segments = repairUnit.getIncrementalRepair() ? nodes.keySet().size() : tokenSegments.size();

    RepairRun.Builder runBuilder = RepairRun.builder(cluster.getName(), repairUnit.getId())
        .intensity(intensity)
        .segmentCount(segments)
        .repairParallelism(repairParallelism)
        .cause(cause.orElse("no cause specified"))
        .owner(owner)
        .tables(repairUnitService.getTablesToRepair(cluster, repairUnit));

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
  @VisibleForTesting
  List<Segment> generateSegments(
      Cluster targetCluster, int segmentCount, int segmentCountPerNode, RepairUnit repairUnit)
      throws ReaperException {

    List<Segment> segments = Lists.newArrayList();

    Preconditions.checkState(
        targetCluster.getPartitioner().isPresent(),
        "no partitioner for cluster: " + targetCluster.getName());

    SegmentGenerator sg = new SegmentGenerator(targetCluster.getPartitioner().get());
    if (targetCluster.getSeedHosts().isEmpty()) {
      String errMsg = String.format("didn't get any seed hosts for cluster \"%s\"", targetCluster.getName());
      LOG.error(errMsg);
      throw new ReaperException(errMsg);
    }

    try {
      List<BigInteger> tokens = clusterFacade.getTokens(targetCluster);
      Map<List<String>, List<String>> rangeToEndpoint
          = clusterFacade.getRangeToEndpointMap(targetCluster, repairUnit.getKeyspaceName());
      Map<String, List<RingRange>> endpointToRange = buildEndpointToRangeMap(rangeToEndpoint);
      Map<List<String>, List<RingRange>> replicasToRange = buildReplicasToRangeMap(rangeToEndpoint);
      String cassandraVersion = clusterFacade.getCassandraVersion(targetCluster);

      int globalSegmentCount = segmentCount;
      if (globalSegmentCount == 0) {
        globalSegmentCount = computeGlobalSegmentCount(segmentCountPerNode, endpointToRange);
      }

      segments = filterSegmentsByNodes(
              sg.generateSegments(
                  globalSegmentCount,
                  tokens,
                  repairUnit.getIncrementalRepair(),
                  replicasToRange,
                  cassandraVersion),
              repairUnit,
              endpointToRange);

    } catch (ReaperException e) {
      LOG.warn("couldn't connect to any host: {}, life sucks...", targetCluster.getSeedHosts(), e);
    }

    if (segments.isEmpty() && !repairUnit.getIncrementalRepair()) {
      String errMsg = String.format("failed to generate repair segments for cluster \"%s\"", targetCluster.getName());
      LOG.error(errMsg);
      throw new ReaperException(errMsg);
    }

    // Compute replicas per DC for each segment
    List<Segment> segmentsWithReplicas = Lists.newArrayList();
    for (Segment segment:segments) {
      segmentsWithReplicas.add(
          Segment.builder()
                 .withBaseRange(segment.getBaseRange())
                 .withTokenRanges(segment.getTokenRanges())
                 .withReplicas(getDCsByNodeForRepairSegment(targetCluster, segment, repairUnit.getKeyspaceName()))
                 .build());
    }

    return segmentsWithReplicas;
  }

  private Map<String, String> getDCsByNodeForRepairSegment(
      Cluster cluster,
      Segment segment,
      String keyspace) throws ReaperException {

    JmxProxy jmxConnection = clusterFacade.connect(cluster);
    // when hosts are coming up or going down, this method can throw an UndeclaredThrowableException
    Collection<String> nodes = clusterFacade.tokenRangeToEndpoint(cluster, keyspace, segment);
    Map<String, String> dcByNode = Maps.newHashMap();
    nodes.forEach(node -> dcByNode.put(node, EndpointSnitchInfoProxy.create(jmxConnection).getDataCenter(node)));
    return dcByNode;
  }

  static int computeGlobalSegmentCount(
      int segmentCountPerNode,
      Map<String, List<RingRange>> endpointToRange) {
    Preconditions.checkArgument(1 <= endpointToRange.keySet().size());

    return endpointToRange.keySet().size()
        * (segmentCountPerNode != 0 ? segmentCountPerNode : DEFAULT_SEGMENT_COUNT_PER_NODE);
  }

  static List<Segment> filterSegmentsByNodes(
      List<Segment> segments,
      RepairUnit repairUnit,
      Map<String, List<RingRange>> endpointToRange) {

    if (repairUnit.getNodes().isEmpty()) {
      return segments;
    } else {
      return segments
          .stream()
          .filter(
              segment -> {
                RingRange firstRange = segment.getBaseRange();
                for (Entry<String, List<RingRange>> entry : endpointToRange.entrySet()) {
                  if (repairUnit.getNodes().contains(entry.getKey())) {
                    for (RingRange range : entry.getValue()) {
                      if (range.encloses(firstRange)) {
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
  static Map<String, List<RingRange>> buildEndpointToRangeMap(Map<List<String>, List<String>> rangeToEndpoint) {
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

  @VisibleForTesting
  static Map<List<String>, List<RingRange>> buildReplicasToRangeMap(
      Map<List<String>, List<String>> rangeToEndpoint) {
    Map<List<String>, List<RingRange>> replicasToRange = Maps.newHashMap();

    for (Entry<List<String>, List<String>> entry : rangeToEndpoint.entrySet()) {
      RingRange range = new RingRange(entry.getKey().toArray(new String[entry.getKey().size()]));
      List<String> sortedReplicas = entry.getValue().stream().sorted().collect(Collectors.toList());

      List<RingRange> ranges = replicasToRange.getOrDefault(sortedReplicas, Lists.newArrayList());
      ranges.add(range);
      replicasToRange.put(sortedReplicas, ranges);

    }

    return replicasToRange;
  }

  /**
   * Creates the repair runs linked to given RepairRun and stores them directly in the storage
   * backend.
   */
  private static List<RepairSegment.Builder> createRepairSegments(
      List<Segment> tokenSegments,
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
            range ->
                repairSegmentBuilders.add(
                    RepairSegment.builder(
                            Segment.builder()
                                .withTokenRanges(Arrays.asList(range.getValue()))
                                .build(),
                            repairUnit.getId())
                        .withCoordinatorHost(range.getKey())));

    return repairSegmentBuilders;
  }

  private Map<String,RingRange> getClusterNodes(Cluster targetCluster, RepairUnit repairUnit) throws ReaperException {

    ConcurrentHashMap<String, RingRange> nodesWithRanges = new ConcurrentHashMap<>();
    if (targetCluster.getSeedHosts().isEmpty()) {
      String errMsg = String.format("didn't get any seed hosts for cluster \"%s\"", targetCluster.getName());
      LOG.error(errMsg);
      throw new ReaperException(errMsg);
    }

    Map<List<String>, List<String>> rangeToEndpoint = Maps.newHashMap();

    try {
      rangeToEndpoint
          = clusterFacade
              .getRangeToEndpointMap(targetCluster, repairUnit.getKeyspaceName());
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

  public Set<String> getTableNamesBasedOnParam(
      Cluster cluster,
      String keyspace,
      Optional<String> tableNamesParam) throws ReaperException {

    Set<String> knownTables;

    knownTables
        = clusterFacade
            .getTablesForKeyspace(cluster, keyspace)
            .stream()
            .map(Table::getName)
            .collect(Collectors.toSet());
    if (knownTables.isEmpty()) {
      LOG.debug("no known tables for keyspace {} in cluster {}", keyspace, cluster.getName());
      throw new IllegalArgumentException("no column families found for keyspace");
    }

    Set<String> tableNames = Collections.emptySet();
    if (tableNamesParam.isPresent() && !tableNamesParam.get().isEmpty()) {
      tableNames = Sets.newHashSet(COMMA_SEPARATED_LIST_SPLITTER.split(tableNamesParam.get()));
      for (String name : tableNames) {
        if (!knownTables.contains(name)) {
          throw new IllegalArgumentException("keyspace doesn't contain a table named \"" + name + "\"");
        }
      }
    }
    return tableNames;
  }

  public Set<String> getNodesToRepairBasedOnParam(
      Cluster cluster,
      Optional<String> nodesToRepairParam) throws ReaperException {

    Set<String> nodesInCluster;

    nodesInCluster = clusterFacade.getEndpointToHostId(cluster).keySet();
    if (nodesInCluster.isEmpty()) {
      LOG.debug("no nodes found in cluster {}", cluster.getName());
      throw new IllegalArgumentException("no nodes found in cluster");
    }

    Set<String> nodesToRepair = Collections.emptySet();
    if (nodesToRepairParam.isPresent() && !nodesToRepairParam.get().isEmpty()) {
      nodesToRepair = Sets.newHashSet(COMMA_SEPARATED_LIST_SPLITTER.split(nodesToRepairParam.get()));
      for (String node : nodesToRepair) {
        if (!nodesInCluster.contains(node)) {
          throw new IllegalArgumentException(
              "cluster \"" + cluster.getName() + "\" doesn't contain a node named \"" + node + "\"");
        }
      }
    }
    return nodesToRepair;
  }

  public static Set<String> getDatacentersToRepairBasedOnParam(Cluster cluster, Optional<String> datacenters) {
    Set<String> datacentersToRepair = Collections.emptySet();
    if (datacenters.isPresent() && !datacenters.get().isEmpty()) {
      datacentersToRepair = Sets.newHashSet(COMMA_SEPARATED_LIST_SPLITTER.split(datacenters.get()));
    }
    return datacentersToRepair;
  }

}
