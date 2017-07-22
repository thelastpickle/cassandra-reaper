package com.spotify.reaper.resources;

import static com.google.common.base.Preconditions.checkNotNull;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.CharMatcher;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.spotify.reaper.AppContext;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSchedule;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.core.RepairUnit;
import com.spotify.reaper.service.RingRange;
import com.spotify.reaper.service.SegmentGenerator;
import java.util.UUID;

public final class CommonTools {

  private static final Logger LOG = LoggerFactory.getLogger(CommonTools.class);

  /**
   * Creates a repair run but does not start it immediately.
   *
   * Creating a repair run involves:
   * 1) split token range into segments
   * 2) create a RepairRun instance
   * 3) create RepairSegment instances linked to RepairRun.
   *
   * @throws com.spotify.reaper.ReaperException if repair run fails to be stored into Reaper's
   * storage.
   */
  public static RepairRun registerRepairRun(AppContext context, Cluster cluster,
                                            RepairUnit repairUnit, Optional<String> cause,
                                            String owner, int segments,
                                            RepairParallelism repairParallelism, Double intensity)
      throws ReaperException {

    // preparing a repair run involves several steps

    // the first step is to generate token segments
    List<RingRange> tokenSegments = generateSegments(context, cluster, segments, repairUnit.getIncrementalRepair());
    checkNotNull(tokenSegments, "failed generating repair segments");

    Map<String, RingRange> nodes = getClusterNodes(context, cluster, repairUnit);
    // the next step is to prepare a repair run object
    segments = repairUnit.getIncrementalRepair() ? nodes.keySet().size() : tokenSegments.size();

    RepairRun.Builder runBuilder
            = createNewRepairRun(cluster, repairUnit, cause, owner, segments, repairParallelism, intensity);

    // the last preparation step is to generate actual repair segments
    List<RepairSegment.Builder> segmentBuilders = repairUnit.getIncrementalRepair()
            ? createRepairSegmentsForIncrementalRepair(nodes, repairUnit)
            : createRepairSegments(tokenSegments, repairUnit);

    RepairRun repairRun = context.storage.addRepairRun(runBuilder, segmentBuilders);

    if (null == repairRun){
      String errMsg = String.format(
              "failed storing repair run for cluster \"%s\", keyspace \"%s\", and column families: %s",
              cluster.getName(),
              repairUnit.getKeyspaceName(),
              repairUnit.getColumnFamilies());

      LOG.error(errMsg);
      throw new ReaperException(errMsg);
    }
    return repairRun;
  }

  /**
   * Splits a token range for given table into segments
 * @param incrementalRepair
   *
   * @return the created segments
   * @throws ReaperException when fails to discover seeds for the cluster or fails to connect to
   * any of the nodes in the Cluster.
   */
  private static List<RingRange> generateSegments(AppContext context, Cluster targetCluster,
                                                  int segmentCount, Boolean incrementalRepair)
      throws ReaperException {
    List<RingRange> segments = null;
    Preconditions.checkState(targetCluster.getPartitioner() != null,
        "no partitioner for cluster: " + targetCluster.getName());
    SegmentGenerator sg = new SegmentGenerator(targetCluster.getPartitioner());
    Set<String> seedHosts = targetCluster.getSeedHosts();
    if (seedHosts.isEmpty()) {
      String errMsg = String.format("didn't get any seed hosts for cluster \"%s\"",
                                    targetCluster.getName());
      LOG.error(errMsg);
      throw new ReaperException(errMsg);
    }

    try (JmxProxy jmxProxy = context.jmxConnectionFactory
            .connectAny(Optional.absent(), seedHosts, context.config.getJmxConnectionTimeoutInSeconds())) {

      List<BigInteger> tokens = jmxProxy.getTokens();
      segments = sg.generateSegments(segmentCount, tokens, incrementalRepair);
    } catch (ReaperException e) {
      LOG.warn("couldn't connect to any host: {}, life sucks...", seedHosts, e);
    }

    if (segments == null) {
      String errMsg = String.format("failed to generate repair segments for cluster \"%s\"",
                                    targetCluster.getName());
      LOG.error(errMsg);
      throw new ReaperException(errMsg);
    }
    return segments;
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
          Double intensity) throws ReaperException {

    return new RepairRun.Builder(cluster.getName(), repairUnit.getId(), DateTime.now(), intensity, segments, repairParallelism)
            .cause(cause.isPresent() ? cause.get() : "no cause specified")
            .owner(owner);
  }

  /**
   * Creates the repair runs linked to given RepairRun and stores them directly in the storage
   * backend.
   */
  private static List<RepairSegment.Builder> createRepairSegments(List<RingRange> tokenSegments, RepairUnit repairUnit){

    List<RepairSegment.Builder> repairSegmentBuilders = Lists.newArrayList();
    tokenSegments.forEach(range -> repairSegmentBuilders.add(new RepairSegment.Builder(range, repairUnit.getId())));
    return repairSegmentBuilders;
  }


  /**
   * Creates the repair runs linked to given RepairRun and stores them directly in the storage
   * backend in case of incrementalRepair
   */
  private static List<RepairSegment.Builder> createRepairSegmentsForIncrementalRepair(
          Map<String, RingRange> nodes,
          RepairUnit repairUnit) {

    List<RepairSegment.Builder> repairSegmentBuilders = Lists.newArrayList();

    nodes.entrySet().forEach(range
            -> repairSegmentBuilders.add(
                new RepairSegment.Builder(range.getValue(), repairUnit.getId()).coordinatorHost(range.getKey())));

    return repairSegmentBuilders;
  }

  private static Map<String, RingRange> getClusterNodes(AppContext context,  Cluster targetCluster, RepairUnit repairUnit) throws ReaperException {
    ConcurrentHashMap<String, RingRange> nodesWithRanges = new ConcurrentHashMap<>();
    Set<String> seedHosts = targetCluster.getSeedHosts();
    if (seedHosts.isEmpty()) {
      String errMsg = String.format("didn't get any seed hosts for cluster \"%s\"", targetCluster.getName());
      LOG.error(errMsg);
      throw new ReaperException(errMsg);
    }

    Map<List<String>, List<String>> rangeToEndpoint = Maps.newHashMap();

    try (JmxProxy jmxProxy = context.jmxConnectionFactory
            .connectAny(Optional.absent(), seedHosts, context.config.getJmxConnectionTimeoutInSeconds())) {

      rangeToEndpoint = jmxProxy.getRangeToEndpointMap(repairUnit.getKeyspaceName());
    } catch (ReaperException e) {
        LOG.error("couldn't connect to any host: {}, will try next one", e);
        throw new ReaperException(e);
    }

    for (Entry<List<String>, List<String>> tokenRangeToEndpoint:rangeToEndpoint.entrySet()) {
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
      int segments,
      RepairParallelism repairParallelism,
      Double intensity)
      throws ReaperException {
    RepairSchedule.Builder scheduleBuilder =
        new RepairSchedule.Builder(repairUnit.getId(), RepairSchedule.State.ACTIVE, daysBetween,
                                   nextActivation, ImmutableList.<UUID>of(), segments,
                                   repairParallelism, intensity,
                                   DateTime.now());
    scheduleBuilder.owner(owner);

    Collection<RepairSchedule> repairSchedules = context.storage.getRepairSchedulesForClusterAndKeyspace(repairUnit.getClusterName(), repairUnit.getKeyspaceName());
    for(RepairSchedule sched:repairSchedules){
      Optional<RepairUnit> repairUnitForSched = context.storage.getRepairUnit(sched.getRepairUnitId());
      if(repairUnitForSched.isPresent() && repairUnitForSched.get().getClusterName().equals(repairUnit.getClusterName()) && repairUnitForSched.get().getKeyspaceName().equals(repairUnit.getKeyspaceName())){
        if(CommonTools.aConflictingScheduleAlreadyExists(repairUnitForSched.get(), repairUnit)){
          String errMsg = String.format("A repair schedule already exists for cluster \"%s\", "
                    + "keyspace \"%s\", and column families: %s",
                    cluster.getName(), repairUnit.getKeyspaceName(),
                    Sets.intersection(repairUnit.getColumnFamilies(),repairUnitForSched.get().getColumnFamilies()));
            LOG.error(errMsg);
          throw new ReaperException(errMsg);
        }
      }
    }

    RepairSchedule newRepairSchedule = context.storage.addRepairSchedule(scheduleBuilder);
    if (newRepairSchedule == null) {
      String errMsg = String.format("failed storing repair schedule for cluster \"%s\", "
                                    + "keyspace \"%s\", and column families: %s",
                                    cluster.getName(), repairUnit.getKeyspaceName(),
                                    repairUnit.getColumnFamilies());
      LOG.error(errMsg);
      throw new ReaperException(errMsg);
    }
    return newRepairSchedule;
  }

  private static final boolean aConflictingScheduleAlreadyExists(RepairUnit newRepairUnit, RepairUnit existingRepairUnit){
    return (newRepairUnit.getColumnFamilies().isEmpty() && existingRepairUnit.getColumnFamilies().isEmpty())
        || newRepairUnit.getColumnFamilies().isEmpty() && !existingRepairUnit.getColumnFamilies().isEmpty()
        || !newRepairUnit.getColumnFamilies().isEmpty() && existingRepairUnit.getColumnFamilies().isEmpty()
        || !Sets.intersection(existingRepairUnit.getColumnFamilies(),newRepairUnit.getColumnFamilies()).isEmpty();

  }

  public static final Splitter COMMA_SEPARATED_LIST_SPLITTER =
      Splitter.on(',').trimResults(CharMatcher.anyOf(" ()[]\"'")).omitEmptyStrings();

  public static Set<String> getTableNamesBasedOnParam(
      AppContext context, Cluster cluster, String keyspace, Optional<String> tableNamesParam)
      throws ReaperException {
    Set<String> knownTables;
    try (JmxProxy jmxProxy = context.jmxConnectionFactory.connectAny(cluster, context.config.getJmxConnectionTimeoutInSeconds())) {
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
          throw new IllegalArgumentException("keyspace doesn't contain a table named \""
                                             + name + "\"");
        }
      }
    } else {
      tableNames = Collections.emptySet();
    }
    return tableNames;
  }

  public static RepairUnit getNewOrExistingRepairUnit(AppContext context, Cluster cluster,
                                                      String keyspace, Set<String> tableNames, Boolean incrementalRepair) throws ReaperException {
    Optional<RepairUnit> storedRepairUnit =
        context.storage.getRepairUnit(cluster.getName(), keyspace, tableNames);
    RepairUnit theRepairUnit;

    Optional<String> cassandraVersion = Optional.absent();

    try (JmxProxy jmxProxy = context.jmxConnectionFactory
            .connectAny(Optional.absent(), cluster.getSeedHosts(), context.config.getJmxConnectionTimeoutInSeconds())) {

      cassandraVersion = Optional.fromNullable(jmxProxy.getCassandraVersion());
    } catch (ReaperException e) {
      LOG.warn("couldn't connect to hosts: {}, life sucks...", cluster.getSeedHosts(), e);
    }

    if(cassandraVersion.isPresent() && cassandraVersion.get().startsWith("2.0") && incrementalRepair){
      String errMsg = "Incremental repair does not work with Cassandra versions before 2.1";
      LOG.error(errMsg);
      throw new ReaperException(errMsg);
    }

    if (storedRepairUnit.isPresent() && storedRepairUnit.get().getIncrementalRepair().equals(incrementalRepair)) {
      LOG.info("use existing repair unit for cluster '{}', keyspace '{}', and column families: {}",
               cluster.getName(), keyspace, tableNames);
      theRepairUnit = storedRepairUnit.get();
    } else {
      LOG.info("create new repair unit for cluster '{}', keyspace '{}', and column families: {}",
               cluster.getName(), keyspace, tableNames);
      theRepairUnit = context.storage.addRepairUnit(
          new RepairUnit.Builder(cluster.getName(), keyspace, tableNames, incrementalRepair));
    }
    return theRepairUnit;
  }

  @Nullable
  public static String dateTimeToISO8601(@Nullable DateTime dateTime) {
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

  private CommonTools(){}

}
