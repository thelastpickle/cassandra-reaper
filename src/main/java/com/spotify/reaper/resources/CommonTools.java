package com.spotify.reaper.resources;

import static com.google.common.base.Preconditions.checkNotNull;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.CharMatcher;
import com.google.common.base.Optional;
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

public class CommonTools {

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
    List<RingRange> tokenSegments = generateSegments(context, cluster, segments);
    checkNotNull(tokenSegments, "failed generating repair segments");

    Map<String, RingRange> nodes = getClusterNodes(context, cluster, repairUnit);
    // the next step is to prepare a repair run object
    RepairRun repairRun = storeNewRepairRun(context, cluster, repairUnit, cause, owner, nodes.keySet().size(),
                                            repairParallelism, intensity);
    checkNotNull(repairRun, "failed preparing repair run");

    // Notice that our RepairRun core object doesn't contain pointer to
    // the set of RepairSegments in the run, as they are accessed separately.
    // However, RepairSegment has a pointer to the RepairRun it lives in

    // the last preparation step is to generate actual repair segments
    if(!repairUnit.getIncrementalRepair()) {
    	storeNewRepairSegments(context, tokenSegments, repairRun, repairUnit);
    } else {
    	storeNewRepairSegmentsForIncrementalRepair(context, nodes, repairRun, repairUnit);
    }

    // now we're done and can return
    return repairRun;
  }

  /**
   * Splits a token range for given table into segments
   *
   * @return the created segments
   * @throws ReaperException when fails to discover seeds for the cluster or fails to connect to
   * any of the nodes in the Cluster.
   */
  private static List<RingRange> generateSegments(AppContext context, Cluster targetCluster,
                                                  int segmentCount)
      throws ReaperException {
    List<RingRange> segments = null;
    assert targetCluster.getPartitioner() != null :
        "no partitioner for cluster: " + targetCluster.getName();
    SegmentGenerator sg = new SegmentGenerator(targetCluster.getPartitioner());
    Set<String> seedHosts = targetCluster.getSeedHosts();
    if (seedHosts.isEmpty()) {
      String errMsg = String.format("didn't get any seed hosts for cluster \"%s\"",
                                    targetCluster.getName());
      LOG.error(errMsg);
      throw new ReaperException(errMsg);
    }
    for (String host : seedHosts) {
      try (JmxProxy jmxProxy = context.jmxConnectionFactory.connect(host)) {
        List<BigInteger> tokens = jmxProxy.getTokens();
        segments = sg.generateSegments(segmentCount, tokens);
        break;
      } catch (ReaperException e) {
        LOG.warn("couldn't connect to host: {}, will try next one", host);
      }
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
  private static RepairRun storeNewRepairRun(AppContext context, Cluster cluster,
                                             RepairUnit repairUnit, Optional<String> cause,
                                             String owner, int segments,
                                             RepairParallelism repairParallelism, Double intensity)
      throws ReaperException {
    RepairRun.Builder runBuilder = new RepairRun.Builder(cluster.getName(), repairUnit.getId(),
                                                         DateTime.now(), intensity,
                                                         segments, repairParallelism);
    runBuilder.cause(cause.isPresent() ? cause.get() : "no cause specified");
    runBuilder.owner(owner);
    RepairRun newRepairRun = context.storage.addRepairRun(runBuilder);
    if (newRepairRun == null) {
      String errMsg = String.format("failed storing repair run for cluster \"%s\", "
                                    + "keyspace \"%s\", and column families: %s",
                                    cluster.getName(), repairUnit.getKeyspaceName(),
                                    repairUnit.getColumnFamilies());
      LOG.error(errMsg);
      throw new ReaperException(errMsg);
    }
    return newRepairRun;
  }

  /**
   * Creates the repair runs linked to given RepairRun and stores them directly in the storage
   * backend.
   */
  private static void storeNewRepairSegments(AppContext context, List<RingRange> tokenSegments,
                                             RepairRun repairRun, RepairUnit repairUnit) {
    List<RepairSegment.Builder> repairSegmentBuilders = Lists.newArrayList();
    for (RingRange range : tokenSegments) {
      RepairSegment.Builder repairSegment = new RepairSegment.Builder(repairRun.getId(), range,
                                                                      repairUnit.getId());
      repairSegmentBuilders.add(repairSegment);
    }
    context.storage.addRepairSegments(repairSegmentBuilders, repairRun.getId());
    if (repairRun.getSegmentCount() != tokenSegments.size()) {
      LOG.debug("created segment amount differs from expected default {} != {}",
                repairRun.getSegmentCount(), tokenSegments.size());
      context.storage.updateRepairRun(
          repairRun.with().segmentCount(tokenSegments.size()).build(repairRun.getId()));
    }
  }
  
  
  /**
   * Creates the repair runs linked to given RepairRun and stores them directly in the storage
   * backend in case of incrementalRepair
   */
  private static void storeNewRepairSegmentsForIncrementalRepair(AppContext context, Map<String, RingRange> nodes,
                                             RepairRun repairRun, RepairUnit repairUnit) {
    List<RepairSegment.Builder> repairSegmentBuilders = Lists.newArrayList();
    for (Entry<String, RingRange> range : nodes.entrySet()) {
      RepairSegment.Builder repairSegment = new RepairSegment.Builder(repairRun.getId(), range.getValue(),
                                                                      repairUnit.getId());
      repairSegment.coordinatorHost(range.getKey());
      repairSegmentBuilders.add(repairSegment);
    }
    context.storage.addRepairSegments(repairSegmentBuilders, repairRun.getId());
    if (repairRun.getSegmentCount() != nodes.keySet().size()) {
      LOG.debug("created segment amount differs from expected default {} != {}",
                repairRun.getSegmentCount(), nodes.keySet().size());
      context.storage.updateRepairRun(
          repairRun.with().segmentCount(nodes.keySet().size()).build(repairRun.getId()));
    }
  }
  
  private static Map<String, RingRange> getClusterNodes(AppContext context,  Cluster targetCluster, RepairUnit repairUnit) throws ReaperException {
	  Set<String> nodes = Sets.newHashSet();
	  ConcurrentHashMap<String, RingRange> nodesWithRanges = new ConcurrentHashMap<String, RingRange>();
	  Set<String> seedHosts = targetCluster.getSeedHosts();
	    if (seedHosts.isEmpty()) {
	      String errMsg = String.format("didn't get any seed hosts for cluster \"%s\"",
	                                    targetCluster.getName());
	      LOG.error(errMsg);
	      throw new ReaperException(errMsg);
	    }
	   
	    
	    Map<List<String>, List<String>> rangeToEndpoint = Maps.newHashMap();
	    for (String host : seedHosts) {
	      try (JmxProxy jmxProxy = context.jmxConnectionFactory.connect(host)) {
	        rangeToEndpoint = jmxProxy.getRangeToEndpointMap(repairUnit.getKeyspaceName());	        
	        break;
	      } catch (ReaperException e) {
	        LOG.warn("couldn't connect to host: {}, will try next one", host);
	      }
	    }
	    
	  for(Entry<List<String>, List<String>> tokenRangeToEndpoint:rangeToEndpoint.entrySet()) {
		  String node = tokenRangeToEndpoint.getValue().get(0);
		  RingRange range = new RingRange(tokenRangeToEndpoint.getKey().get(0), tokenRangeToEndpoint.getKey().get(1));
		  RingRange added = nodesWithRanges.putIfAbsent(node, range);			
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
                                   nextActivation, ImmutableList.<Long>of(), segments,
                                   repairParallelism, intensity,
                                   DateTime.now());
    scheduleBuilder.owner(owner);
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

  public static final Splitter COMMA_SEPARATED_LIST_SPLITTER =
      Splitter.on(',').trimResults(CharMatcher.anyOf(" ()[]\"'")).omitEmptyStrings();

  public static Set<String> getTableNamesBasedOnParam(
      AppContext context, Cluster cluster, String keyspace, Optional<String> tableNamesParam)
      throws ReaperException {
    Set<String> knownTables;
    try (JmxProxy jmxProxy = context.jmxConnectionFactory.connectAny(cluster)) {
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
                                                      String keyspace, Set<String> tableNames, Boolean incrementalRepair) {
    Optional<RepairUnit> storedRepairUnit =
        context.storage.getRepairUnit(cluster.getName(), keyspace, tableNames);
    RepairUnit theRepairUnit;
    if (storedRepairUnit.isPresent()) {
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

}
