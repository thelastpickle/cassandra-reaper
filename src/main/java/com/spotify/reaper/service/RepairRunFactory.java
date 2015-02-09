package com.spotify.reaper.service;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import com.spotify.reaper.AppContext;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.core.RepairUnit;

import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class RepairRunFactory {

  private static final Logger LOG = LoggerFactory.getLogger(RepairRunFactory.class);

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

    // the next step is to prepare a repair run object
    RepairRun repairRun = storeNewRepairRun(context, cluster, repairUnit, cause, owner, segments,
                                            repairParallelism, intensity);
    checkNotNull(repairRun, "failed preparing repair run");

    // Notice that our RepairRun core object doesn't contain pointer to
    // the set of RepairSegments in the run, as they are accessed separately.
    // However, RepairSegment has a pointer to the RepairRun it lives in

    // the last preparation step is to generate actual repair segments
    storeNewRepairSegments(context, tokenSegments, repairRun, repairUnit);

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
                                             RepairRun repairRun, RepairUnit repairUnit)
      throws ReaperException {
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


}
