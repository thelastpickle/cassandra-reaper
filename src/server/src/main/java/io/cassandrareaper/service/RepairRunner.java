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

package io.cassandrareaper.service;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration.DatacenterAvailability;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.jmx.ClusterFacade;
import io.cassandrareaper.storage.IDistributedStorage;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.Collectors;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class RepairRunner implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(RepairRunner.class);

  private final AppContext context;
  private final ClusterFacade clusterFacade;
  private final UUID repairRunId;
  private final String clusterName;
  private final AtomicReferenceArray<UUID> currentlyRunningSegments;
  private final List<RingRange> parallelRanges;
  private final String metricNameForMillisSinceLastRepairPerKeyspace;
  private final String metricNameForMillisSinceLastRepair;
  private final Cluster cluster;
  private float repairProgress;
  private float segmentsDone;
  private float segmentsTotal;
  private final List<RingRange> localEndpointRanges;

  private RepairRunner(
      AppContext context,
      UUID repairRunId,
      ClusterFacade clusterFacade) throws ReaperException {

    LOG.debug("Creating RepairRunner for run with ID {}", repairRunId);
    this.context = context;
    this.clusterFacade = clusterFacade;
    this.repairRunId = repairRunId;
    Optional<RepairRun> repairRun = context.storage.getRepairRun(repairRunId);
    assert repairRun.isPresent() : "No RepairRun with ID " + repairRunId + " found from storage";
    this.cluster = context.storage.getCluster(repairRun.get().getClusterName());
    RepairUnit repairUnitOpt = context.storage.getRepairUnit(repairRun.get().getRepairUnitId());
    this.clusterName = cluster.getName();
    String keyspace = repairUnitOpt.getKeyspaceName();

    int parallelRepairs = getPossibleParallelRepairsCount(
            clusterFacade.getRangeToEndpointMap(cluster, keyspace),
            clusterFacade.getEndpointToHostId(cluster),
            context.config.getDatacenterAvailability());

    if (repairUnitOpt.getIncrementalRepair()) {
      // with incremental repair, can't have more parallel repairs than nodes
      // Same goes for local mode
      parallelRepairs = 1;
    }
    currentlyRunningSegments = new AtomicReferenceArray(parallelRepairs);
    for (int i = 0; i < parallelRepairs; i++) {
      currentlyRunningSegments.set(i, null);
    }

    Collection<RepairSegment> repairSegments = context.storage.getRepairSegmentsForRun(repairRunId);

    parallelRanges = getParallelRanges(
            parallelRepairs,
            Lists.newArrayList(
                Collections2.transform(
                    repairSegments, segment -> segment.getTokenRange().getBaseRange())));

    localEndpointRanges = context.config.isInSidecarMode()
        ? clusterFacade.getRangesForLocalEndpoint(cluster, repairUnitOpt.getKeyspaceName())
        : Collections.emptyList();

    String repairUnitClusterName = repairUnitOpt.getClusterName();
    String repairUnitKeyspaceName = repairUnitOpt.getKeyspaceName();

    // below four metric names are duplicated, so monitoring systems can follow per cluster or per cluster and keyspace
    String metricNameForRepairProgressPerKeyspace
        = metricName(
            "repairProgress",
            repairUnitClusterName,
            repairUnitKeyspaceName,
            repairRun.get().getRepairUnitId());

    String metricNameForRepairProgress
        = metricName("repairProgress", repairUnitClusterName, repairRun.get().getRepairUnitId());

    registerMetric(metricNameForRepairProgressPerKeyspace, (Gauge<Float>) ()  -> repairProgress);
    registerMetric(metricNameForRepairProgress, (Gauge<Float>) ()  -> repairProgress);

    metricNameForMillisSinceLastRepairPerKeyspace
      = metricName(
            "millisSinceLastRepair",
            repairUnitClusterName,
            repairUnitKeyspaceName,
            repairRun.get().getRepairUnitId());

    metricNameForMillisSinceLastRepair
      = metricName(
            "millisSinceLastRepair", repairUnitClusterName, repairRun.get().getRepairUnitId());

    String metricNameForDoneSegmentsPerKeyspace
        = metricName("segmentsDone", repairUnitClusterName, repairUnitKeyspaceName, repairRun.get().getRepairUnitId());

    String metricNameForDoneSegments
        = metricName("segmentsDone", repairUnitClusterName, repairRun.get().getRepairUnitId());

    registerMetric(metricNameForDoneSegmentsPerKeyspace, (Gauge<Float>) ()  -> segmentsDone);
    registerMetric(metricNameForDoneSegments, (Gauge<Integer>) ()  -> (int)segmentsDone);

    String metricNameForTotalSegmentsPerKeyspace
        = metricName("segmentsTotal", repairUnitClusterName, repairUnitKeyspaceName, repairRun.get().getRepairUnitId());

    String metricNameForTotalSegments
        = metricName("segmentsTotal", repairUnitClusterName, repairRun.get().getRepairUnitId());


    registerMetric(metricNameForTotalSegmentsPerKeyspace, (Gauge<Integer>) ()  -> (int)segmentsTotal);
    registerMetric(metricNameForTotalSegments, (Gauge<Float>) ()  -> segmentsTotal);
  }

  public static RepairRunner create(
      AppContext context,
      UUID repairRunId,
      ClusterFacade clusterFacade) throws ReaperException {

    return new RepairRunner(context, repairRunId, clusterFacade);
  }

  private void registerMetric(String metricName, Gauge<?> gauge) {
    if (!context.metricRegistry.getMetrics().containsKey(metricName)) {
      context.metricRegistry.register(metricName, gauge);
    }
  }

  UUID getRepairRunId() {
    return repairRunId;
  }

  static int getPossibleParallelRepairsCount(
      Map<List<String>, List<String>> ranges,
      Map<String, String> hostsInRing,
      DatacenterAvailability datacenterAvailability) throws ReaperException {

    if (ranges.isEmpty()) {
      String msg = "Repairing 0-sized cluster.";
      LOG.error(msg);
      throw new ReaperException(msg);
    }

    if (DatacenterAvailability.SIDECAR == datacenterAvailability) {
      // only 1 segment can be repaired by each instance at once in sidecar mode
      return 1;
    }

    LOG.debug(
        "Possible parallel repairs : {}",
        Math.min(
            Math.max(1, ranges.size() / ranges.values().iterator().next().size()),
            Math.max(1, hostsInRing.keySet().size() / ranges.values().iterator().next().size())));
    return Math.min(
        Math.max(1, ranges.size() / ranges.values().iterator().next().size()),
        Math.max(1, hostsInRing.keySet().size() / ranges.values().iterator().next().size()));
  }

  static List<RingRange> getParallelRanges(int parallelRepairs, List<RingRange> segments) throws ReaperException {
    if (parallelRepairs == 0) {
      String msg = "Can't repair anything with 0 threads";
      LOG.error(msg);
      throw new ReaperException(msg);
    }

    Collections.sort(segments, RingRange.START_COMPARATOR);

    List<RingRange> parallelRanges = Lists.newArrayList();
    for (int i = 0; i < parallelRepairs - 1; i++) {
      parallelRanges.add(
          new RingRange(
              segments.get(i * segments.size() / parallelRepairs).getStart(),
              segments.get((i + 1) * segments.size() / parallelRepairs).getStart()));
    }
    parallelRanges.add(
        new RingRange(
            segments.get((parallelRepairs - 1) * segments.size() / parallelRepairs).getStart(),
            segments.get(0).getStart()));

    LOG.debug("Parallel ranges : {}", parallelRanges);

    return parallelRanges;
  }

  /**
   * Starts/resumes a repair run that is supposed to run.
   */
  @Override
  public void run() {
    Thread.currentThread().setName(clusterName + ":" + repairRunId);

    try {
      Optional<RepairRun> repairRun = context.storage.getRepairRun(repairRunId);
      if ((!repairRun.isPresent() || repairRun.get().getRunState().isTerminated())) {
        // this might happen if a run is deleted while paused etc.
        LOG.warn("RepairRun \"{}\" does not exist. Killing RepairRunner for this run instance.", repairRunId);
        killAndCleanupRunner();
        return;
      }

      RepairRun.RunState state = repairRun.get().getRunState();
      LOG.debug("run() called for repair run #{} with run state {}", repairRunId, state);
      switch (state) {
        case NOT_STARTED:
          start();
          break;
        case RUNNING:
          startNextSegment();
          // We're updating the node list of the cluster at the start of each new run.
          // Helps keeping up with topology changes.
          updateClusterNodeList();
          break;
        case PAUSED:
          context.repairManager.scheduleRetry(this);
          break;
        default:
          throw new IllegalStateException("un-known/implemented state " + state);
      }
    } catch (RuntimeException | ReaperException | InterruptedException e) {
      LOG.error("RepairRun FAILURE, scheduling retry", e);
      context.repairManager.scheduleRetry(this);
    }
    // Adding this here to catch a deadlock
    LOG.debug("run() exiting for repair run #{}", repairRunId);
  }

  /**
   * Starts the repair run.
   */
  private void start() throws ReaperException, InterruptedException {
    LOG.info("Repairs for repair run #{} starting", repairRunId);
    synchronized (this) {
      RepairRun repairRun = context.storage.getRepairRun(repairRunId).get();
      context.storage.updateRepairRun(
          repairRun.with().runState(RepairRun.RunState.RUNNING).startTime(DateTime.now()).build(repairRun.getId()));
    }

    startNextSegment();
  }

  /**
   * Updates the list of nodes in storage for the cluster that's being repaired.
   *
   * @throws ReaperException Thrown in case the cluster cannot be found in storage
   */
  private void updateClusterNodeList() throws ReaperException {
    Set<String> liveNodes  = ImmutableSet.copyOf(clusterFacade.getLiveNodes(cluster));
    Cluster cluster = context.storage.getCluster(clusterName);
    // Note that the seed hosts only get updated if enableDynamicSeedList is true. This is
    // consistent with the logic in ClusterResource.findClusterWithSeedHost.
    if (context.config.getEnableDynamicSeedList() && !cluster.getSeedHosts().equals(liveNodes)
            && !liveNodes.isEmpty()) {
      // Updating storage only if the seed lists has changed
      LOG.info("Updating the seed list for cluster {} as topology changed since the last repair.", clusterName);
      context.storage.updateCluster(cluster.with().withSeedHosts(liveNodes).build());
    }
  }

  private void endRepairRun() {
    LOG.info("Repairs for repair run #{} done", repairRunId);
    synchronized (this) {
      // if the segment has been removed ignore. should only happen in tests on backends that delete repair segments.
      Optional<RepairRun> repairRun = context.storage.getRepairRun(repairRunId);
      if (repairRun.isPresent()) {
        DateTime repairRunCompleted = DateTime.now();

        context.storage.updateRepairRun(
            repairRun.get()
                .with()
                .runState(RepairRun.RunState.DONE)
                .endTime(repairRunCompleted)
                .lastEvent("All done")
                .build(repairRun.get().getId()));

        killAndCleanupRunner();

        context.metricRegistry.remove(metricNameForMillisSinceLastRepairPerKeyspace);
        context.metricRegistry.remove(metricNameForMillisSinceLastRepair);

        context.metricRegistry.register(
            metricNameForMillisSinceLastRepairPerKeyspace,
            (Gauge<Long>) () -> DateTime.now().getMillis() - repairRunCompleted.toInstant().getMillis());

        context.metricRegistry.register(
            metricNameForMillisSinceLastRepair,
            (Gauge<Long>) () -> DateTime.now().getMillis() - repairRunCompleted.toInstant().getMillis());
      }
    }
  }

  /**
   * Get the next segment and repair it. If there is none, we're done.
   */
  private void startNextSegment() throws ReaperException, InterruptedException {
    boolean scheduleRetry = true;
    boolean anythingRunningStill = false;

    // We want to know whether a repair was started,
    // so that a rescheduling of this runner will happen.
    boolean repairStarted = false;

    for (int rangeIndex = 0; rangeIndex < currentlyRunningSegments.length(); rangeIndex++) {

      if (currentlyRunningSegments.get(rangeIndex) != null) {
        anythingRunningStill = true;

        // Just checking that no currently running segment runner is stuck.
        RepairSegment supposedlyRunningSegment
            = context.storage.getRepairSegment(repairRunId, currentlyRunningSegments.get(rangeIndex)).get();
        DateTime startTime = supposedlyRunningSegment.getStartTime();
        if (startTime != null && startTime.isBefore(DateTime.now().minusDays(1))) {
          LOG.warn(
              "Looks like segment #{} has been running more than a day. Start time: {}",
              supposedlyRunningSegment.getId(),
              supposedlyRunningSegment.getStartTime());
        } else if (startTime != null && startTime.isBefore(DateTime.now().minusHours(1))) {
          LOG.info(
              "Looks like segment #{} has been running more than an hour. Start time: {}",
              supposedlyRunningSegment.getId(),
              supposedlyRunningSegment.getStartTime());
        } else if (startTime != null && startTime.isBefore(DateTime.now().minusMinutes(2))) {
          LOG.debug(
              "Looks like segment #{} has been running more than two minutes. Start time: {}",
              supposedlyRunningSegment.getId(),
              supposedlyRunningSegment.getStartTime());
        }
        // No need to try starting new repair for already active slot.
        continue;
      }

      // We have an empty slot, so let's start new segment runner if possible.
      // When in sidecar mode, filter on ranges that the local node is a replica for only.
      LOG.info("Running segment for range {}", parallelRanges.get(rangeIndex));
      Optional<RepairSegment> nextRepairSegment
          = context.config.isInSidecarMode()
              ? ((IDistributedStorage) context.storage)
                  .getNextFreeSegmentForRanges(
                      repairRunId, Optional.of(parallelRanges.get(rangeIndex)), localEndpointRanges)
              : context.storage.getNextFreeSegmentInRange(
                  repairRunId, Optional.of(parallelRanges.get(rangeIndex)));

      if (!nextRepairSegment.isPresent()) {
        LOG.debug("No repair segment available for range {}", parallelRanges.get(rangeIndex));

      } else {
        LOG.info("Next segment to run : {}", nextRepairSegment.get().getId());
        UUID segmentId = nextRepairSegment.get().getId();
        boolean wasSet = currentlyRunningSegments.compareAndSet(rangeIndex, null, segmentId);
        if (!wasSet) {
          LOG.debug("Didn't set segment id `{}` to slot {} because it was busy", segmentId, rangeIndex);
        } else {
          LOG.debug("Did set segment id `{}` to slot {}", segmentId, rangeIndex);
          scheduleRetry = repairSegment(
                  rangeIndex,
                  nextRepairSegment.get().getId(),
                  nextRepairSegment.get());
          if (!scheduleRetry) {
            break;
          }
          segmentsTotal = context.storage.getSegmentAmountForRepairRun(repairRunId);
          repairStarted = true;
        }
      }
    }

    if (!repairStarted && !anythingRunningStill) {
      segmentsDone = context.storage.getSegmentAmountForRepairRunWithState(repairRunId, RepairSegment.State.DONE);
      segmentsTotal = context.storage.getSegmentAmountForRepairRun(repairRunId);

      LOG.info("Repair amount done {}", segmentsDone);
      repairProgress = segmentsDone / segmentsTotal;

      if (segmentsDone == segmentsTotal) {
        endRepairRun();
        scheduleRetry = false;
      }
    } else {
      segmentsDone = context.storage.getSegmentAmountForRepairRunWithState(repairRunId, RepairSegment.State.DONE);
    }

    if (scheduleRetry) {
      context.repairManager.scheduleRetry(this);
    }
  }

  /**
   * Start the repair of a segment.
   *
   * @param segmentId id of the segment to repair.
   * @param segment token range of the segment to repair.
   * @return Boolean indicating whether rescheduling next run is needed.
   * @throws ReaperException any runtime exception we caught in the execution
   */
  private boolean repairSegment(final int rangeIndex, final UUID segmentId, RepairSegment segment)
      throws InterruptedException, ReaperException {

    RepairRun repairRun;
    final UUID unitId;
    final double intensity;
    final RepairParallelism validationParallelism;
    {
      repairRun = context.storage.getRepairRun(repairRunId).get();
      unitId = repairRun.getRepairUnitId();
      intensity = repairRun.getIntensity();
      validationParallelism = repairRun.getRepairParallelism();

      int amountDone = context.storage.getSegmentAmountForRepairRunWithState(repairRunId, RepairSegment.State.DONE);
      repairProgress = (float) amountDone / repairRun.getSegmentCount();
    }

    RepairUnit repairUnit = context.storage.getRepairUnit(unitId);
    repairRun = fixMissingRepairRunTables(repairRun, repairUnit);
    String keyspace = repairUnit.getKeyspaceName();
    LOG.debug("preparing to repair segment {} on run with id {}", segmentId, repairRunId);

    List<String> potentialCoordinators;
    Set<String> segmentReplicas = segment.getReplicas().keySet();
    if (!repairUnit.getIncrementalRepair()) {
      // full repair
      try {
        potentialCoordinators = filterPotentialCoordinatorsByDatacenters(
                repairUnit.getDatacenters(),
                clusterFacade.tokenRangeToEndpoint(cluster, keyspace, segment.getTokenRange()));

      } catch (RuntimeException e) {
        LOG.warn("Couldn't get token ranges from coordinator: #{}", e);
        return true;
      }
      if (potentialCoordinators.isEmpty()) {
        LOG.warn(
            "Segment #{} is faulty, no potential coordinators for range: [{}, {}]",
            segmentId,
            segment.getStartToken(), segment.getEndToken());
        // This segment has a faulty token range. Abort the entire repair run.
        synchronized (this) {
          context.storage.updateRepairRun(
              context.storage.getRepairRun(repairRunId).get()
                  .with()
                  .runState(RepairRun.RunState.ERROR)
                  .lastEvent(
                      String.format(
                          "No coordinators for range %s - %s",
                          segment.getStartToken(),
                          segment.getEndToken()))
                  .endTime(DateTime.now())
                  .build(repairRunId));

          killAndCleanupRunner();
        }

        return false;
      } else if (
          !Sets.difference(
            segmentReplicas,
            potentialCoordinators.stream().collect(Collectors.toSet())).isEmpty()) {
        LOG.warn(
            "Segment #{} is faulty, replica set changed since repair was started: [{}, {}]",
            segmentId,
            segment.getStartToken(), segment.getEndToken());
        // This segment has a faulty token range. Abort the entire repair run.
        synchronized (this) {
          context.storage.updateRepairRun(
              context.storage.getRepairRun(repairRunId).get()
                  .with()
                  .runState(RepairRun.RunState.ERROR)
                  .lastEvent(String.format("Replica set changed for segment %s on range [%s, %s]",
                      segmentId,
                      segment.getStartToken(),
                      segment.getEndToken()))
                  .endTime(DateTime.now())
                  .build(repairRunId));

          killAndCleanupRunner();
        }

        return false;
      }
    } else {
      // Add random sleep time to avoid one Reaper instance locking all others during multi DC incremental repairs
      Thread.sleep(ThreadLocalRandom.current().nextInt(10, 100) * 100);
      Optional<RepairSegment> rs = context.storage.getRepairSegment(repairRunId, segmentId);
      if (rs.isPresent()) {
        potentialCoordinators = Arrays.asList(rs.get().getCoordinatorHost());
      } else {
        // the segment has been removed. should only happen in tests on backends that delete repair segments.
        return false;
      }
    }

    try {
      SegmentRunner segmentRunner = SegmentRunner.create(
          context,
          clusterFacade,
          segmentId,
          potentialCoordinators,
          context.repairManager.getRepairTimeoutMillis(),
          intensity,
          validationParallelism,
          clusterName,
          repairUnit,
          repairRun.getTables(),
          this);

      ListenableFuture<?> segmentResult = context.repairManager.submitSegment(segmentRunner);
      Futures.addCallback(
          segmentResult,
          new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object ignored) {
              currentlyRunningSegments.set(rangeIndex, null);
              handleResult(segmentId);
            }

            @Override
            public void onFailure(Throwable throwable) {
              currentlyRunningSegments.set(rangeIndex, null);
              LOG.error("Executing SegmentRunner failed", throwable);
            }
          });
    } catch (ReaperException ex) {
      LOG.error("Executing SegmentRunner failed", ex);
    }

    return true;
  }

  private List<String> filterPotentialCoordinatorsByDatacenters(
      Collection<String> datacenters,
      List<String> potentialCoordinators) throws ReaperException {

    List<Pair<String, String>> coordinatorsWithDc = Lists.newArrayList();
    for (String coordinator:potentialCoordinators) {
      coordinatorsWithDc.add(getNodeDatacenterPair(coordinator));
    }

    List<String> coordinators = coordinatorsWithDc
        .stream()
        .filter(node -> datacenters.contains(node.getRight()) || datacenters.isEmpty())
        .map(nodeTuple -> nodeTuple.getLeft())
        .collect(Collectors.toList());

    LOG.debug(
        "[filterPotentialCoordinatorsByDatacenters] coordinators filtered by dc {}. Before : {} / After : {}",
        datacenters,
        potentialCoordinators,
        coordinators);

    return coordinators;
  }

  private Pair<String, String> getNodeDatacenterPair(String node) throws ReaperException {
    Pair<String, String> result = Pair.of(node, clusterFacade.getDatacenter(cluster, node));
    LOG.debug("[getNodeDatacenterPair] node/datacenter association {}", result);
    return result;
  }

  private void handleResult(UUID segmentId) {
    Optional<RepairSegment> segment = context.storage.getRepairSegment(repairRunId, segmentId);

    // Don't do rescheduling here, not to spawn uncontrolled amount of threads
    if (segment.isPresent()) {
      RepairSegment.State state = segment.get().getState();
      LOG.debug("In repair run #{}, triggerRepair on segment {} ended with state {}", repairRunId, segmentId, state);
      switch (state) {
        case NOT_STARTED:
          // Unsuccessful repair
          break;

        case DONE:
          // Successful repair
          break;

        default:
          // Another thread has started a new repair on this segment already
          // Or maybe the same repair segment id should never be re-run in which case this is an error
          String msg = "handleResult called with a segment state ("
              + state
              + ") that it "
              + "should not have after segmentRunner has tried a repair";
          LOG.error(msg);
          throw new AssertionError(msg);
      }
    } else {
      LOG.warn("In repair run #{}, triggerRepair on segment {} ended, but run is missing", repairRunId, segmentId);
    }
  }

  void updateLastEvent(String newEvent) {
    synchronized (this) {
      Optional<RepairRun> repairRun = context.storage.getRepairRun(repairRunId);
      // absent if deleted. should only happen in tests on backends that delete repair segments.
      if (!repairRun.isPresent() || repairRun.get().getRunState().isTerminated()) {
        LOG.warn(
            "Will not update lastEvent of run that has already terminated. The message was: " + "\"{}\"",
            newEvent);
      } else {
        context.storage.updateRepairRun(
            repairRun.get().with().lastEvent(newEvent).build(repairRunId),
            Optional.of(false));
        LOG.info(newEvent);
      }
    }
  }

  void killAndCleanupRunner() {
    context.repairManager.removeRunner(this);
    Thread.currentThread().interrupt();
  }

  private String metricName(String metric, String clusterName, String keyspaceName, UUID repairRunId) {
    String cleanClusterName = clusterName.replaceAll("[^A-Za-z0-9]", "");
    String cleanRepairRunId = repairRunId.toString().replaceAll("-", "");
    String cleanKeyspaceName = keyspaceName.replaceAll("[^A-Za-z0-9]", "");
    return MetricRegistry.name(RepairRunner.class, metric, cleanClusterName, cleanKeyspaceName, cleanRepairRunId);
  }

  private String metricName(String metric, String clusterName, UUID repairRunId) {
    String cleanClusterName = clusterName.replaceAll("[^A-Za-z0-9]", "");
    String cleanRepairRunId = repairRunId.toString().replaceAll("-", "");
    return MetricRegistry.name(RepairRunner.class, metric, cleanClusterName, cleanRepairRunId);
  }

  private RepairRun fixMissingRepairRunTables(RepairRun repairRun, RepairUnit repairUnit) throws ReaperException {
    if (repairRun.getTables().isEmpty()) {
      RepairRun newRepairRun = repairRun
          .with()
          .tables(RepairUnitService.create(context).getTablesToRepair(cluster, repairUnit))
          .build(repairRun.getId());

      context.storage.updateRepairRun(newRepairRun, Optional.of(false));
      return newRepairRun;
    }
    return repairRun;
  }
}
