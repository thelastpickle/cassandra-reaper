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
import io.cassandrareaper.core.CompactionStats;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.core.Segment;
import io.cassandrareaper.jmx.ClusterFacade;
import io.cassandrareaper.jmx.EndpointSnitchInfoProxy;
import io.cassandrareaper.jmx.JmxProxy;
import io.cassandrareaper.storage.IDistributedStorage;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class RepairRunner implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(RepairRunner.class);
  private static final ExecutorService METRICS_GRABBER_EXECUTOR = Executors.newFixedThreadPool(10);
  // Threshold over which adaptive schedules will get tuned for more segments.
  private static final int PERCENT_EXTENDED_THRESHOLD = 20;
  // Minimun number of segments per nodes for adaptive schedule auto-tune.
  private static final int MIN_SEGMENTS_PER_NODE_REDUCTION = 16;
  // Segment duration under which adaptive schedules will get a reduction in segments per node.
  private static final int SEGMENT_DURATION_FOR_REDUCTION_THRESHOLD = 5;

  private final AppContext context;
  private final ClusterFacade clusterFacade;
  private final UUID repairRunId;
  private final String clusterName;
  private final String metricNameForMillisSinceLastRepairPerKeyspace;
  private final String metricNameForMillisSinceLastRepair;
  private final Cluster cluster;
  private float repairProgress;
  private float segmentsDone;
  private float segmentsTotal;
  private final List<RingRange> localEndpointRanges;
  private final RepairUnit repairUnit;

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
    repairUnit = context.storage.getRepairUnit(repairRun.get().getRepairUnitId());
    this.clusterName = cluster.getName();

    localEndpointRanges = context.config.isInSidecarMode()
        ? clusterFacade.getRangesForLocalEndpoint(cluster, repairUnit.getKeyspaceName())
        : Collections.emptyList();

    String repairUnitClusterName = repairUnit.getClusterName();
    String repairUnitKeyspaceName = repairUnit.getKeyspaceName();

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
    if (context.metricRegistry.getMetrics().containsKey(metricName)) {
      context.metricRegistry.remove(metricName);
    }
    context.metricRegistry.register(metricName, gauge);
  }

  UUID getRepairRunId() {
    return repairRunId;
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

        context.metricRegistry.counter(
          MetricRegistry.name(RepairManager.class, "repairDone", RepairRun.RunState.DONE.toString())).inc();

        maybeAdaptRepairSchedule();
      }
    }
  }

  /**
   * Tune segment timeout and number of segments for adaptive schedules.
   * Checks that the run was triggered by an adaptive schedule and gathers info on the run to apply tunings.
   */
  @VisibleForTesting
  public void maybeAdaptRepairSchedule() {
    Optional<RepairRun> repairRun = context.storage.getRepairRun(repairRunId);
    if (repairRun.isPresent() && Boolean.TRUE.equals(repairRun.get().getAdaptiveSchedule())) {
      Collection<RepairSegment> segments
          = context.storage.getSegmentsWithState(repairRunId, RepairSegment.State.DONE);
      int maxSegmentDurationInMins = segments.stream()
          .mapToInt(repairSegment
              -> (int) (repairSegment.getEndTime().getMillis() - repairSegment.getStartTime().getMillis()) / 60_000)
          .max()
          .orElseThrow(NoSuchElementException::new);
      int extendedSegments = (int) segments.stream().filter(segment -> segment.getFailCount() > 0).count();
      double percentExtendedSegments
          = ((float) extendedSegments / (float) repairRun.get().getSegmentCount()) * 100.0;
      LOG.info("extendedSegments = {}, total segments = {}, percent extended = {}",
          extendedSegments,
          repairRun.get().getSegmentCount(),
          percentExtendedSegments);
      tuneAdaptiveRepair(percentExtendedSegments, maxSegmentDurationInMins);
    }
  }

  @VisibleForTesting
  void tuneAdaptiveRepair(double percentExtendedSegments, int maxSegmentDuration) {
    LOG.info("Percent extended segments: {}", percentExtendedSegments);
    if (percentExtendedSegments > PERCENT_EXTENDED_THRESHOLD) {
      // Too many segments were extended, meaning that the number of segments is too low
      addSegmentsPerNodeToScheduleForUnit();
    } else if ((int) percentExtendedSegments <= PERCENT_EXTENDED_THRESHOLD && percentExtendedSegments >= 1) {
      // The number of extended segments is moderate and timeout should be extended
      raiseTimeoutOfUnit();
    } else if (percentExtendedSegments == 0 && maxSegmentDuration < SEGMENT_DURATION_FOR_REDUCTION_THRESHOLD) {
      // Segments are being executed very fast so segment count could be lowered
      reduceSegmentsPerNodeToScheduleForUnit();
    }
  }

  @VisibleForTesting
  void reduceSegmentsPerNodeToScheduleForUnit() {
    LOG.debug("Reducing segments per node for adaptive schedule on repair unit {}", repairUnit.getId());
    RepairSchedule scheduleToTune = getScheduleForRun();

    // Reduce segments by decrements of 10%
    int newSegmentCountPerNode
        = (int) Math.max(MIN_SEGMENTS_PER_NODE_REDUCTION, scheduleToTune.getSegmentCountPerNode() / 1.1);

    // update schedule with new segment per node value
    RepairSchedule newSchedule
        = scheduleToTune.with().segmentCountPerNode(newSegmentCountPerNode).build(scheduleToTune.getId());
    context.storage.updateRepairSchedule(newSchedule);

  }

  @VisibleForTesting
  void raiseTimeoutOfUnit() {
    // Build updated repair unit
    RepairUnit updatedUnit = repairUnit.with().timeout(repairUnit.getTimeout() * 2).build(repairUnit.getId());

    // update unit with new timeout
    context.storage.updateRepairUnit(updatedUnit);
  }

  @VisibleForTesting
  void addSegmentsPerNodeToScheduleForUnit() {
    LOG.debug("Adding segments per node for adaptive schedule on repair unit {}", repairUnit.getId());
    RepairSchedule scheduleToTune = getScheduleForRun();

    // Reduce segments by increments of 20%
    int newSegmentCountPerNode
        = (int) Math.max(MIN_SEGMENTS_PER_NODE_REDUCTION, scheduleToTune.getSegmentCountPerNode() * 1.2);

    // update schedule with new segment per node value
    RepairSchedule newSchedule
        = scheduleToTune.with().segmentCountPerNode(newSegmentCountPerNode).build(scheduleToTune.getId());
    context.storage.updateRepairSchedule(newSchedule);
  }

  private RepairSchedule getScheduleForRun() {
    // find schedule
    Collection<RepairSchedule> schedulesForKeyspace
        = context.storage.getRepairSchedulesForClusterAndKeyspace(clusterName, repairUnit.getKeyspaceName());
    List<RepairSchedule> schedulesToTune = schedulesForKeyspace.stream()
        .filter(schedule -> schedule.getRepairUnitId().equals(repairUnit.getId()))
        .collect(Collectors.toList());

    // Set precondition that only a single schedule should match
    Preconditions.checkArgument(schedulesToTune.size() == 1, String.format("Update for repair run %s and unit %s "
        + "should impact a single schedule. %d were found", repairRunId, repairUnit.getId(), schedulesToTune.size()));

    return schedulesToTune.get(0);
  }

  /**
   * Get the next segment and repair it. If there is none, we're done.
   */
  private void startNextSegment() throws ReaperException, InterruptedException {
    boolean scheduleRetry = true;

    // We want to know whether a repair was started,
    // so that a rescheduling of this runner will happen.
    boolean repairStarted = false;

    // We have an empty slot, so let's start new segment runner if possible.
    // When in sidecar mode, filter on ranges that the local node is a replica for only.
    LOG.info("Attempting to run new segment...");
    List<RepairSegment> nextRepairSegments
        = context.config.isInSidecarMode()
            ? ((IDistributedStorage) context.storage)
                .getNextFreeSegmentsForRanges(
                    repairRunId, localEndpointRanges)
            : context.storage.getNextFreeSegments(
                repairRunId);

    Optional<RepairSegment> nextRepairSegment = Optional.empty();
    for (RepairSegment segment : nextRepairSegments) {
      Collection<String> potentialReplicas = repairUnit.getIncrementalRepair()
          ? Collections.singletonList(segment.getCoordinatorHost())
          : segment.getReplicas().keySet();
      JmxProxy coordinator = clusterFacade.connect(cluster, potentialReplicas);
      if (nodesReadyForNewRepair(coordinator, segment, segment.getReplicas(), repairRunId)) {
        nextRepairSegment = Optional.of(segment);
        break;
      }
    }
    if (!nextRepairSegment.isPresent()) {
      String msg = "All nodes are busy or have too many pending compactions for the remaining candidate segments.";
      LOG.info(msg);
      updateLastEvent(msg);
    } else {
      LOG.info("Next segment to run : {}", nextRepairSegment.get().getId());
      scheduleRetry = repairSegment(
              nextRepairSegment.get().getId(),
              nextRepairSegment.get().getTokenRange());
      if (scheduleRetry) {
        segmentsTotal = context.storage.getSegmentAmountForRepairRun(repairRunId);
        repairStarted = true;
      }
    }

    if (!repairStarted) {
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


  Pair<String, Callable<Optional<CompactionStats>>> getNodeMetrics(String node, String localDc, String nodeDc) {

    return Pair.of(node, () -> {
      LOG.debug("getMetricsForHost {} / {} / {}", node, localDc, nodeDc);
      CompactionStats activeCompactions = clusterFacade.listActiveCompactions(
          Node.builder()
              .withCluster(cluster)
              .withHostname(node)
          .build());

      return Optional.ofNullable(activeCompactions);
    });
  }

  private boolean nodesReadyForNewRepair(
      JmxProxy coordinator,
      RepairSegment segment,
      Map<String, String> dcByNode,
      UUID segmentId) {

    Collection<String> nodes = getNodesInvolvedInSegment(dcByNode);
    String dc = EndpointSnitchInfoProxy.create(coordinator).getDataCenter();
    boolean requireAllHostMetrics = DatacenterAvailability.LOCAL != context.config.getDatacenterAvailability();
    boolean allLocalDcHostsChecked = true;
    boolean allHostsChecked = true;
    Set<String> unreachableNodes = Sets.newHashSet();

    List<Pair<String, Future<Optional<CompactionStats>>>> nodeMetricsTasks = nodes.stream()
        .map(node -> getNodeMetrics(node, dc != null ? dc : "", dcByNode.get(node) != null ? dcByNode.get(node) : ""))
        .map(pair -> Pair.of(pair.getLeft(), METRICS_GRABBER_EXECUTOR.submit(pair.getRight())))
        .collect(Collectors.toList());

    for (Pair<String, Future<Optional<CompactionStats>>> pair : nodeMetricsTasks) {
      try {
        Optional<CompactionStats> result = pair.getRight().get();
        if (result.isPresent()) {
          CompactionStats metrics = result.get();
          Optional<Integer> pendingCompactions = metrics.getPendingCompactions();
          if (pendingCompactions.isPresent() && pendingCompactions.get() > context.config.getMaxPendingCompactions()) {
            String msg = String.format(
                "postponed repair segment %s because of too many pending compactions (%s > %s) on host %s",
                segmentId, pendingCompactions, context.config.getMaxPendingCompactions(), pair.getLeft());

            updateLastEvent(msg);
            return false;
          }

          continue;
        }
      } catch (InterruptedException | ExecutionException e) {
        LOG.info("Failed grabbing metrics from {}", pair.getLeft(), e);
      }
      allHostsChecked = false;
      if (dcByNode.get(pair.getLeft()).equals(dc)) {
        allLocalDcHostsChecked = false;
      }
      if (requireAllHostMetrics || dcByNode.get(pair.getLeft()).equals(dc)) {
        unreachableNodes.add(pair.getLeft());
      }
    }

    if (okToRepairSegment(allHostsChecked, allLocalDcHostsChecked, context.config.getDatacenterAvailability())) {
      LOG.debug("Ok to repair segment '{}' on repair run with id '{}'", segment.getId(), segment.getRunId());
      return true;
    } else {
      String msg = String.format(
          "Postponed repair segment %s on repair run with id %s because we couldn't get %shosts metrics on %s",
          segment.getId(),
          segment.getRunId(),
          (requireAllHostMetrics ? "" : "datacenter "),
          StringUtils.join(unreachableNodes, ' '));

      updateLastEvent(msg);
      return false;
    }
  }

  static boolean okToRepairSegment(
      boolean allHostsChecked,
      boolean allLocalDcHostsChecked,
      DatacenterAvailability dcAvailability) {

    return allHostsChecked || (allLocalDcHostsChecked && DatacenterAvailability.LOCAL == dcAvailability);
  }

  private Collection<String> getNodesInvolvedInSegment(Map<String, String> dcByNode) {
    Set<String> datacenters = repairUnit.getDatacenters();

    return dcByNode.keySet().stream()
        .filter(node -> datacenters.isEmpty() || datacenters.contains(dcByNode.get(node)))
        .collect(Collectors.toList());
  }

  /**
   * Start the repair of a segment.
   *
   * @param segmentId id of the segment to repair.
   * @param segment token range of the segment to repair.
   * @return Boolean indicating whether rescheduling next run is needed.
   * @throws ReaperException any runtime exception we caught in the execution
   */
  private boolean repairSegment(final UUID segmentId, Segment segment)
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
    if (!repairUnit.getIncrementalRepair()) {
      Set<String> segmentReplicas = segment.getReplicas().keySet();
      // full repair
      try {
        potentialCoordinators = filterPotentialCoordinatorsByDatacenters(
                repairUnit.getDatacenters(),
                clusterFacade.tokenRangeToEndpoint(cluster, keyspace, segment));

      } catch (RuntimeException e) {
        LOG.warn("Couldn't get token ranges from coordinator: #{}", e);
        return true;
      }
      if (potentialCoordinators.isEmpty()) {
        LOG.warn(
            "Segment #{} is faulty, no potential coordinators for range: {}",
            segmentId,
            segment.toString());
        // This segment has a faulty token range. Abort the entire repair run.
        synchronized (this) {
          context.storage.updateRepairRun(
              context.storage.getRepairRun(repairRunId).get()
                  .with()
                  .runState(RepairRun.RunState.ERROR)
                  .lastEvent(String.format("No coordinators for range %s", segment))
                  .endTime(DateTime.now())
                  .build(repairRunId));

          context.metricRegistry.counter(
            MetricRegistry.name(RepairManager.class, "repairDone", RepairRun.RunState.ERROR.toString())).inc();

          killAndCleanupRunner();
        }

        return false;
      } else if (
          !Sets.difference(
            segmentReplicas,
            potentialCoordinators.stream().collect(Collectors.toSet())).isEmpty()) {
        LOG.warn(
            "Segment #{} is faulty, replica set changed since repair was started: {}",
            segmentId,
            segment.toString());
        // This segment has a faulty token range. Abort the entire repair run.
        synchronized (this) {
          context.storage.updateRepairRun(
              context.storage.getRepairRun(repairRunId).get()
                  .with()
                  .runState(RepairRun.RunState.ERROR)
                  .lastEvent(String.format("Replica set changed for segment %s on range %s",
                      segmentId,
                      segment))
                  .endTime(DateTime.now())
                  .build(repairRunId));

          context.metricRegistry.counter(
            MetricRegistry.name(RepairManager.class, "repairDone", RepairRun.RunState.ERROR.toString())).inc();

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
          TimeUnit.MINUTES.toMillis(repairUnit.getTimeout()),
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
              handleResult(segmentId);
            }

            @Override
            public void onFailure(Throwable throwable) {
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

  public Cluster getCluster() {
    return this.cluster;
  }
}
