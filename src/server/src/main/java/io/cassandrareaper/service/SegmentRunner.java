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
import io.cassandrareaper.ReaperApplicationConfiguration.DatacenterAvailability;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.NodeMetrics;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.jmx.ClusterFacade;
import io.cassandrareaper.jmx.EndpointSnitchInfoProxy;
import io.cassandrareaper.jmx.JmxProxy;
import io.cassandrareaper.jmx.RepairStatusHandler;
import io.cassandrareaper.jmx.SnapshotProxy;
import io.cassandrareaper.storage.IDistributedStorage;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.management.JMException;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sun.management.UnixOperatingSystemMXBean;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.ConcurrentException;
import org.apache.commons.lang3.concurrent.LazyInitializer;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


final class SegmentRunner implements RepairStatusHandler, Runnable {

  // Caching all active SegmentRunners.
  static final Map<UUID, SegmentRunner> SEGMENT_RUNNERS = Maps.newConcurrentMap();

  private static final Logger LOG = LoggerFactory.getLogger(SegmentRunner.class);

  private static final int MAX_TIMEOUT_EXTENSIONS = 10;
  private static final Pattern REPAIR_UUID_PATTERN
      = Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");

  private static final long SLEEP_TIME_AFTER_POSTPONE_IN_MS = 10000;
  private static final ExecutorService METRICS_GRABBER_EXECUTOR = Executors.newFixedThreadPool(10);
  private static final long METRICS_POLL_INTERVAL_MS = TimeUnit.SECONDS.toMillis(5);
  private static final long METRICS_MAX_WAIT_MS = TimeUnit.MINUTES.toMillis(2);

  private final AppContext context;
  private final RepairUnitService repairUnitService;
  private final UUID segmentId;
  private final Condition condition = new SimpleCondition();
  private final Collection<String> potentialCoordinators;
  private final long timeoutMillis;
  private final double intensity;
  private final RepairParallelism validationParallelism;
  private final String clusterName;
  private final RepairRunner repairRunner;
  private final RepairUnit repairUnit;
  private volatile int repairNo;
  private final AtomicBoolean segmentFailed;
  private final UUID leaderElectionId;
  private final AtomicBoolean successOrFailedNotified = new AtomicBoolean(false);
  private final AtomicBoolean completeNotified = new AtomicBoolean(false);
  private final ClusterFacade clusterFacade;


  private SegmentRunner(
      AppContext context,
      ClusterFacade clusterFacade,
      UUID segmentId,
      Collection<String> potentialCoordinators,
      long timeoutMillis,
      double intensity,
      RepairParallelism validationParallelism,
      String clusterName,
      RepairUnit repairUnit,
      RepairRunner repairRunner)
      throws ReaperException {

    if (SEGMENT_RUNNERS.containsKey(segmentId)) {
      LOG.error("SegmentRunner already exists for segment with ID: {}", segmentId);
      throw new ReaperException("SegmentRunner already exists for segment with ID: " + segmentId);
    }
    this.context = context;
    this.clusterFacade = clusterFacade;
    this.repairUnitService = RepairUnitService.create(context);
    this.segmentId = segmentId;
    this.potentialCoordinators = potentialCoordinators;
    this.timeoutMillis = timeoutMillis;
    this.intensity = intensity;
    this.validationParallelism = validationParallelism;
    this.clusterName = clusterName;
    this.repairUnit = repairUnit;
    this.repairRunner = repairRunner;
    this.segmentFailed = new AtomicBoolean(false);
    this.leaderElectionId = repairUnit.getIncrementalRepair() ? repairRunner.getRepairRunId() : segmentId;
  }

  public static SegmentRunner create(
      AppContext context,
      ClusterFacade clusterFacade,
      UUID segmentId,
      Collection<String> potentialCoordinators,
      long timeoutMillis,
      double intensity,
      RepairParallelism validationParallelism,
      String clusterName,
      RepairUnit repairUnit,
      RepairRunner repairRunner) throws ReaperException {

    return new SegmentRunner(
        context,
        clusterFacade,
        segmentId,
        potentialCoordinators,
        timeoutMillis,
        intensity,
        validationParallelism,
        clusterName,
        repairUnit,
        repairRunner);
  }

  @Override
  public void run() {
    boolean ran = false;
    if (takeLead()) {
      try {
        ran = runRepair();
      } finally {
        releaseLead();
      }
    }
    if (ran) {
      long delay = intensityBasedDelayMillis(intensity);
      try {
        Thread.sleep(delay);
      } catch (InterruptedException e) {
        LOG.warn("Slept shorter than intended delay.");
      }
    }
  }

  static void postponeSegment(AppContext context, RepairSegment segment) {
    postpone(context, segment, context.storage.getRepairUnit(segment.getRepairUnitId()));
  }

  private static void postpone(AppContext context, RepairSegment segment, RepairUnit repairUnit) {
    LOG.info("Postponing segment {}", segment.getId());
    try {
      context.storage.updateRepairSegment(
          segment
              .reset()
              // set coordinator host to null only for full repairs
              .withCoordinatorHost(repairUnit.getIncrementalRepair() ? segment.getCoordinatorHost() : null)
              .withFailCount(segment.getFailCount() + 1)
              .withId(segment.getId())
              .build());
    } finally {
      SEGMENT_RUNNERS.remove(segment.getId());
      context.metricRegistry.counter(metricNameForPostpone(repairUnit, segment)).inc();
    }
  }

  static void abort(AppContext context, RepairSegment segment, JmxProxy jmxConnection) {
    postpone(context, segment, context.storage.getRepairUnit(segment.getRepairUnitId()));
    LOG.info("Aborting repair on segment with id {} on coordinator {}", segment.getId(), segment.getCoordinatorHost());

    String metric = MetricRegistry.name(
        SegmentRunner.class,
        "abort",
        Optional.ofNullable(segment.getCoordinatorHost()).orElse("null").replace('.', '-'));

    context.metricRegistry.counter(metric).inc();
    jmxConnection.cancelAllRepairs();
  }

  private void abort(RepairSegment segment, JmxProxy jmxConnection) {
    abort(context, segment, jmxConnection);
  }

  /**
   * Remember to call method postponeCurrentSegment() outside of synchronized(condition) block.
   */
  void postponeCurrentSegment() {
    synchronized (condition) {
      RepairSegment segment = context.storage.getRepairSegment(repairRunner.getRepairRunId(), segmentId).get();
      postpone(context, segment, context.storage.getRepairUnit(segment.getRepairUnitId()));
    }

    try {
      Thread.sleep(SLEEP_TIME_AFTER_POSTPONE_IN_MS);
    } catch (InterruptedException e) {
      LOG.debug("Interrupted while sleeping after a segment was postponed... weird stuff...");
    }
  }

  /**
   * This method is intended to be temporary, until we find the root issue of too many open files issue.
   */
  private static long getOpenFilesAmount() {
    OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
    long amountOfOpenFiles = -1;
    if (os instanceof UnixOperatingSystemMXBean) {
      amountOfOpenFiles = ((UnixOperatingSystemMXBean) os).getOpenFileDescriptorCount();
    }
    return amountOfOpenFiles;
  }

  private boolean runRepair() {
    LOG.debug("Run repair for segment #{}", segmentId);
    RepairSegment segment = context.storage.getRepairSegment(repairRunner.getRepairRunId(), segmentId).get();
    Thread.currentThread().setName(clusterName + ":" + segment.getRunId() + ":" + segmentId);

    try (Timer.Context cxt = context.metricRegistry.timer(metricNameForRunRepair(segment)).time()) {
      Cluster cluster = context.storage.getCluster(clusterName).get();
      JmxProxy coordinator = clusterFacade.connectAny(cluster, potentialCoordinators);

      if (SEGMENT_RUNNERS.containsKey(segmentId)) {
        LOG.error("SegmentRunner already exists for segment with ID: {}", segmentId);
        throw new ReaperException("SegmentRunner already exists for segment with ID: " + segmentId);
      }

      Set<String> tablesToRepair;
      try {
        tablesToRepair = repairUnitService.getTablesToRepair(coordinator, cluster, repairUnit);
      } catch (IllegalStateException e) {
        String msg = "Invalid blacklist definition. It filtered all tables in the keyspace.";
        LOG.error(msg, e);
        RepairRun repairRun = context.storage.getRepairRun(segment.getRunId()).get();

        context.storage.updateRepairRun(
            repairRun
                .with()
                .runState(RepairRun.RunState.ERROR)
                .lastEvent(String.format(msg))
                .endTime(DateTime.now())
                .build(segment.getRunId()));

        repairRunner.killAndCleanupRunner();

        context.storage.updateRepairSegment(
            segment
                .with()
                .withState(RepairSegment.State.DONE)
                .withStartTime(DateTime.now())
                .withEndTime(DateTime.now())
                .withId(segmentId)
                .build());

        return false;
      }

      String keyspace = repairUnit.getKeyspaceName();
      boolean fullRepair = !repairUnit.getIncrementalRepair();

      LazyInitializer<Set<String>> busyHosts = new BusyHostsInitializer(cluster);

      if (!canRepair(segment, keyspace, coordinator, cluster, busyHosts)) {
        LOG.info(
            "Cannot run segment {} for repair {} at the moment. Will try again later",
            segmentId,
            segment.getRunId());
        SEGMENT_RUNNERS.remove(segment.getId());
        try {
          Thread.sleep(SLEEP_TIME_AFTER_POSTPONE_IN_MS);
        } catch (InterruptedException e) {
          LOG.debug("Interrupted while sleeping after a segment was postponed... weird stuff...");
        }
        return false;
      }

      try (Timer.Context cxt1 = context.metricRegistry.timer(metricNameForRepairing(segment)).time()) {
        try {
          LOG.debug("Enter synchronized section with segment ID {}", segmentId);
          synchronized (condition) {

            segment = segment
                    .with()
                    .withState(RepairSegment.State.STARTED)
                    .withCoordinatorHost(coordinator.getHost())
                    .withStartTime(DateTime.now())
                    .withId(segmentId)
                    .build();

            context.storage.updateRepairSegment(segment);

            repairNo = coordinator.triggerRepair(
                    segment.getStartToken(),
                    segment.getEndToken(),
                    keyspace,
                    validationParallelism,
                    tablesToRepair,
                    fullRepair,
                    repairUnit.getDatacenters(),
                    this,
                    segment.getTokenRange().getTokenRanges(),
                    repairUnit.getRepairThreadCount());

            if (0 != repairNo) {
              processTriggeredSegment(segment, coordinator, repairNo);
            } else {
              LOG.info("Nothing to repair for segment {} in keyspace {}", segmentId, keyspace);

              context.storage.updateRepairSegment(
                  segment
                      .with()
                      .withState(RepairSegment.State.DONE)
                      .withEndTime(DateTime.now())
                      .withId(segmentId)
                      .build());

              SEGMENT_RUNNERS.remove(segment.getId());
            }
          }
        } finally {
          LOG.debug("Exiting synchronized section with segment ID {}", segmentId);
        }
      }
    } catch (RuntimeException | ReaperException e) {
      LOG.warn("Failed to connect to a coordinator node for segment {}", segmentId, e);
      String msg = "Postponed a segment because no coordinator was reachable";
      repairRunner.updateLastEvent(msg);
      postponeCurrentSegment();
      LOG.warn("Open files amount for process: " + getOpenFilesAmount());
      return false;
    } finally {
      SEGMENT_RUNNERS.remove(segment.getId());
      context.metricRegistry
          .histogram(MetricRegistry.name(SegmentRunner.class, "openFiles"))
          .update(getOpenFilesAmount());
    }
    return true;
  }

  private void processTriggeredSegment(final RepairSegment segment, final JmxProxy coordinator, int repairNo) {

    repairRunner.updateLastEvent(
        String.format("Triggered repair of segment %s via host %s", segment.getId(), coordinator.getHost()));

    {
      long timeout = repairUnit.getIncrementalRepair() ? timeoutMillis * MAX_TIMEOUT_EXTENSIONS : timeoutMillis;
      LOG.info("Repair for segment {} started, status wait will timeout in {} millis", segmentId, timeout);
    }

    try {
      final long startTime = System.currentTimeMillis();
      final long maxTime = startTime + timeoutMillis;
      final long waitTime = Math.min(timeoutMillis, 60000);
      long lastLoopTime = startTime;

      while (System.currentTimeMillis() < maxTime) {
        condition.await(waitTime, TimeUnit.MILLISECONDS);

        boolean isDoneOrTimedOut = lastLoopTime + 60_000 > System.currentTimeMillis();

        isDoneOrTimedOut |= RepairSegment.State.DONE == context.storage
            .getRepairSegment(segment.getRunId(), segmentId).get().getState();

        if (isDoneOrTimedOut) {
          break;
        }
        renewLead();
        lastLoopTime = System.currentTimeMillis();
      }
    } catch (InterruptedException e) {
      LOG.warn("Repair command {} on segment {} interrupted", this.repairNo, segmentId, e);
    } finally {
      coordinator.removeRepairStatusHandler(repairNo);
      RepairSegment resultingSegment
          = context.storage.getRepairSegment(repairRunner.getRepairRunId(), segmentId).get();

      LOG.info(
          "Repair command {} on segment {} returned with state {}",
          this.repairNo,
          segmentId,
          resultingSegment.getState());

      switch (resultingSegment.getState()) {
        case STARTED:
        case RUNNING:
          LOG.info("Repair command {} on segment {} has been cancelled while running", this.repairNo, segmentId);
          segmentFailed.set(true);
          abort(resultingSegment, coordinator);
          break;

        case DONE:
          LOG.debug(
              "Repair segment with id '{}' was repaired in {} seconds",
              resultingSegment.getId(),
              Seconds.secondsBetween(resultingSegment.getStartTime(), resultingSegment.getEndTime()).getSeconds());

          SEGMENT_RUNNERS.remove(resultingSegment.getId());
          break;

        default:
          // Something went wrong on the coordinator node and we never got the RUNNING notification
          // or we are in an undetermined state.
          // Let's just abort and reschedule the segment.
          LOG.info(
              "Repair command {} on segment {} never managed to start within timeout.",
              this.repairNo,
              segmentId);
          segmentFailed.set(true);
          abort(resultingSegment, coordinator);
      }
      // Repair is still running, we'll renew lead on the segment when using Cassandra as storage backend
      renewLead();
    }
  }

  private static String metricNameForPostpone(RepairUnit unit, RepairSegment segment) {
    String cleanHostName = Optional.ofNullable(segment.getCoordinatorHost()).orElse("null")
        .replace('.', 'x')
        .replaceAll("[^A-Za-z0-9]", "");
    return MetricRegistry.name(
            SegmentRunner.class,
            "postpone",
            cleanHostName,
            unit.getClusterName().replaceAll("[^A-Za-z0-9]", ""),
            unit.getKeyspaceName().replaceAll("[^A-Za-z0-9]", ""));
  }

  private String metricNameForRepairing(RepairSegment rs) {
    String cleanHostName = Optional.ofNullable(rs.getCoordinatorHost()).orElse("null")
        .replace('.', 'x')
        .replaceAll("[^A-Za-z0-9]", "");
    return MetricRegistry.name(
        SegmentRunner.class,
        "repairing",
        cleanHostName,
        clusterName.replaceAll("[^A-Za-z0-9]", ""),
        repairUnit.getKeyspaceName().replaceAll("[^A-Za-z0-9]", ""));
  }

  private String metricNameForRunRepair(RepairSegment rs) {
    String cleanHostName = Optional.ofNullable(rs.getCoordinatorHost()).orElse("null")
        .replace('.', 'x')
        .replaceAll("[^A-Za-z0-9]", "");
    return MetricRegistry.name(
        SegmentRunner.class,
        "runRepair",
        cleanHostName,
        clusterName.replaceAll("[^A-Za-z0-9]", ""),
        repairUnit.getKeyspaceName().replaceAll("[^A-Za-z0-9]", ""));
  }

  boolean canRepair(
      RepairSegment segment,
      String keyspace,
      JmxProxy coordinator,
      Cluster cluster,
      LazyInitializer<Set<String>> busyHosts) {

    try {
      Map<String, String> dcByNode = getDCsByNodeForRepairSegment(coordinator, cluster, segment, keyspace);

      return !isRepairRunningOnNodes(segment, dcByNode, keyspace, cluster)
          && nodesReadyForNewRepair(coordinator, segment, dcByNode, busyHosts);

    } catch (RuntimeException e) {
      LOG.warn("SegmentRunner couldn't get token ranges from coordinator: ", e);
      String msg = "SegmentRunner couldn't get token ranges from coordinator";
      repairRunner.updateLastEvent(msg);
      return false;
    }
  }

  static boolean okToRepairSegment(
      boolean allHostsChecked,
      boolean allLocalDcHostsChecked,
      DatacenterAvailability dcAvailability) {

    return allHostsChecked || (allLocalDcHostsChecked && DatacenterAvailability.LOCAL == dcAvailability);
  }

  private void handlePotentialStuckRepairs(LazyInitializer<Set<String>> busyHosts, String hostName)
      throws ConcurrentException {

    if (!busyHosts.get().contains(hostName) && context.storage instanceof IDistributedStorage) {
      try {
        JmxProxy hostProxy
            = clusterFacade.connectAny(context.storage.getCluster(clusterName).get(), Arrays.asList(hostName));

        // We double check that repair is still running there before actually cancelling repairs
        if (hostProxy.isRepairRunning()) {
          LOG.warn(
              "A host ({}) reported that it is involved in a repair, but there is no record "
                  + "of any ongoing repair involving the host. Sending command to abort all repairs "
                  + "on the host.",
              hostName);
          hostProxy.cancelAllRepairs();
        }
      } catch (ReaperException | RuntimeException | JMException e) {
        LOG.debug("failed to cancel repairs on host {}", hostName, e);
      }
    }
  }

  Pair<String, Callable<Optional<NodeMetrics>>> getNodeMetrics(String node, String localDc, String nodeDc) {

    return Pair.of(node, () -> {
      LOG.debug("getMetricsForHost {} / {} / {}", node, localDc, nodeDc);

      if (clusterFacade.nodeIsAccessibleThroughJmx(nodeDc, node)) {
        try {
          JmxProxy nodeProxy
              = clusterFacade.connectAny(context.storage.getCluster(clusterName).get(), Arrays.asList(node));

          NodeMetrics metrics = NodeMetrics.builder()
                  .withNode(node)
                  .withDatacenter(nodeDc)
                  .withCluster(nodeProxy.getClusterName())
                  .withPendingCompactions(nodeProxy.getPendingCompactions())
                  .withHasRepairRunning(nodeProxy.isRepairRunning())
                  .withActiveAnticompactions(0) // for future use
                  .build();

          return Optional.of(metrics);
        } catch (RuntimeException | ReaperException e) {
          LOG.debug("failed to query metrics for host {}, trying to get metrics from storage...", node, e);
        }
      }
      return nodeDc.equals(localDc)
          ? Optional.empty()
          : getRemoteNodeMetrics(node, nodeDc);
    });
  }

  private Optional<NodeMetrics> getRemoteNodeMetrics(String node, String nodeDc) {
    Preconditions.checkState(DatacenterAvailability.ALL != context.config.getDatacenterAvailability());
    Preconditions.checkState(context.storage instanceof IDistributedStorage);
    IDistributedStorage storage = ((IDistributedStorage) context.storage);
    Optional<NodeMetrics> result = storage.getNodeMetrics(repairRunner.getRepairRunId(), node);
    if (!result.isPresent()) {
      // Sending a request for metrics to the other reaper instances through the Cassandra backend
      storeNodeMetrics(
          NodeMetrics.builder()
              .withCluster(clusterName)
              .withDatacenter(nodeDc)
              .withNode(node)
              .withRequested(true)
              .build());

      long start = System.currentTimeMillis();

      while ( (!result.isPresent() || result.get().isRequested())
          && start + METRICS_MAX_WAIT_MS > System.currentTimeMillis()) {

        try {
          Thread.sleep(METRICS_POLL_INTERVAL_MS);
        } catch (InterruptedException ignore) { }
        LOG.info("Trying to get metrics from remote DCs for {} in {} of {}", node, nodeDc, clusterName);
        result = storage.getNodeMetrics(repairRunner.getRepairRunId(), node);
        if (result.isPresent() && !result.get().isRequested()) {
          // delete the metrics to force other instances to get a refreshed value
          storage.deleteNodeMetrics(repairRunner.getRepairRunId(), node);
        }
      }
    }
    return result;
  }

  private boolean nodesReadyForNewRepair(
      JmxProxy coordinator,
      RepairSegment segment,
      Map<String, String> dcByNode,
      LazyInitializer<Set<String>> busyHosts) {

    Collection<String> nodes = getNodesInvolvedInSegment(dcByNode);
    String dc = EndpointSnitchInfoProxy.create(coordinator).getDataCenter();
    boolean requireAllHostMetrics = DatacenterAvailability.ALL == context.config.getDatacenterAvailability();
    boolean allLocalDcHostsChecked = true;
    boolean allHostsChecked = true;
    Set<String> unreachableNodes = Sets.newHashSet();

    List<Pair<String, Future<Optional<NodeMetrics>>>> nodeMetricsTasks = nodes.stream()
        .map(node -> getNodeMetrics(node, dc != null ? dc : "", dcByNode.get(node) != null ? dcByNode.get(node) : ""))
        .map(pair -> Pair.of(pair.getLeft(), METRICS_GRABBER_EXECUTOR.submit(pair.getRight())))
        .collect(Collectors.toList());

    for (Pair<String, Future<Optional<NodeMetrics>>> pair : nodeMetricsTasks) {
      try {
        Optional<NodeMetrics> result = pair.getRight().get();
        if (result.isPresent()) {
          NodeMetrics metrics = result.get();
          int pendingCompactions = metrics.getPendingCompactions();
          if (pendingCompactions > context.config.getMaxPendingCompactions()) {
            String msg = String.format(
                "postponed repair segment %s because of too many pending compactions (%s > %s) on host %s",
                segmentId, pendingCompactions, context.config.getMaxPendingCompactions(), metrics.getNode());

            repairRunner.updateLastEvent(msg);
            return false;
          }
          if (metrics.hasRepairRunning()) {
            String msg = String.format(
                "postponed repair segment %s because one of the hosts (%s) was already involved in a repair",
                segmentId, metrics.getNode());

            repairRunner.updateLastEvent(msg);
            handlePotentialStuckRepairs(busyHosts, metrics.getNode());
            return false;
          }
          continue;
        }
      } catch (InterruptedException | ExecutionException | ConcurrentException e) {
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
      LOG.info("Ok to repair segment '{}' on repair run with id '{}'", segment.getId(), segment.getRunId());
      return true;
    } else {
      String msg = String.format(
          "Postponed repair segment %s on repair run with id %s because we couldn't get %shosts metrics on %s",
          segment.getId(),
          segment.getRunId(),
          (requireAllHostMetrics ? "" : "datacenter "),
          StringUtils.join(unreachableNodes, ' '));

      repairRunner.updateLastEvent(msg);
      return false;
    }
  }

  private boolean isRepairRunningOnNodes(
      RepairSegment segment,
      Map<String, String> dcByNode,
      String keyspace,
      Cluster cluster) {

    Collection<String> nodes = repairUnit.getIncrementalRepair()
        ? Collections.EMPTY_SET
        : getNodesInvolvedInSegment(dcByNode);

    Collection<RepairSegment> segments;
    {
      UUID repairRunId = segment.getRunId();
      // this only checks whether any segments from this repair are running,
      //   so `nodesReadyForNewRepair(..)` should always also be called with this method
      segments = Sets.newHashSet(context.storage.getSegmentsWithState(repairRunId, RepairSegment.State.RUNNING));
      segments.addAll(context.storage.getSegmentsWithState(repairRunId, RepairSegment.State.STARTED));
    }

    for (RepairSegment seg : segments) {
      // incremental repairs only one segment is allowed at once (one segment == the full primary range of one node)
      if (repairUnit.getIncrementalRepair() || hasReplicaInNodes(cluster, keyspace, seg, nodes)) {

        String msg = String.format(
            "postponed repair segment %s because segment %s is running on host %s",
            segment.getId(), seg.getId(), seg.getCoordinatorHost());

        repairRunner.updateLastEvent(msg);
        return true;
      }
    }
    return false;
  }

  private Collection<String> getNodesInvolvedInSegment(Map<String, String> dcByNode) {
    Set<String> datacenters = repairUnit.getDatacenters();

    return dcByNode.keySet().stream()
        .filter(node -> datacenters.isEmpty() || datacenters.contains(dcByNode.get(node)))
        .collect(Collectors.toList());
  }

  private boolean hasReplicaInNodes(
      Cluster cluster,
      String keyspace,
      RepairSegment segment,
      Collection<String> nodes) {

    return !Collections.disjoint(
        clusterFacade.tokenRangeToEndpoint(cluster, keyspace, segment.getTokenRange()),
        nodes);
  }

  private Map<String, String> getDCsByNodeForRepairSegment(
      JmxProxy coordinator,
      Cluster cluster,
      RepairSegment segment,
      String keyspace) {

    // when hosts are coming up or going down, this method can throw an UndeclaredThrowableException
    Collection<String> nodes = clusterFacade.tokenRangeToEndpoint(cluster, keyspace, segment.getTokenRange());
    Map<String, String> dcByNode = Maps.newHashMap();
    nodes.forEach(node -> dcByNode.put(node, EndpointSnitchInfoProxy.create(coordinator).getDataCenter(node)));
    return dcByNode;
  }

  private void storeNodeMetrics(NodeMetrics metrics) {
    assert context.storage instanceof IDistributedStorage;
    if (DatacenterAvailability.ALL != context.config.getDatacenterAvailability()) {
      ((IDistributedStorage) context.storage).storeNodeMetrics(repairRunner.getRepairRunId(), metrics);
    }
  }

  /**
   * Called when there is an event coming either from JMX or this runner regarding on-going repairs.
   *
   * @param repairNo repair sequence number, obtained when triggering a repair
   * @param status new status of the repair
   * @param message additional information about the repair
   */
  @Override
  public void handle(
      int repairNo,
      Optional<ActiveRepairService.Status> status,
      Optional<ProgressEventType> progress,
      String message,
      JmxProxy jmxProxy) {

    final RepairSegment segment = context.storage.getRepairSegment(repairRunner.getRepairRunId(), segmentId).get();
    Thread.currentThread().setName(clusterName + ":" + segment.getRunId() + ":" + segmentId);
    LOG.debug(
        "handle called for repairCommandId {}, outcome {} / {} and message: {}",
        repairNo,
        status,
        progress,
        message);

    Preconditions.checkArgument(
        repairNo == this.repairNo,
        "Handler for command id %s not handling message %s with number %s",
        this.repairNo, (status.isPresent() ? status.get() : progress.get()), repairNo);

    boolean failOutsideSynchronizedBlock = false;
    // DO NOT ADD EXTERNAL CALLS INSIDE THIS SYNCHRONIZED BLOCK (JMX PROXY ETC)
    synchronized (condition) {
      RepairSegment currentSegment = context.storage.getRepairSegment(repairRunner.getRepairRunId(), segmentId).get();

      Preconditions.checkState(
          RepairSegment.State.NOT_STARTED != currentSegment.getState() || successOrFailedNotified.get(),
          "received " + (status.isPresent() ? status.get() : progress.get()) + " on unstarted segment " + segmentId);

      // See status explanations at: https://wiki.apache.org/cassandra/RepairAsyncAPI
      // Old repair API – up to Cassandra-2.1.x
      if (status.isPresent()) {
        failOutsideSynchronizedBlock = handleJmxNotificationForCassandra21(
            status,
            currentSegment,
            repairNo,
            failOutsideSynchronizedBlock,
            progress,
            jmxProxy);
      }
      // New repair API – Cassandra-2.2 onwards
      if (progress.isPresent()) {
        failOutsideSynchronizedBlock = handleJmxNotificationForCassandra22(
            progress,
            currentSegment,
            repairNo,
            failOutsideSynchronizedBlock,
            jmxProxy);
      }
    }

    if (failOutsideSynchronizedBlock) {
      if (takeLead() || renewLead()) {
        try {
          postponeCurrentSegment();
          tryClearSnapshots(message);
        } finally {
          // if someone else does hold the lease, ie renewLead(..) was true,
          // then their writes to repair_run table and any call to releaseLead(..) will throw an exception
          try {
            releaseLead();
          } catch (AssertionError ignore) { }
        }
      }
    }
  }

  private boolean handleJmxNotificationForCassandra22(
      Optional<ProgressEventType> progress,
      RepairSegment currentSegment,
      int repairNumber,
      boolean failOutsideSynchronizedBlock,
      JmxProxy jmxProxy) {

    switch (progress.get()) {
      case START:
        try {
          // avoid changing state to RUNNING if later notifications have already arrived
          if (!successOrFailedNotified.get()
              && RepairSegment.State.STARTED == currentSegment.getState()
              && renewLead()) {

            context.storage.updateRepairSegment(
                currentSegment
                    .with()
                    .withState(RepairSegment.State.RUNNING)
                    .withId(segmentId)
                    .build());

            LOG.debug("updated segment {} with state {}", segmentId, RepairSegment.State.RUNNING);
            break;
          }
        } catch (AssertionError er) {
          // ignore. segment repair has since timed out.
        }
        segmentFailed.set(true);
        break;

      case SUCCESS:

        Preconditions.checkState(
            !successOrFailedNotified.get(),
            "illegal multiple 'SUCCESS' and 'FAILURE', %s:%s", repairRunner.getRepairRunId(), segmentId);

        try {
          if (segmentFailed.get()) {
            LOG.debug(
                "Got SUCCESS for segment with id '{}' and repair number '{}', but it had already timed out",
                segmentId,
                repairNumber);
          } else if (renewLead()) {
            LOG.debug(
                "repair session succeeded for segment with id '{}' and repair number '{}'",
                segmentId,
                repairNumber);

            context.storage.updateRepairSegment(
                currentSegment
                    .with()
                    .withState(RepairSegment.State.DONE)
                    .withEndTime(DateTime.now())
                    .withId(segmentId)
                    .build());

            successOrFailedNotified.set(true);
            // Since we can get out of order notifications,
            // we need to exit if we already got the COMPLETE notification.
            if (completeNotified.get()) {
              condition.signalAll();
              jmxProxy.removeRepairStatusHandler(repairNumber);
            }
            break;
          }
        } catch (AssertionError er) {
          // ignore. segment repair has since timed out.
        }
        segmentFailed.set(true);
        break;

      case ERROR:
      case ABORT:

        Preconditions.checkState(
            !successOrFailedNotified.get(),
            "illegal multiple 'SUCCESS' and 'FAILURE', %s:%s", repairRunner.getRepairRunId(), segmentId);

        LOG.warn(
            "repair session failed for segment with id '{}' and repair number '{}'",
            segmentId,
            repairNumber);
        failOutsideSynchronizedBlock = true;
        successOrFailedNotified.set(true);
        // Since we can get out of order notifications,
        // we need to exit if we already got the COMPLETE notification.
        if (completeNotified.get()) {
          condition.signalAll();
          jmxProxy.removeRepairStatusHandler(repairNumber);
        }
        break;

      case COMPLETE:
        // This gets called through the JMX proxy at the end
        // regardless of succeeded or failed sessions.
        // Since we can get out of order notifications,
        // we won't exit unless we already got a SUCCESS or ERROR notification.

        Preconditions.checkState(
            !completeNotified.get(),
            "illegal multiple 'COMPLETE', %s:%s", repairRunner.getRepairRunId(), segmentId);

        LOG.debug(
            "repair session finished for segment with id '{}' and repair number '{}'",
            segmentId,
            repairNumber);
        completeNotified.set(true);
        if (successOrFailedNotified.get()) {
          condition.signalAll();
          jmxProxy.removeRepairStatusHandler(repairNumber);
        }
        break;
      default:
        LOG.debug(
            "Unidentified progressStatus {} for segment with id '{}' and repair number '{}'",
            progress.get(),
            segmentId,
            repairNumber);
    }
    return failOutsideSynchronizedBlock;
  }

  private boolean handleJmxNotificationForCassandra21(
      Optional<ActiveRepairService.Status> status,
      RepairSegment currentSegment,
      int repairNumber,
      boolean failOutsideSynchronizedBlock,
      Optional<ProgressEventType> progress,
      JmxProxy jmxProxy) {

    switch (status.get()) {
      case STARTED:
        try {
          // avoid changing state to RUNNING if later notifications have already arrived
          if (!successOrFailedNotified.get()
              && RepairSegment.State.STARTED == currentSegment.getState()
              && renewLead()) {

            context.storage.updateRepairSegment(
                currentSegment
                    .with()
                    .withState(RepairSegment.State.RUNNING)
                    .withId(segmentId)
                    .build());

            LOG.debug("updated segment {} with state {}", segmentId, RepairSegment.State.RUNNING);
            break;
          }
        } catch (AssertionError er) {
          // ignore. segment repair has since timed out.
        }
        segmentFailed.set(true);
        break;

      case SESSION_SUCCESS:
        // Cassandra 2.1 sends several SUCCESS/FAILED notifications during incremental repair
        if (!(repairUnit.getIncrementalRepair() && successOrFailedNotified.get())) {
          Preconditions.checkState(
              !successOrFailedNotified.get(),
              "illegal multiple 'SUCCESS' and 'FAILURE', %s:%s",
              repairRunner.getRepairRunId(),
              segmentId);

          try {
            if (segmentFailed.get()) {
              LOG.debug(
                  "Got SESSION_SUCCESS for segment with id '{}' and repair number '{}', but it had already timed out",
                  segmentId,
                  repairNumber);
            } else if (renewLead()) {
              LOG.debug(
                  "repair session succeeded for segment with id '{}' and repair number '{}'",
                  segmentId,
                  repairNumber);

              context.storage.updateRepairSegment(
                  currentSegment
                      .with()
                      .withState(RepairSegment.State.DONE)
                      .withEndTime(DateTime.now())
                      .withId(segmentId)
                      .build());

              // Since we can get out of order notifications,
              // we need to exit if we already got the COMPLETE notification.
              successOrFailedNotified.set(true);
              if (completeNotified.get()) {
                condition.signalAll();
                jmxProxy.removeRepairStatusHandler(repairNumber);
              }

              break;
            }
          } catch (AssertionError er) {
            // ignore. segment repair has since timed out.
          }
          segmentFailed.set(true);
          break;
        }
        break;

      case SESSION_FAILED:
        // Cassandra 2.1 sends several SUCCESS/FAILED notifications during incremental repair
        if (!(repairUnit.getIncrementalRepair() && successOrFailedNotified.get())) {
          Preconditions.checkState(
              !successOrFailedNotified.get(),
              "illegal multiple 'SUCCESS' and 'FAILURE', %s:%s",
              repairRunner.getRepairRunId(),
              segmentId);

          LOG.warn(
              "repair session failed for segment with id '{}' and repair number '{}'",
              segmentId,
              repairNumber);
          failOutsideSynchronizedBlock = true;
          // Since we can get out of order notifications,
          // we need to exit if we already got the COMPLETE notification.
          successOrFailedNotified.set(true);
          if (completeNotified.get()) {
            condition.signalAll();
            jmxProxy.removeRepairStatusHandler(repairNumber);
          }
          break;
        }
        break;

      case FINISHED:

        Preconditions.checkState(
            !completeNotified.get(),
            "illegal multiple 'COMPLETE', %s:%s", repairRunner.getRepairRunId(), segmentId);

        // This gets called through the JMX proxy at the end
        // regardless of succeeded or failed sessions.
        // Since we can get out of order notifications,
        // we won't exit unless we already got a SUCCESS or ERROR notification.
        LOG.debug(
            "repair session finished for segment with id '{}' and repair number '{}'",
            segmentId,
            repairNumber);
        if (successOrFailedNotified.get()) {
          condition.signalAll();
          jmxProxy.removeRepairStatusHandler(repairNumber);
        }
        break;
      default:
        LOG.debug(
            "Unidentified progressStatus {} for segment with id '{}' and repair number '{}'",
            progress.get(),
            segmentId,
            repairNumber);
    }
    return failOutsideSynchronizedBlock;
  }

  /**
   * Attempts to clear snapshots that are possibly left behind after failed repair sessions.
   */
  void tryClearSnapshots(String message) {
    String keyspace = repairUnit.getKeyspaceName();
    String repairId = parseRepairId(message);
    if (repairId != null) {
      for (String involvedNode : potentialCoordinators) {
        try {
          JmxProxy jmx
              = clusterFacade.connectAny(context.storage.getCluster(clusterName).get(), Arrays.asList(involvedNode));

          // there is no way of telling if the snapshot was cleared or not :(
          SnapshotProxy.create(jmx).clearSnapshot(repairId, keyspace);
        } catch (ReaperException | NumberFormatException e) {
          LOG.warn(
              "Failed to clear snapshot after failed session for host {}, keyspace {}: {}",
              involvedNode,
              keyspace,
              e.getMessage(),
              e);
        }
      }
    }
  }

  static String parseRepairId(String message) {
    Matcher uuidMatcher = REPAIR_UUID_PATTERN.matcher(message);
    if (uuidMatcher.find()) {
      return uuidMatcher.group();
    } else {
      return null;
    }
  }

  /**
   * Calculate the delay that should be used before starting the next repair segment.
   *
   * @return the delay in milliseconds.
   */
  long intensityBasedDelayMillis(double intensity) {
    RepairSegment repairSegment = context.storage.getRepairSegment(repairRunner.getRepairRunId(), segmentId).get();
    if (repairSegment.getEndTime() == null && repairSegment.getStartTime() == null) {
      return 0;
    } else if (repairSegment.getEndTime() != null && repairSegment.getStartTime() != null) {
      long repairEnd = repairSegment.getEndTime().getMillis();
      long repairStart = repairSegment.getStartTime().getMillis();
      long repairDuration = Math.max(1, repairEnd - repairStart);
      long delay = (long) (repairDuration / intensity - repairDuration);
      LOG.debug("Scheduling next runner run() with delay {} ms", delay);
      int nbRunningReapers = countRunningReapers();
      LOG.debug("Concurrent reaper instances : {}", nbRunningReapers);
      return delay * nbRunningReapers;
    } else {
      LOG.error(
          "Segment {} returned with startTime {} and endTime {}. This should not happen."
          + "Intensity cannot apply, so next run will start immediately.",
          repairSegment.getId(),
          repairSegment.getStartTime(),
          repairSegment.getEndTime());
      return 0;
    }
  }

  private boolean takeLead() {
    try (Timer.Context cx
        = context.metricRegistry.timer(MetricRegistry.name(SegmentRunner.class, "takeLead")).time()) {

      boolean result = context.storage instanceof IDistributedStorage
          ? ((IDistributedStorage) context.storage).takeLead(leaderElectionId)
          : true;

      if (!result) {
        context.metricRegistry.counter(MetricRegistry.name(SegmentRunner.class, "takeLead", "failed")).inc();
      }
      return result;
    }
  }

  private boolean renewLead() {
    try (Timer.Context cx
        = context.metricRegistry.timer(MetricRegistry.name(SegmentRunner.class, "renewLead")).time()) {

      boolean result = context.storage instanceof IDistributedStorage
          ? ((IDistributedStorage) context.storage).renewLead(leaderElectionId)
          : true;

      if (!result) {
        context.metricRegistry.counter(MetricRegistry.name(SegmentRunner.class, "renewLead", "failed")).inc();
      }
      return result;
    }
  }

  private void releaseLead() {
    try (Timer.Context cx
        = context.metricRegistry.timer(MetricRegistry.name(SegmentRunner.class, "releaseLead")).time()) {
      if (context.storage instanceof IDistributedStorage) {
        ((IDistributedStorage) context.storage).releaseLead(leaderElectionId);
      }
    }
  }

  private int countRunningReapers() {
    return context.storage instanceof IDistributedStorage
        ? ((IDistributedStorage) context.storage).countRunningReapers()
        : 1;
  }

  private class BusyHostsInitializer extends LazyInitializer<Set<String>> {

    private final Cluster cluster;

    BusyHostsInitializer(Cluster cluster) {
      this.cluster = cluster;
    }

    @Override
    protected Set<String> initialize() {
      Collection<RepairParameters> ongoingRepairs = context.storage.getOngoingRepairsInCluster(clusterName);
      Set<String> busyHosts = Sets.newHashSet();
      ongoingRepairs.forEach(
          (ongoingRepair) -> {
            busyHosts.addAll(
                clusterFacade.tokenRangeToEndpoint(
                    cluster, ongoingRepair.keyspaceName, ongoingRepair.tokenRange));
          });
      return busyHosts;
    }
  }

}
