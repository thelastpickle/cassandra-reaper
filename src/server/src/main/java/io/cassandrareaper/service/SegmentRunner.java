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

package io.cassandrareaper.service;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration.DatacenterAvailability;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.NodeMetrics;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.jmx.JmxProxy;
import io.cassandrareaper.jmx.RepairStatusHandler;
import io.cassandrareaper.storage.IDistributedStorage;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sun.management.UnixOperatingSystemMXBean;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.progress.ProgressEventType;
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

  private static final int MAX_PENDING_COMPACTIONS = 20;
  private static final int MAX_TIMEOUT_EXTENSIONS = 10;
  private static final Pattern REPAIR_UUID_PATTERN
      = Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");

  private static final long SLEEP_TIME_AFTER_POSTPONE_IN_MS = 10000;
  private static final ExecutorService METRICS_GRABBER_EXECUTOR = Executors.newFixedThreadPool(10);
  private static final long METRICS_POLL_INTERVAL_MS = TimeUnit.SECONDS.toMillis(10);
  private static final long METRICS_MAX_WAIT_MS = TimeUnit.MINUTES.toMillis(2);

  private final AppContext context;
  private final UUID segmentId;
  private final Condition condition = new SimpleCondition();
  private final Collection<String> potentialCoordinators;
  private final long timeoutMillis;
  private final double intensity;
  private final RepairParallelism validationParallelism;
  private final String clusterName;
  private final RepairRunner repairRunner;
  private final RepairUnit repairUnit;
  private int commandId;
  private final AtomicBoolean segmentFailed;
  private final UUID leaderElectionId;


  SegmentRunner(
      AppContext context,
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

  @Override
  public void run() {
    if (takeLead()) {
      try {
        if (runRepair()) {
          long delay = intensityBasedDelayMillis(intensity);
          try {
            Thread.sleep(delay);
          } catch (InterruptedException e) {
            LOG.warn("Slept shorter than intended delay.");
          }
        }
      } finally {
        releaseLead();
      }
    }
  }

  private static void postpone(AppContext context, RepairSegment segment, Optional<RepairUnit> repairUnit) {
    LOG.info("Postponing segment {}", segment.getId());
    try {
      context.storage.updateRepairSegment(
          segment
              .with()
              .state(RepairSegment.State.NOT_STARTED)
              .coordinatorHost(
                  repairUnit.isPresent() && repairUnit.get().getIncrementalRepair()
                  ? segment.getCoordinatorHost()
                  : null) // set coordinator host to null only for full repairs
              .startTime(null)
              .failCount(segment.getFailCount() + 1)
              .build(segment.getId()));
    } finally {
      SEGMENT_RUNNERS.remove(segment.getId());
      context.metricRegistry.counter(metricNameForPostpone(repairUnit, segment)).inc();
    }
  }

  static void abort(AppContext context, RepairSegment segment, JmxProxy jmxConnection) {
    postpone(context, segment, context.storage.getRepairUnit(segment.getRepairUnitId()));
    LOG.info("Aborting repair on segment with id {} on coordinator {}", segment.getId(), segment.getCoordinatorHost());
    String metric = MetricRegistry.name(SegmentRunner.class, "abort", segment.getCoordinatorHost());
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
    final RepairSegment segment = context.storage.getRepairSegment(repairRunner.getRepairRunId(), segmentId).get();
    Thread.currentThread().setName(clusterName + ":" + segment.getRunId() + ":" + segmentId);

    try (Timer.Context cxt = context.metricRegistry.timer(metricNameForRunRepair(segment)).time();
        JmxProxy coordinator = context.jmxConnectionFactory.connectAny(
            Optional.<RepairStatusHandler>fromNullable(this),
            potentialCoordinators,
            context.config.getJmxConnectionTimeoutInSeconds())) {

      if (SEGMENT_RUNNERS.containsKey(segmentId)) {
        LOG.error("SegmentRunner already exists for segment with ID: {}", segmentId);
        closeJmxConnection(Optional.fromNullable(coordinator));
        throw new ReaperException("SegmentRunner already exists for segment with ID: " + segmentId);
      }
      SEGMENT_RUNNERS.put(segmentId, this);
      String keyspace = repairUnit.getKeyspaceName();
      boolean fullRepair = !repairUnit.getIncrementalRepair();

      LazyInitializer<Set<String>> busyHosts = new LazyInitializer<Set<String>>() {
        @Override
        protected Set<String> initialize() {
          Collection<RepairParameters> ongoingRepairs = context.storage.getOngoingRepairsInCluster(clusterName);
          Set<String> busyHosts = Sets.newHashSet();
          for (RepairParameters ongoingRepair : ongoingRepairs) {
            busyHosts.addAll(
                coordinator.tokenRangeToEndpoint(ongoingRepair.keyspaceName, ongoingRepair.tokenRange));
          }
          return busyHosts;
        }
      };
      if (!canRepair(segment, keyspace, coordinator, busyHosts)) {
        postponeCurrentSegment();
        closeJmxConnection(Optional.fromNullable(coordinator));
        return false;
      }

      try (Timer.Context cxt1 = context.metricRegistry.timer(metricNameForRepairing(segment)).time()) {
        Set<String> tablesToRepair;
        try {
          tablesToRepair = getTablesToRepair(coordinator, repairUnit);
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
                  .state(RepairSegment.State.DONE)
                  .endTime(DateTime.now())
                  .build(segmentId));
          return false;

        }

        LOG.debug("Enter synchronized section with segment ID {}", segmentId);
        synchronized (condition) {
          commandId = coordinator.triggerRepair(
              segment.getStartToken(),
              segment.getEndToken(),
              keyspace,
              validationParallelism,
              tablesToRepair,
              fullRepair,
              repairUnit.getDatacenters());

          if (commandId == 0) {
            LOG.info("Nothing to repair for keyspace {}", keyspace);
            context.storage.updateRepairSegment(
                segment.with().coordinatorHost(coordinator.getHost()).state(RepairSegment.State.DONE).build(segmentId));
            SEGMENT_RUNNERS.remove(segment.getId());
            closeJmxConnection(Optional.fromNullable(coordinator));
            return true;
          }

          long timeout = repairUnit.getIncrementalRepair() ? timeoutMillis * MAX_TIMEOUT_EXTENSIONS : timeoutMillis;
          context.storage.updateRepairSegment(segment.with().coordinatorHost(coordinator.getHost()).build(segmentId));
          String eventMsg
              = String.format("Triggered repair of segment %s via host %s", segment.getId(), coordinator.getHost());

          repairRunner.updateLastEvent(eventMsg);
          LOG.info("Repair for segment {} started, status wait will timeout in {} millis", segmentId, timeout);
          try {
            long startTime = System.currentTimeMillis();
            long maxTime = startTime + timeoutMillis;
            long waitTime = timeoutMillis < 60000 ? timeoutMillis : 60000;
            long lastLoopTime = System.currentTimeMillis();
            while (System.currentTimeMillis() < maxTime) {
              condition.await(waitTime, TimeUnit.MILLISECONDS);
              if (lastLoopTime + 60_000 > System.currentTimeMillis()
                  || context.storage.getRepairSegment(segment.getRunId(), segmentId).get().getState()
                        == RepairSegment.State.DONE) {
                break;
              }

              renewLead();
              lastLoopTime = System.currentTimeMillis();
            }
          } catch (InterruptedException e) {
            LOG.warn("Repair command {} on segment {} interrupted", commandId, segmentId, e);
          } finally {
            RepairSegment resultingSegment
                = context.storage.getRepairSegment(repairRunner.getRepairRunId(), segmentId).get();
            LOG.info(
                "Repair command {} on segment {} returned with state {}",
                commandId,
                segmentId,
                resultingSegment.getState());
            if (resultingSegment.getState() == RepairSegment.State.RUNNING) {
              LOG.info("Repair command {} on segment {} has been cancelled while running", commandId, segmentId);
              segmentFailed.set(true);
              abort(resultingSegment, coordinator);
            } else if (resultingSegment.getState() == RepairSegment.State.DONE) {
              LOG.debug(
                  "Repair segment with id '{}' was repaired in {} seconds",
                  resultingSegment.getId(),
                  Seconds.secondsBetween(resultingSegment.getStartTime(), resultingSegment.getEndTime()).getSeconds());

              SEGMENT_RUNNERS.remove(resultingSegment.getId());
            }
            // Repair is still running, we'll renew lead on the segment when using Cassandra as storage backend
            renewLead();
          }
        }
        closeJmxConnection(Optional.fromNullable(coordinator));
      }
    } catch (ReaperException e) {
      LOG.warn("Failed to connect to a coordinator node for segment {}", segmentId, e);
      String msg = "Postponed a segment because no coordinator was reachable";
      repairRunner.updateLastEvent(msg);
      postponeCurrentSegment();
      LOG.warn("Open files amount for process: " + getOpenFilesAmount());
      return false;
    } finally {
      context.metricRegistry
          .histogram(MetricRegistry.name(SegmentRunner.class, "open-files"))
          .update(getOpenFilesAmount());
    }
    LOG.debug("Exiting synchronized section with segment ID {}", segmentId);
    return true;
  }

  private static String metricNameForPostpone(Optional<RepairUnit> unit, RepairSegment segment) {
    return unit.isPresent()
        ? MetricRegistry.name(
            SegmentRunner.class,
            "postpone",
            Optional.fromNullable(segment.getCoordinatorHost()).or("null").replace('.', '-'),
            unit.get().getClusterName(),
            unit.get().getKeyspaceName())
        : MetricRegistry.name(
            SegmentRunner.class,
            "postpone",
            Optional.fromNullable(segment.getCoordinatorHost()).or("null").replace('.', '-'));
  }

  private String metricNameForRepairing(RepairSegment rs) {
    return MetricRegistry.name(
        SegmentRunner.class,
        "repairing",
        Optional.fromNullable(rs.getCoordinatorHost()).or("null").replace('.', '-'),
        clusterName,
        repairUnit.getKeyspaceName());
  }

  private String metricNameForRunRepair(RepairSegment rs) {
    return MetricRegistry.name(
        SegmentRunner.class,
        "runRepair",
        Optional.fromNullable(rs.getCoordinatorHost()).or("null").replace('.', '-'),
        clusterName,
        repairUnit.getKeyspaceName());
  }

  private void closeJmxConnection(Optional<JmxProxy> jmxProxy) {
    if (jmxProxy.isPresent()) {
      jmxProxy.get().close();
    }
  }

  private void declineRun() {
    LOG.info(
        "SegmentRunner declined to repair segment {} "
            + "because only one segment is allowed at once for incremental repairs",
        segmentId);

    String msg = "Postponed due to already running segment";
    repairRunner.updateLastEvent(msg);
  }

  boolean canRepair(
      RepairSegment segment,
      String keyspace,
      JmxProxy coordinator,
      LazyInitializer<Set<String>> busyHosts) {

    Collection<String> allHosts;

    if (repairUnit.getIncrementalRepair()) {
      // In incremental repairs, only one segment is allowed at once (one segment == the full primary range of one node)
      if (repairHasSegmentRunning(segment.getRunId())) {
        declineRun();
        return false;
      }
      if (isRepairRunningOnOneNode(segment)) {
        declineRun();
        return false;
      }

      return true;
    }
    try {
      // when hosts are coming up or going down, this method can throw an
      //  UndeclaredThrowableException
      allHosts = coordinator.tokenRangeToEndpoint(keyspace, segment.getTokenRange());
    } catch (RuntimeException e) {
      LOG.warn("SegmentRunner couldn't get token ranges from coordinator: ", e);
      String msg = "SegmentRunner couldn't get token ranges from coordinator";
      repairRunner.updateLastEvent(msg);
      return false;
    }
    String datacenter = coordinator.getDataCenter();
    boolean gotMetricsForAllHostsInDc = true;
    boolean gotMetricsForAllHosts = true;
    Map<String, String> dcByHost = Maps.newHashMap();
    allHosts.forEach(host -> dcByHost.put(host, coordinator.getDataCenter(host)));

    List<Callable<Pair<String, Optional<NodeMetrics>>>> getMetricsTasks = allHosts
        .stream()
        .filter(
            host
              -> repairUnit.getDatacenters().isEmpty() || repairUnit.getDatacenters().contains(dcByHost.get(host)))
        .map(host -> getNodeMetrics(host, datacenter, dcByHost.get(host)))
        .collect(Collectors.toList());

    try {
      for (Future<Pair<String, Optional<NodeMetrics>>> future : METRICS_GRABBER_EXECUTOR.invokeAll(getMetricsTasks)) {
        try {
          Pair<String, Optional<NodeMetrics>> result = future.get();
          if (!result.getRight().isPresent()) {
            // We failed at getting metrics for that node
            gotMetricsForAllHosts = false;
            if (dcByHost.get(result.getLeft()).equals(datacenter)) {
              gotMetricsForAllHostsInDc = false;
            }
          } else {
            NodeMetrics metrics = result.getRight().get();
            int pendingCompactions = metrics.getPendingCompactions();
            if (pendingCompactions > MAX_PENDING_COMPACTIONS) {
              LOG.info(
                  "SegmentRunner declined to repair segment {} because of"
                      + " too many pending compactions (> {}) on host \"{}\"",
                  segmentId,
                  MAX_PENDING_COMPACTIONS,
                  metrics.getHostAddress());
              String msg = String.format("Postponed due to pending compactions (%d)", pendingCompactions);
              repairRunner.updateLastEvent(msg);
              return false;
            }
            if (metrics.hasRepairRunning()) {
              LOG.info(
                  "SegmentRunner declined to repair segment {} because one of the hosts ({}) was "
                  + "already involved in a repair",
                  segmentId,
                  metrics.getHostAddress());
              String msg = "Postponed due to affected hosts already doing repairs";
              repairRunner.updateLastEvent(msg);
              handlePotentialStuckRepairs(busyHosts, metrics.getHostAddress());
              return false;
            }
          }
        } catch (InterruptedException | ExecutionException | ConcurrentException e) {
          LOG.warn("Failed grabbing metrics from at least one node. Cannot repair segment :'(", e);
          gotMetricsForAllHostsInDc = false;
          gotMetricsForAllHosts = false;
        }
      }
    } catch (InterruptedException e) {
      LOG.debug("failed grabbing nodes metrics", e);
    }

    if (okToRepairSegment(
        gotMetricsForAllHostsInDc, gotMetricsForAllHosts, context.config.getDatacenterAvailability())) {
      LOG.info("It is ok to repair segment '{}' on repair run with id '{}'", segment.getId(), segment.getRunId());
      return true;
    } else {
      LOG.info(
          "Not ok to repair segment '{}' on repair run with id '{}' because we couldn't get all hosts metrics :'(",
          segment.getId(),
          segment.getRunId());
      return false;
    }
  }

  static boolean okToRepairSegment(
      boolean allHostsInLocalDc,
      boolean allHosts,
      DatacenterAvailability dcAvailability) {

    return allHosts || (allHostsInLocalDc && DatacenterAvailability.LOCAL.equals(dcAvailability));
  }

  private void handlePotentialStuckRepairs(LazyInitializer<Set<String>> busyHosts, String hostName)
      throws ConcurrentException {

    if (!busyHosts.get().contains(hostName) && context.storage instanceof IDistributedStorage) {
      try (JmxProxy hostProxy
          = context.jmxConnectionFactory.connect(hostName, context.config.getJmxConnectionTimeoutInSeconds())) {
        // We double check that repair is still running there before actually canceling repairs
        if (hostProxy.isRepairRunning()) {
          LOG.warn(
              "A host ({}) reported that it is involved in a repair, but there is no record "
                  + "of any ongoing repair involving the host. Sending command to abort all repairs "
                  + "on the host.",
              hostName);
          hostProxy.cancelAllRepairs();
          hostProxy.close();
        }
      } catch (ReaperException | RuntimeException | InterruptedException | JMException e) {
        LOG.debug("failed to cancel repairs on host {}", hostName, e);
      }
    }
  }

  Callable<Pair<String, Optional<NodeMetrics>>> getNodeMetrics(
      String hostName,
      String localDatacenter,
      String hostDatacenter) {

    return () -> {
      LOG.debug("getMetricsForHost {} / {} / {}", hostName, localDatacenter, hostDatacenter);
      try (JmxProxy hostProxy
          = context.jmxConnectionFactory.connect(hostName, context.config.getJmxConnectionTimeoutInSeconds())) {

        int pendingCompactions = hostProxy.getPendingCompactions();
        boolean hasRepairRunning = hostProxy.isRepairRunning();

        NodeMetrics metrics = NodeMetrics.builder()
            .withHostAddress(hostName)
            .withDatacenter(hostDatacenter)
            .withPendingCompactions(pendingCompactions)
            .withHasRepairRunning(hasRepairRunning)
            .withActiveAnticompactions(0) // for future use
            .build();

        return Pair.of(hostName, Optional.of(metrics));
      } catch (RuntimeException | ReaperException e) {
        LOG.debug("failed to query metrics for host {}, trying to get metrics from storage...", hostName, e);
        if (!DatacenterAvailability.ALL.equals(context.config.getDatacenterAvailability())
            && !hostDatacenter.equals(localDatacenter)) {

          // We can get metrics for remote datacenters from storage
          Optional<NodeMetrics> metrics = getRemoteNodeMetrics(hostName, hostDatacenter);
          if (metrics.isPresent()) {
            return Pair.of(hostName, metrics);
          }
        }
        return Pair.of(hostName, Optional.absent());
      }
    };
  }

  private Optional<NodeMetrics> getRemoteNodeMetrics(String hostName, String hostDatacenter) {
    Preconditions.checkState(!DatacenterAvailability.ALL.equals(context.config.getDatacenterAvailability()));

    Optional<NodeMetrics> result = Optional.absent();
    if (context.storage instanceof IDistributedStorage) {
      IDistributedStorage storage = ((IDistributedStorage) context.storage);
      result = storage.getNodeMetrics(repairRunner.getRepairRunId(), hostName);

      if (!result.isPresent() && DatacenterAvailability.EACH.equals(context.config.getDatacenterAvailability())) {
        // Sending a request for metrics to the other reaper instances through the Cassandra backend
        storeNodeMetrics(
            NodeMetrics.builder()
                .withCluster(clusterName)
                .withDatacenter(hostDatacenter)
                .withHostAddress(hostName)
                .withRequested(true)
                .build());

        long start = System.currentTimeMillis();

        while ( (!result.isPresent() || result.get().isRequested())
            && start + METRICS_MAX_WAIT_MS > System.currentTimeMillis()) {

          try {
            Thread.sleep(METRICS_POLL_INTERVAL_MS);
          } catch (InterruptedException ignore) { }
          LOG.info("Trying to get metrics from remote DCs for {} in {} of {}", hostName, hostDatacenter, clusterName);
          result = storage.getNodeMetrics(repairRunner.getRepairRunId(), hostName);
        }
      }
    }
    return result;
  }

  private boolean isRepairRunningOnOneNode(RepairSegment segment) {
    for (RepairSegment segmentInRun : context.storage.getRepairSegmentsForRun(segment.getRunId())) {
      try (JmxProxy hostProxy = context.jmxConnectionFactory.connect(
          segmentInRun.getCoordinatorHost(), context.config.getJmxConnectionTimeoutInSeconds())) {

        if (hostProxy.isRepairRunning()) {
          return true;
        }
      } catch (ReaperException | JMException | NumberFormatException | InterruptedException e) {
        LOG.error(
            "Unreachable node when trying to determine if repair is running on a node."
                + " Crossing fingers and continuing...",
            e);
      }
    }

    return false;
  }

  private boolean repairHasSegmentRunning(UUID repairRunId) {
    Collection<RepairSegment> segments = context.storage.getRepairSegmentsForRun(repairRunId);
    for (RepairSegment segment : segments) {
      if (segment.getState() == RepairSegment.State.RUNNING) {
        LOG.info("segment '{}' is running on host '{}'", segment.getId(), segment.getCoordinatorHost());
        return true;
      }
    }

    return false;
  }

  private void storeNodeMetrics(NodeMetrics metrics) {
    if (context.storage instanceof IDistributedStorage
        && !DatacenterAvailability.ALL.equals(context.config.getDatacenterAvailability())) {

      ((IDistributedStorage) context.storage).storeNodeMetrics(repairRunner.getRepairRunId(), metrics);
    }
  }

  /**
   * Called when there is an event coming either from JMX or this runner regarding on-going repairs.
   *
   * @param repairNumber repair sequence number, obtained when triggering a repair
   * @param status new status of the repair
   * @param message additional information about the repair
   */
  @Override
  public void handle(
      int repairNumber,
      Optional<ActiveRepairService.Status> status,
      Optional<ProgressEventType> progress,
      String message) {

    final RepairSegment segment = context.storage.getRepairSegment(repairRunner.getRepairRunId(), segmentId).get();
    Thread.currentThread().setName(clusterName + ":" + segment.getRunId() + ":" + segmentId);
    LOG.debug(
        "handle called for repairCommandId {}, outcome {} / {} and message: {}",
        repairNumber,
        status,
        progress,
        message);
    if (repairNumber != commandId) {
      LOG.debug("Handler for command id {} not handling message with number {}", commandId, repairNumber);
      return;
    }

    boolean failOutsideSynchronizedBlock = false;
    // DO NOT ADD EXTERNAL CALLS INSIDE THIS SYNCHRONIZED BLOCK (JMX PROXY ETC)
    synchronized (condition) {
      RepairSegment currentSegment = context.storage.getRepairSegment(repairRunner.getRepairRunId(), segmentId).get();
      // See status explanations at: https://wiki.apache.org/cassandra/RepairAsyncAPI
      // Old repair API – up to Cassandra-2.1.x
      if (status.isPresent()) {
        failOutsideSynchronizedBlock = handleJmxNotificationForCassandra21(
            status,
            currentSegment,
            repairNumber,
            failOutsideSynchronizedBlock,
            progress);
      }
      // New repair API – Cassandra-2.2 onwards
      if (progress.isPresent()) {
        failOutsideSynchronizedBlock = handleJmxNotificationForCassandra22(
            progress,
            currentSegment,
            repairNumber,
            failOutsideSynchronizedBlock);
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
      boolean failOutsideSynchronizedBlock) {

    switch (progress.get()) {
      case START:
        try {
          if (renewLead()) {
            DateTime now = DateTime.now();

            context.storage.updateRepairSegment(
                currentSegment.with().state(RepairSegment.State.RUNNING).startTime(now).build(segmentId));

            LOG.debug("updated segment {} with state {}", segmentId, RepairSegment.State.RUNNING);
            break;
          }
        } catch (AssertionError er) {
          // ignore. segment repair has since timed out.
        }
        segmentFailed.set(true);
        break;

      case SUCCESS:
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
                currentSegment.with().state(RepairSegment.State.DONE).endTime(DateTime.now()).build(segmentId));
            break;
          }
        } catch (AssertionError er) {
          // ignore. segment repair has since timed out.
        }
        segmentFailed.set(true);
        break;

      case ERROR:
      case ABORT:
        LOG.warn("repair session failed for segment with id '{}' and repair number '{}'", segmentId, repairNumber);
        failOutsideSynchronizedBlock = true;
        break;

      case COMPLETE:
        // This gets called through the JMX proxy at the end
        // regardless of succeeded or failed sessions.
        LOG.debug(
            "repair session finished for segment with id '{}' and repair number '{}'", segmentId, repairNumber);
        condition.signalAll();
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
      Optional<ProgressEventType> progress) {

    switch (status.get()) {
      case STARTED:
        try {
          if (renewLead()) {
            DateTime now = DateTime.now();

            context.storage.updateRepairSegment(
                currentSegment.with().state(RepairSegment.State.RUNNING).startTime(now).build(segmentId));

            LOG.debug("updated segment {} with state {}", segmentId, RepairSegment.State.RUNNING);
            break;
          }
        } catch (AssertionError er) {
          // ignore. segment repair has since timed out.
        }
        segmentFailed.set(true);
        break;

      case SESSION_SUCCESS:
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
                currentSegment.with().state(RepairSegment.State.DONE).endTime(DateTime.now()).build(segmentId));

            break;
          }
        } catch (AssertionError er) {
          // ignore. segment repair has since timed out.
        }
        segmentFailed.set(true);
        break;

      case SESSION_FAILED:
        LOG.warn("repair session failed for segment with id '{}' and repair number '{}'", segmentId, repairNumber);
        failOutsideSynchronizedBlock = true;
        break;

      case FINISHED:
        // This gets called through the JMX proxy at the end
        // regardless of succeeded or failed sessions.
        LOG.debug(
            "repair session finished for segment with id '{}' and repair number '{}'", segmentId, repairNumber);
        condition.signalAll();
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
        try (JmxProxy jmx
            = context.jmxConnectionFactory.connect(involvedNode, context.config.getJmxConnectionTimeoutInSeconds())) {
          // there is no way of telling if the snapshot was cleared or not :(
          jmx.clearSnapshot(repairId, keyspace);
          jmx.close();
        } catch (ReaperException | NumberFormatException | InterruptedException e) {
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

  /**
   * Applies blacklist filter on tables for the given repair unit.
   *
   * @param coordinator : a JMX proxy instance
   * @param unit : the repair unit for the current run
   * @return the list of tables to repair for the keyspace without the blacklisted ones
   * @throws ReaperException, IllegalStateException
   */
  static Set<String> getTablesToRepair(JmxProxy coordinator, RepairUnit unit)
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
