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
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairType;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.management.ClusterFacade;
import io.cassandrareaper.management.ICassandraManagementProxy;
import io.cassandrareaper.management.RepairStatusHandler;
import io.cassandrareaper.management.SnapshotProxy;
import io.cassandrareaper.storage.IDistributedStorage;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.sun.management.UnixOperatingSystemMXBean;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.cassandrareaper.metrics.MetricNameUtils.cleanHostName;
import static io.cassandrareaper.metrics.MetricNameUtils.cleanName;

final class SegmentRunner implements RepairStatusHandler, Runnable {

  // Caching all active SegmentRunners.
  static final Map<UUID, SegmentRunner> SEGMENT_RUNNERS = Maps.newConcurrentMap();

  private static final Logger LOG = LoggerFactory.getLogger(SegmentRunner.class);

  private static final int MAX_TIMEOUT_EXTENSIONS = 10;
  private static final Pattern REPAIR_UUID_PATTERN
      = Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");

  private static final long SLEEP_TIME_AFTER_POSTPONE_IN_MS
      = Integer.getInteger(SegmentRunner.class.getName() + ".sleep_time_after_postpone_in_ms", 10000);

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
  private volatile int repairNo;
  private final AtomicBoolean segmentFailed;
  private final UUID leaderElectionId;
  private final AtomicBoolean successOrFailedNotified = new AtomicBoolean(false);
  private final AtomicBoolean completeNotified = new AtomicBoolean(false);
  private final ClusterFacade clusterFacade;
  private final Set<String> tablesToRepair;


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
      Set<String> tablesToRepair,
      RepairRunner repairRunner)
      throws ReaperException {

    if (SEGMENT_RUNNERS.containsKey(segmentId)) {
      LOG.error("SegmentRunner already exists for segment with ID: {}", segmentId);
      throw new ReaperException("SegmentRunner already exists for segment with ID: " + segmentId);
    }
    this.context = context;
    this.clusterFacade = clusterFacade;
    this.segmentId = segmentId;
    this.potentialCoordinators = potentialCoordinators;
    this.timeoutMillis = timeoutMillis;
    this.intensity = intensity;
    this.validationParallelism = validationParallelism;
    this.clusterName = clusterName;
    this.repairUnit = repairUnit;
    this.repairRunner = repairRunner;
    this.segmentFailed = new AtomicBoolean(false);
    this.leaderElectionId = repairUnit.getIncrementalRepair() && !repairUnit.getSubrangeIncrementalRepair()
        ? repairRunner.getRepairRunId()
        : segmentId;
    this.tablesToRepair = tablesToRepair;
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
      Set<String> tablesToRepair,
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
        tablesToRepair,
        repairRunner);
  }

  static void postponeSegment(AppContext context, RepairSegment segment) {
    LOG.info("Reset segment {}", segment.getId());
    RepairUnit unit = context.storage.getRepairUnitDao().getRepairUnit(segment.getRepairUnitId());
    RepairSegment postponed
        = segment
        .reset()
        // set coordinator host to null only for full repairs
        .withCoordinatorHost(unit.getIncrementalRepair() && !unit.getSubrangeIncrementalRepair()
            ? segment.getCoordinatorHost()
            : null)
        .withFailCount(segment.getFailCount() + 1)
        .withId(segment.getId())
        .build();

    context.storage.getRepairSegmentDao().updateRepairSegmentUnsafe(postponed);
  }

  private static void postpone(AppContext context, RepairSegment segment, RepairUnit repairUnit) {
    LOG.info("Postponing segment {}", segment.getId());
    try {
      context.storage.getRepairSegmentDao().updateRepairSegment(
          segment
              .reset()
              // set coordinator host to null only for full repairs
              .withCoordinatorHost(repairUnit.getIncrementalRepair() && !repairUnit.getSubrangeIncrementalRepair()
                  ? segment.getCoordinatorHost()
                  : null)
              .withFailCount(segment.getFailCount() + 1)
              .withId(segment.getId())
              .build());
    } finally {
      SEGMENT_RUNNERS.remove(segment.getId());
      context.metricRegistry.counter(metricNameForPostpone(repairUnit, segment)).inc();
    }
  }

  static void abort(AppContext context, RepairSegment segment, ICassandraManagementProxy jmxConnection) {
    postpone(context, segment, context.storage.getRepairUnitDao().getRepairUnit(segment.getRepairUnitId()));
    LOG.info("Aborting repair on segment with id {} on coordinator {}", segment.getId(), segment.getCoordinatorHost());

    String metric = MetricRegistry.name(
        SegmentRunner.class,
        "abort",
        Optional.ofNullable(segment.getCoordinatorHost()).orElse("null").replace('.', '-'));

    context.metricRegistry.counter(metric).inc();
    jmxConnection.cancelAllRepairs();
  }

  private void abort(RepairSegment segment, ICassandraManagementProxy jmxConnection) {
    abort(context, segment, jmxConnection);
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

  private static String metricNameForPostpone(RepairUnit unit, RepairSegment segment) {
    return MetricRegistry.name(
        SegmentRunner.class,
        "postpone",
        cleanHostName(segment.getCoordinatorHost()),
        cleanName(unit.getClusterName()),
        cleanName(unit.getKeyspaceName()));
  }

  static String parseRepairId(String message) {
    Matcher uuidMatcher = REPAIR_UUID_PATTERN.matcher(message);
    if (uuidMatcher.find()) {
      return uuidMatcher.group();
    } else {
      return null;
    }
  }


  @Override
  public void run() {
    boolean ran = false;
    RepairSegment segment = context.storage.getRepairSegmentDao().getRepairSegment(repairRunner.getRepairRunId(),
        segmentId).get();
    if (takeLead(segment)) {
      try {
        ran = runRepair();
      } finally {
        releaseLead(segment);
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

  /**
   * Remember to call method postponeCurrentSegment() outside of synchronized(condition) block.
   */
  void postponeCurrentSegment() {
    synchronized (condition) {
      RepairSegment segment = context.storage.getRepairSegmentDao().getRepairSegment(repairRunner.getRepairRunId(),
          segmentId).get();
      postpone(context, segment, context.storage.getRepairUnitDao().getRepairUnit(segment.getRepairUnitId()));
    }

    try {
      Thread.sleep(SLEEP_TIME_AFTER_POSTPONE_IN_MS);
    } catch (InterruptedException e) {
      LOG.debug("Interrupted while sleeping after a segment was postponed... weird stuff...");
    }
  }

  private boolean runRepair() {
    LOG.debug("Run repair for segment #{}", segmentId);
    RepairSegment segment = context.storage.getRepairSegmentDao().getRepairSegment(repairRunner.getRepairRunId(),
        segmentId).get();
    Thread.currentThread().setName(clusterName + ":" + segment.getRunId() + ":" + segmentId);

    try (Timer.Context cxt = context.metricRegistry.timer(metricNameForRunRepair(segment)).time()) {
      if (SEGMENT_RUNNERS.containsKey(segmentId)) {
        LOG.error("SegmentRunner already exists for segment with ID: {}", segmentId);
        throw new ReaperException("SegmentRunner already exists for segment with ID: " + segmentId);
      }

      if (RepairSegment.State.NOT_STARTED != segment.getState()) {
        LOG.info(
            "Cannot run segment {} for repair {} at the moment. Will try again later", segmentId, segment.getRunId());

        try {
          Thread.sleep(SLEEP_TIME_AFTER_POSTPONE_IN_MS);
        } catch (InterruptedException ignore) {
        }
        return false;
      }

      Cluster cluster = context.storage.getClusterDao().getCluster(clusterName);
      ICassandraManagementProxy coordinator = clusterFacade.connect(cluster, potentialCoordinators);
      String keyspace = repairUnit.getKeyspaceName();
      RepairType repairType = repairUnit.getSubrangeIncrementalRepair() ? RepairType.SUBRANGE_INCREMENTAL
          : repairUnit.getIncrementalRepair() ? RepairType.INCREMENTAL : RepairType.SUBRANGE_FULL;

      try (Timer.Context cxt1 = context.metricRegistry.timer(metricNameForRepairing(segment)).time()) {
        try {
          LOG.debug("Enter synchronized section with segment ID {}", segmentId);
          synchronized (condition) {
            String coordinatorHost = context.config.getDatacenterAvailability() == DatacenterAvailability.SIDECAR
                ? context.getLocalNodeAddress()
                : coordinator.getHost();
            segment = segment
                .with()
                .withState(RepairSegment.State.STARTED)
                .withCoordinatorHost(coordinatorHost)
                .withStartTime(DateTime.now())
                .withId(segmentId)
                .build();

            context.storage.getRepairSegmentDao().updateRepairSegment(segment);

            repairNo = coordinator.triggerRepair(
                keyspace,
                validationParallelism,
                tablesToRepair,
                repairType,
                repairUnit.getDatacenters(),
                this,
                segment.getTokenRange().getTokenRanges(),
                repairUnit.getRepairThreadCount());

            if (0 != repairNo) {
              processTriggeredSegment(segment, coordinator, repairNo);
            } else {
              LOG.info("Nothing to repair for segment {} in keyspace {}", segmentId, keyspace);

              context.storage.getRepairSegmentDao().updateRepairSegment(
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

  private void processTriggeredSegment(final RepairSegment segment, final ICassandraManagementProxy coordinator,
                                       int repairNo) {

    repairRunner.updateLastEvent(
        String.format("Triggered repair of segment %s via host %s", segment.getId(), coordinator.getHost()));

    // Timeout is extended for each attempt to prevent repairs from blocking if settings aren't accurate
    int attempt = segment.getFailCount() + 1;
    long segmentTimeout = repairUnit.getIncrementalRepair() && !repairUnit.getSubrangeIncrementalRepair()
        ? timeoutMillis * MAX_TIMEOUT_EXTENSIONS * attempt
        : timeoutMillis * attempt;
    LOG.info("Repair for segment {} started, status wait will timeout in {} millis", segmentId, segmentTimeout);

    try {
      final long startTime = System.currentTimeMillis();
      final long maxTime = startTime + segmentTimeout;
      final long waitTime = Math.min(segmentTimeout, 60000);

      while (System.currentTimeMillis() < maxTime) {
        boolean isDoneOrFailed = condition.await(waitTime, TimeUnit.MILLISECONDS);

        isDoneOrFailed |= RepairSegment.State.DONE == context.storage
            .getRepairSegmentDao().getRepairSegment(segment.getRunId(), segmentId).get().getState();

        if (isDoneOrFailed) {
          break;
        }
        renewLead(segment);
      }
    } catch (InterruptedException e) {
      LOG.warn("Repair command {} on segment {} interrupted", this.repairNo, segmentId, e);
    } finally {
      coordinator.removeRepairStatusHandler(repairNo);
      RepairSegment resultingSegment
          = context.storage.getRepairSegmentDao().getRepairSegment(repairRunner.getRepairRunId(), segmentId).get();

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
      renewLead(segment);
    }
  }

  private String metricNameForRepairing(RepairSegment rs) {
    return MetricRegistry.name(
        SegmentRunner.class,
        "repairing",
        cleanHostName(rs.getCoordinatorHost()),
        cleanName(clusterName),
        cleanName(repairUnit.getKeyspaceName()));
  }

  private String metricNameForRunRepair(RepairSegment rs) {
    return MetricRegistry.name(
        SegmentRunner.class,
        "runRepair",
        cleanHostName(rs.getCoordinatorHost()),
        cleanName(clusterName),
        cleanName(repairUnit.getKeyspaceName()));
  }

  /**
   * Called when there is an event coming either from JMX or this runner regarding on-going repairs.
   *
   * @param repairNo repair sequence number, obtained when triggering a repair
   * @param status   new status of the repair
   * @param message  additional information about the repair
   */
  @Override
  public void handle(
      int repairNo,
      Optional<ActiveRepairService.Status> status,
      Optional<ProgressEventType> progress,
      String message,
      ICassandraManagementProxy cassandraManagementProxy) {

    final RepairSegment segment = context.storage.getRepairSegmentDao().getRepairSegment(repairRunner.getRepairRunId(),
        segmentId).get();
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
      RepairSegment currentSegment = context.storage.getRepairSegmentDao().getRepairSegment(
          repairRunner.getRepairRunId(), segmentId).get();

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
            cassandraManagementProxy);
      }

      // New repair API – Cassandra-2.2 onwards
      if (progress.isPresent()) {
        failOutsideSynchronizedBlock = handleJmxNotificationForCassandra22(
            progress,
            currentSegment,
            repairNo,
            failOutsideSynchronizedBlock,
            cassandraManagementProxy);
      }
    }

    if (failOutsideSynchronizedBlock) {
      if (takeLead(segment) || renewLead(segment)) {
        try {
          postponeCurrentSegment();
          tryClearSnapshots(message);
        } finally {
          // if someone else does hold the lease, ie renewLead(..) was true,
          // then their writes to repair_run table and any call to releaseLead(..) will throw an exception
          try {
            releaseLead(segment);
          } catch (AssertionError ignore) {
          }
        }
      }
    }
  }

  private boolean handleJmxNotificationForCassandra22(
      Optional<ProgressEventType> progress,
      RepairSegment currentSegment,
      int repairNumber,
      boolean failOutsideSynchronizedBlock,
      ICassandraManagementProxy cassandraManagementProxy) {

    switch (progress.get()) {
      case START:
        try {
          // avoid changing state to RUNNING if later notifications have already arrived
          if (!successOrFailedNotified.get()
              && RepairSegment.State.STARTED == currentSegment.getState()
              && renewLead(currentSegment)) {

            context.storage.getRepairSegmentDao().updateRepairSegment(
                currentSegment
                    .with()
                    .withState(RepairSegment.State.RUNNING)
                    .withId(segmentId)
                    .build());

            LOG.debug("updated segment {} with state {}", segmentId, RepairSegment.State.RUNNING);
            break;
          }
        } catch (AssertionError er) {
          LOG.debug("Failed processing START notification for segment {}", segmentId, er);
        }
        segmentFailed.set(true);
        break;

      case SUCCESS:
        Preconditions.checkState(
            !successOrFailedNotified.get(),
            "illegal multiple 'SUCCESS' and 'FAILURE', %s:%s", repairRunner.getRepairRunId(), segmentId);
        successOrFailedNotified.set(true);

        try {
          if (segmentFailed.get()) {
            LOG.debug(
                "Got SUCCESS for segment with id '{}' and repair number '{}', but it had already timed out",
                segmentId,
                repairNumber);
          } else if (renewLead(currentSegment)) {
            LOG.debug(
                "repair session succeeded for segment with id '{}' and repair number '{}'",
                segmentId,
                repairNumber);

            context.storage.getRepairSegmentDao().updateRepairSegment(
                currentSegment
                    .with()
                    .withState(RepairSegment.State.DONE)
                    .withEndTime(DateTime.now())
                    .withId(segmentId)
                    .build());

            // Since we can get out of order notifications,
            // we need to exit if we already got the COMPLETE notification.
            if (completeNotified.get()) {
              LOG.debug("Complete was already notified for segment {}. Signaling the condition object...", segmentId);
              condition.signalAll();
              cassandraManagementProxy.removeRepairStatusHandler(repairNumber);
            }
            break;
          }
        } catch (AssertionError er) {
          LOG.debug("Failed processing SUCCESS notification for segment {}", segmentId, er);
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
          cassandraManagementProxy.removeRepairStatusHandler(repairNumber);
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

        completeNotified.set(true);
        LOG.debug(
            "repair session finished for segment with id '{}' and repair number '{}'",
            segmentId,
            repairNumber);

        if (successOrFailedNotified.get()) {
          LOG.debug("Success was already notified for segment {}. Signaling the condition object...", segmentId);
          condition.signalAll();
          cassandraManagementProxy.removeRepairStatusHandler(repairNumber);
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
      ICassandraManagementProxy cassandraManagementProxy) {

    switch (status.get()) {
      case STARTED:
        try {
          // avoid changing state to RUNNING if later notifications have already arrived
          if (!successOrFailedNotified.get()
              && RepairSegment.State.STARTED == currentSegment.getState()
              && renewLead(currentSegment)) {

            context.storage.getRepairSegmentDao().updateRepairSegment(
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
          successOrFailedNotified.set(true);

          try {
            if (segmentFailed.get()) {
              LOG.debug(
                  "Got SESSION_SUCCESS for segment with id '{}' and repair number '{}', but it had already timed out",
                  segmentId,
                  repairNumber);
            } else if (renewLead(currentSegment)) {
              LOG.debug(
                  "repair session succeeded for segment with id '{}' and repair number '{}'",
                  segmentId,
                  repairNumber);

              context.storage.getRepairSegmentDao().updateRepairSegment(
                  currentSegment
                      .with()
                      .withState(RepairSegment.State.DONE)
                      .withEndTime(DateTime.now())
                      .withId(segmentId)
                      .build());

              // Since we can get out of order notifications,
              // we need to exit if we already got the COMPLETE notification.
              if (completeNotified.get()) {
                condition.signalAll();
                cassandraManagementProxy.removeRepairStatusHandler(repairNumber);
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
            cassandraManagementProxy.removeRepairStatusHandler(repairNumber);
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
        completeNotified.set(true);
        LOG.debug(
            "repair session finished for segment with id '{}' and repair number '{}'",
            segmentId,
            repairNumber);
        if (successOrFailedNotified.get()) {
          condition.signalAll();
          cassandraManagementProxy.removeRepairStatusHandler(repairNumber);
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
          ICassandraManagementProxy jmx = clusterFacade.connect(
              context.storage.getClusterDao().getCluster(clusterName),
              Arrays.asList(involvedNode));
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

  /**
   * Calculate the delay that should be used before starting the next repair segment.
   *
   * @return the delay in milliseconds.
   */
  long intensityBasedDelayMillis(double intensity) {
    RepairSegment repairSegment = context.storage.getRepairSegmentDao().getRepairSegment(repairRunner.getRepairRunId(),
        segmentId).get();
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

  private boolean takeLead(RepairSegment segment) {
    try (Timer.Context cx
             = context.metricRegistry.timer(MetricRegistry.name(SegmentRunner.class, "takeLead")).time()) {

      boolean result = false;
      if (repairUnit.getIncrementalRepair() && !repairUnit.getSubrangeIncrementalRepair()) {
        result = context.storage instanceof IDistributedStorage
            ? ((IDistributedStorage) context.storage).takeLead(leaderElectionId)
            : true;
      } else {
        result = context.storage.lockRunningRepairsForNodes(this.repairRunner.getRepairRunId(),
            segment.getId(), segment.getReplicas().keySet());
      }
      if (!result) {
        context.metricRegistry.counter(MetricRegistry.name(SegmentRunner.class, "takeLead", "failed")).inc();
      }
      return result;
    }
  }

  private boolean renewLead(RepairSegment segment) {
    try (Timer.Context cx
             = context.metricRegistry.timer(MetricRegistry.name(SegmentRunner.class, "renewLead")).time()) {

      if (repairUnit.getIncrementalRepair() && !repairUnit.getSubrangeIncrementalRepair()) {
        boolean result = context.storage instanceof IDistributedStorage
            ? ((IDistributedStorage) context.storage).renewLead(leaderElectionId)
            : true;

        if (!result) {
          context.metricRegistry.counter(MetricRegistry.name(SegmentRunner.class, "renewLead", "failed")).inc();
        }
        return result;
      } else {
        boolean resultLock2 = context.storage.renewRunningRepairsForNodes(this.repairRunner.getRepairRunId(),
            segment.getId(), segment.getReplicas().keySet());
        if (!resultLock2) {
          context.metricRegistry.counter(MetricRegistry.name(SegmentRunner.class, "renewLead", "failed")).inc();
          releaseLead(segment);
        }

        return resultLock2;
      }
    }
  }

  private void releaseLead(RepairSegment segment) {
    try (Timer.Context cx
             = context.metricRegistry.timer(MetricRegistry.name(SegmentRunner.class, "releaseLead")).time()) {

      if (repairUnit.getIncrementalRepair() && !repairUnit.getSubrangeIncrementalRepair()) {
        if (context.storage instanceof IDistributedStorage) {
          ((IDistributedStorage) context.storage).releaseLead(leaderElectionId);
        }
      } else {
        context.storage.releaseRunningRepairsForNodes(this.repairRunner.getRepairRunId(),
            segment.getId(), segment.getReplicas().keySet());
      }
    }
  }

  private int countRunningReapers() {
    return context.isDistributed.get() ? ((IDistributedStorage) context.storage).countRunningReapers() : 1;
  }
}