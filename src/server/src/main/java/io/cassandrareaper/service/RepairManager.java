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
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.jmx.JmxProxy;
import io.cassandrareaper.storage.IDistributedStorage;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RepairManager {

  private static final Logger LOG = LoggerFactory.getLogger(RepairManager.class);

  // Caching all active RepairRunners.
  final Map<UUID, RepairRunner> repairRunners = Maps.newConcurrentMap();

  private final AppContext context;
  private final Heart heart;
  private ListeningScheduledExecutorService executor;
  private long repairTimeoutMillis;
  private long retryDelayMillis;

  private RepairManager(AppContext context)  {
    this.context = context;
    this.heart = Heart.create(context);
  }

  public static RepairManager create(AppContext context)  {
    return new RepairManager(context);
  }

  long getRepairTimeoutMillis() {
    return repairTimeoutMillis;
  }

  @VisibleForTesting
  public void initializeThreadPool(
      int threadAmount,
      long repairTimeout,
      TimeUnit repairTimeoutTimeUnit,
      long retryDelay,
      TimeUnit retryDelayTimeUnit) {

    executor = MoreExecutors.listeningDecorator(
        Executors.newScheduledThreadPool(threadAmount, new NamedThreadFactory("RepairRunner")));

    repairTimeoutMillis = repairTimeoutTimeUnit.toMillis(repairTimeout);
    retryDelayMillis = retryDelayTimeUnit.toMillis(retryDelay);
  }

  /**
   * Consult storage to see if any repairs are running, and resume those repair runs.
   */
  public void resumeRunningRepairRuns() throws ReaperException {
    try {
      heart.beat();
      Collection<RepairRun> running =
          context.storage.getRepairRunsWithState(RepairRun.RunState.RUNNING);
      for (RepairRun repairRun : running) {

        Collection<RepairSegment> runningSegments =
            context.storage.getSegmentsWithState(repairRun.getId(), RepairSegment.State.RUNNING);

        abortSegmentsWithNoLeader(repairRun, runningSegments);

        if (!repairRunners.containsKey(repairRun.getId())) {
          LOG.info("Restarting run id {} that has no runner", repairRun.getId());
          startRepairRun(repairRun);
        }
      }

      Collection<RepairRun> paused =
          context.storage.getRepairRunsWithState(RepairRun.RunState.PAUSED);
      for (RepairRun pausedRepairRun : paused) {
        if (repairRunners.containsKey(pausedRepairRun.getId())) {
          // Abort all running segments for paused repair runs
          Collection<RepairSegment> runningSegments =
              context.storage.getSegmentsWithState(
                  pausedRepairRun.getId(), RepairSegment.State.RUNNING);

          abortSegments(runningSegments, pausedRepairRun, false, false);
        }

        if (!repairRunners.containsKey(pausedRepairRun.getId())) {
          startRunner(pausedRepairRun.getId());
        }
      }
    } catch (RuntimeException e) {
      throw new ReaperException(e);
    }
  }

  private void abortSegmentsWithNoLeader(RepairRun repairRun, Collection<RepairSegment> runningSegments) {

    LOG.debug(
        "Checking leadership on the following segments : {}",
        runningSegments.stream().map(seg -> seg.getId()).collect(Collectors.toList()));
    if (context.storage instanceof IDistributedStorage || !repairRunners.containsKey(repairRun.getId())) {
      // When multiple Reapers are in use, we can get stuck segments when one instance is rebooted
      // Any segment in RUNNING state but with no leader should be killed
      List<UUID> activeLeaders =
          context.storage instanceof IDistributedStorage
              ? ((IDistributedStorage) context.storage).getLeaders()
              : Collections.emptyList();

      LOG.debug(
          "No leader on the following segments : {}",
          runningSegments
              .stream()
              .filter(segment -> !activeLeaders.contains(segment.getId()))
              .map(seg -> seg.getId())
              .collect(Collectors.toSet()));

      abortSegments(
          runningSegments
              .stream()
              .filter(segment -> !activeLeaders.contains(segment.getId()))
              .collect(Collectors.toSet()),
          repairRun,
          false,
          true);
    }
  }

  public RepairSegment abortSegment(UUID repairRunId, UUID segmentId) {
    RepairSegment segment = context.storage.getRepairSegment(repairRunId, segmentId).get();
    RepairRun repairRun = context.storage.getRepairRun(repairRunId).get();
    if (context.storage instanceof IDistributedStorage) {
      ((IDistributedStorage) context.storage).forceReleaseLead(segmentId);
      ((IDistributedStorage) context.storage).takeLead(segmentId);
    }
    if (null == segment.getCoordinatorHost() || RepairSegment.State.DONE == segment.getState()) {
      SegmentRunner.postponeSegment(context, segment);
    } else {
      abortSegments(Arrays.asList(segment), repairRun, true, false);
    }

    return context.storage.getRepairSegment(repairRunId, segmentId).get();
  }

  void abortSegments(Collection<RepairSegment> runningSegments, RepairRun repairRun) {
    abortSegments(runningSegments, repairRun, false, false);
  }

  public void abortSegments(
      Collection<RepairSegment> runningSegments,
      RepairRun repairRun,
      boolean forced,
      boolean postponeWithoutAborting) {
    RepairUnit repairUnit = context.storage.getRepairUnit(repairRun.getRepairUnitId()).get();
    for (RepairSegment segment : runningSegments) {
      LOG.debug(
          "Trying to abort stuck segment {} in repair run {}", segment.getId(), repairRun.getId());
      UUID leaderElectionId = repairUnit.getIncrementalRepair() ? repairRun.getId() : segment.getId();
      if (forced || takeLead(context, leaderElectionId) || renewLead(context, leaderElectionId)) {
        // refresh segment once we're inside leader-election
        segment = context.storage.getRepairSegment(repairRun.getId(), segment.getId()).get();
        if (RepairSegment.State.RUNNING == segment.getState()) {
          try (JmxProxy jmxProxy = context.jmxConnectionFactory.connect(
              segment.getCoordinatorHost(), context.config.getJmxConnectionTimeoutInSeconds())) {
            LOG.warn(
                "Aborting stuck segment {} in repair run {}", segment.getId(), repairRun.getId());
            if (postponeWithoutAborting) {
              SegmentRunner.abort(context, segment, jmxProxy);
            }
          } catch (ReaperException | NumberFormatException | InterruptedException e) {
            LOG.debug(
                "Tried to abort repair on segment {} marked as RUNNING, "
                    + "but the host was down  (so abortion won't be needed). "
                    + "Postponing the segment.",
                segment.getId(),
                e);
            SegmentRunner.postponeSegment(context, segment);
          } finally {
            // if someone else does hold the lease, ie renewLead(..) was true,
            // then their writes to repair_run table and any call to releaseLead(..) will throw an exception
            releaseLead(context, leaderElectionId);
          }
        }
      }
    }
  }

  public RepairRun startRepairRun(RepairRun runToBeStarted) throws ReaperException {
    assert null != executor : "you need to initialize the thread pool first";
    UUID runId = runToBeStarted.getId();
    LOG.info("Starting a run with id #{} with current state '{}'", runId, runToBeStarted.getRunState());
    switch (runToBeStarted.getRunState()) {
      case NOT_STARTED: {
        RepairRun updatedRun = runToBeStarted
            .with()
            .runState(RepairRun.RunState.RUNNING)
            .startTime(DateTime.now())
            .build(runToBeStarted.getId());
        if (!context.storage.updateRepairRun(updatedRun)) {
          throw new ReaperException("failed updating repair run " + updatedRun.getId());
        }
        startRunner(runId);
        return updatedRun;
      }
      case PAUSED: {
        RepairRun updatedRun = runToBeStarted.with()
            .runState(RepairRun.RunState.RUNNING)
            .pauseTime(null)
            .build(runToBeStarted.getId());

        if (!context.storage.updateRepairRun(updatedRun)) {
          throw new ReaperException("failed updating repair run " + updatedRun.getId());
        }
        return updatedRun;
      }
      case RUNNING:
        Preconditions.checkState(
            !repairRunners.containsKey(runId), "trying to re-trigger run that is already running, with id " + runId);
        LOG.info("re-trigger a running run after restart, with id {}", runId);
        startRunner(runId);
        return runToBeStarted;
      case ERROR: {
        RepairRun updatedRun
            = runToBeStarted.with().runState(RepairRun.RunState.RUNNING).endTime(null).build(runToBeStarted.getId());
        if (!context.storage.updateRepairRun(updatedRun)) {
          throw new ReaperException("failed updating repair run " + updatedRun.getId());
        }
        startRunner(runId);
        return updatedRun;
      }
      default:
        throw new ReaperException("cannot start run with state: " + runToBeStarted.getRunState());
    }
  }

  public RepairRun updateRepairRunIntensity(RepairRun repairRun, Double intensity) throws ReaperException {
    RepairRun updatedRun = repairRun.with().intensity(intensity).build(repairRun.getId());
    if (!context.storage.updateRepairRun(updatedRun)) {
      throw new ReaperException("failed updating repair run " + updatedRun.getId());
    }
    return updatedRun;
  }

  private void startRunner(UUID runId) {
    if (!repairRunners.containsKey(runId)) {
      LOG.info("scheduling repair for repair run #{}", runId);
      try {
        RepairRunner newRunner = new RepairRunner(context, runId);
        repairRunners.put(runId, newRunner);
        executor.submit(newRunner);
      } catch (ReaperException e) {
        LOG.warn("Failed to schedule repair for repair run #{}", runId, e);
      }
    } else {
      LOG.error(
          "there is already a repair runner for run with id {}, so not starting new runner. This "
          + "should not happen.",
          runId);
    }
  }

  public RepairRun pauseRepairRun(RepairRun runToBePaused) throws ReaperException {
    RepairRun updatedRun = runToBePaused.with()
        .runState(RepairRun.RunState.PAUSED)
        .pauseTime(DateTime.now())
        .build(runToBePaused.getId());

    if (!context.storage.updateRepairRun(updatedRun)) {
      throw new ReaperException("failed updating repair run " + updatedRun.getId());
    }
    return updatedRun;
  }

  public RepairRun abortRepairRun(RepairRun runToBeAborted) throws ReaperException {
    RepairRun updatedRun = runToBeAborted
        .with()
        .runState(RepairRun.RunState.ABORTED)
        .pauseTime(DateTime.now())
        .build(runToBeAborted.getId());

    if (!context.storage.updateRepairRun(updatedRun)) {
      throw new ReaperException("failed updating repair run " + updatedRun.getId());
    }
    return updatedRun;
  }

  void scheduleRetry(RepairRunner runner) {
    executor.schedule(runner, retryDelayMillis, TimeUnit.MILLISECONDS);
  }

  ListenableFuture<?> submitSegment(SegmentRunner runner) {
    return executor.submit(runner);
  }

  void removeRunner(RepairRunner runner) {
    repairRunners.remove(runner.getRepairRunId());
  }

  private static boolean takeLead(AppContext context, UUID leaderElectionId) {
    try (Timer.Context cx
        = context.metricRegistry.timer(MetricRegistry.name(RepairManager.class, "takeLead")).time()) {

      boolean result = context.storage instanceof IDistributedStorage
          ? ((IDistributedStorage) context.storage).takeLead(leaderElectionId)
          : true;

      if (!result) {
        context.metricRegistry.counter(MetricRegistry.name(RepairManager.class, "takeLead", "failed")).inc();
      }
      return result;
    }
  }

  private static boolean renewLead(AppContext context, UUID leaderElectionId) {
    try (Timer.Context cx
        = context.metricRegistry.timer(MetricRegistry.name(RepairManager.class, "renewLead")).time()) {

      boolean result = context.storage instanceof IDistributedStorage
          ? ((IDistributedStorage) context.storage).renewLead(leaderElectionId)
          : true;

      if (!result) {
        context.metricRegistry.counter(MetricRegistry.name(RepairManager.class, "renewLead", "failed")).inc();
      }
      return result;
    }
  }

  private static void releaseLead(AppContext context, UUID leaderElectionId) {
    try (Timer.Context cx
        = context.metricRegistry.timer(MetricRegistry.name(RepairManager.class, "releaseLead")).time()) {
      if (context.storage instanceof IDistributedStorage) {
        ((IDistributedStorage) context.storage).releaseLead(leaderElectionId);
      }
    }
  }
}
