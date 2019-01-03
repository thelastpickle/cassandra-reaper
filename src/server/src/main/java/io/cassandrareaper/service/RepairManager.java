/*
 * Copyright 2015-2017 Spotify AB
 * Copyright 2016-2018 The Last Pickle Ltd
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
import io.cassandrareaper.core.Node;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import com.codahale.metrics.InstrumentedScheduledExecutorService;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RepairManager implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(RepairManager.class);

  // State of all active RepairRunners
  final Map<UUID, RepairRunner> repairRunners = Maps.newConcurrentMap();
  private final Lock repairRunnersLock = new ReentrantLock();

  private final AppContext context;
  private final Heart heart;
  private final ListeningScheduledExecutorService executor;
  private final long repairTimeoutMillis;
  private final long retryDelayMillis;

  private RepairManager(
      AppContext context,
      ScheduledExecutorService executor,
      long repairTimeout,
      TimeUnit repairTimeoutTimeUnit,
      long retryDelay,
      TimeUnit retryDelayTimeUnit)  {

    this.context = context;
    this.heart = Heart.create(context);
    this.repairTimeoutMillis = repairTimeoutTimeUnit.toMillis(repairTimeout);
    this.retryDelayMillis = retryDelayTimeUnit.toMillis(retryDelay);

    this.executor = MoreExecutors.listeningDecorator(
        new InstrumentedScheduledExecutorService(executor, context.metricRegistry));
  }

  public static RepairManager create(
      AppContext context,
      ScheduledExecutorService executor,
      long repairTimeout,
      TimeUnit repairTimeoutTimeUnit,
      long retryDelay,
      TimeUnit retryDelayTimeUnit) {

    return new RepairManager(context, executor, repairTimeout, repairTimeoutTimeUnit, retryDelay, retryDelayTimeUnit);
  }

  long getRepairTimeoutMillis() {
    return repairTimeoutMillis;
  }

  /**
   * Consult storage to see if any repairs are running, and resume those repair runs.
   */
  public void resumeRunningRepairRuns() throws ReaperException {
    try {
      heart.beat();
      Collection<RepairRun> runningRepairRuns = context.storage.getRepairRunsWithState(RepairRun.RunState.RUNNING);
      Collection<RepairRun> pausedRepairRuns = context.storage.getRepairRunsWithState(RepairRun.RunState.PAUSED);
      abortAllRunningSegmentsWithNoLeader(runningRepairRuns);
      abortAllRunningSegmentsInKnownPausedRepairRuns(pausedRepairRuns);
      resumeUnkownRunningRepairRuns(runningRepairRuns);
      resumeUnknownPausedRepairRuns(pausedRepairRuns);
    } catch (RuntimeException e) {
      throw new ReaperException(e);
    }
  }

  private void abortAllRunningSegmentsWithNoLeader(Collection<RepairRun> runningRepairRuns) throws ReaperException {
    runningRepairRuns
        .forEach((repairRun) -> {
          Collection<RepairSegment> runningSegments
              = context.storage.getSegmentsWithState(repairRun.getId(), RepairSegment.State.RUNNING);

          abortSegmentsWithNoLeader(repairRun, runningSegments);
        });
  }

  private void resumeUnkownRunningRepairRuns(Collection<RepairRun> runningRepairRuns) throws ReaperException {
    try {
      repairRunnersLock.lock();
      for (RepairRun repairRun : runningRepairRuns) {
        if (!repairRunners.containsKey(repairRun.getId())) {
          LOG.info("Restarting run id {} that has no runner", repairRun.getId());
          // it may be that this repair is already "running" actively on other reaper instances
          //  nonetheless we need to make it actively running on this reaper instance as well
          //   so to help in running the queued segments
          startRepairRun(repairRun);
        }
      }
    } finally {
      repairRunnersLock.unlock();
    }
  }

  private void abortAllRunningSegmentsInKnownPausedRepairRuns(Collection<RepairRun> pausedRepairRuns) {
    try {
      repairRunnersLock.lock();

      pausedRepairRuns
          .stream()
          .filter((pausedRepairRun) -> repairRunners.containsKey(pausedRepairRun.getId()))
          .forEach((pausedRepairRun) -> {
            // Abort all running segments for paused repair runs
            Collection<RepairSegment> runningSegments
                = context.storage.getSegmentsWithState(pausedRepairRun.getId(), RepairSegment.State.RUNNING);

            abortSegments(runningSegments, pausedRepairRun, false, false);
          });
    } finally {
      repairRunnersLock.unlock();
    }
  }

  private void resumeUnknownPausedRepairRuns(Collection<RepairRun> pausedRepairRuns) {
    try {
      repairRunnersLock.lock();

      pausedRepairRuns
          .stream()
          .filter((pausedRepairRun) -> (!repairRunners.containsKey(pausedRepairRun.getId())))
          // add "paused" repair run to this reaper instance, so it can be visualised in UI
          .forEachOrdered((pausedRepairRun) -> startRunner(pausedRepairRun.getId()));
    } finally {
      repairRunnersLock.unlock();
    }
  }

  private void abortSegmentsWithNoLeader(RepairRun repairRun, Collection<RepairSegment> runningSegments) {

    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Checking leadership on the following segments : {}",
          runningSegments.stream().map(seg -> seg.getId()).collect(Collectors.toList()));
    }
    try {
      repairRunnersLock.lock();
      if (context.storage instanceof IDistributedStorage || !repairRunners.containsKey(repairRun.getId())) {
        // When multiple Reapers are in use, we can get stuck segments when one instance is rebooted
        // Any segment in RUNNING state but with no leader should be killed
        List<UUID> leaders = context.storage instanceof IDistributedStorage
                ? ((IDistributedStorage) context.storage).getLeaders()
                : Collections.emptyList();

        Collection<RepairSegment> orphanedSegments = runningSegments
            .stream()
            .filter(segment -> !leaders.contains(segment.getId()) && !leaders.contains(segment.getRunId()))
            .collect(Collectors.toSet());

        LOG.debug("No leader on the following segments : {}", orphanedSegments);
        abortSegments(orphanedSegments, repairRun, false, true);
      }
    } finally {
      repairRunnersLock.unlock();
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

    RepairUnit repairUnit = context.storage.getRepairUnit(repairRun.getRepairUnitId());

    for (RepairSegment segment : runningSegments) {
      LOG.debug("Trying to abort stuck segment {} in repair run {}", segment.getId(), repairRun.getId());
      UUID leaderElectionId = repairUnit.getIncrementalRepair() ? repairRun.getId() : segment.getId();
      if (forced || takeLead(context, leaderElectionId) || renewLead(context, leaderElectionId)) {
        // refresh segment once we're inside leader-election
        segment = context.storage.getRepairSegment(repairRun.getId(), segment.getId()).get();
        if (RepairSegment.State.RUNNING == segment.getState()) {
          try {
            Node node
                = Node.builder()
                    .withCluster(context.storage.getCluster(repairRun.getClusterName()).get())
                    .withHostname(segment.getCoordinatorHost())
                    .build();

            JmxProxy jmxProxy
                = context.jmxConnectionFactory.connect(node, context.config.getJmxConnectionTimeoutInSeconds());

            SegmentRunner.abort(context, segment, jmxProxy);
          } catch (ReaperException | NumberFormatException | InterruptedException e) {
            String msg = "Tried to abort repair on segment {} marked as RUNNING, but the "
                + "host was down (so abortion won't be needed). Postponing the segment.";

            LOG.debug(msg, segment.getId(), e);
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
    try {
      repairRunnersLock.lock();

      Preconditions.checkState(
          !repairRunners.containsKey(runId),
          "there is already a repair runner for run with id " + runId + ". This should not happen.");

      LOG.info("scheduling repair for repair run #{}", runId);
      try {
        RepairRunner newRunner = new RepairRunner(context, runId);
        repairRunners.put(runId, newRunner);
        executor.submit(newRunner);
      } catch (ReaperException e) {
        LOG.warn("Failed to schedule repair for repair run #" + runId, e);
      }
    } finally {
      repairRunnersLock.unlock();
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
    try {
      repairRunnersLock.lock();
      repairRunners.remove(runner.getRepairRunId());
    } finally {
      repairRunnersLock.unlock();
    }
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

  @Override
  public void close() {
    heart.close();
    executor.shutdownNow();
  }
}
