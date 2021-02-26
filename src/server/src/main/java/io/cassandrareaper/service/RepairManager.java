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
import io.cassandrareaper.ReaperException;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import com.codahale.metrics.InstrumentedScheduledExecutorService;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
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
  private final ClusterFacade clusterFacade;
  private final ListeningScheduledExecutorService executor;
  private final long repairTimeoutMillis;
  private final long retryDelayMillis;
  private final int maxParallelRepairs;

  private RepairManager(
      AppContext context,
      ClusterFacade clusterFacade,
      ScheduledExecutorService executor,
      long repairTimeout,
      TimeUnit repairTimeoutTimeUnit,
      long retryDelay,
      TimeUnit retryDelayTimeUnit,
      int maxParallelRepairs
  ) throws ReaperException {

    this.context = context;
    this.clusterFacade = clusterFacade;
    this.repairTimeoutMillis = repairTimeoutTimeUnit.toMillis(repairTimeout);
    this.retryDelayMillis = retryDelayTimeUnit.toMillis(retryDelay);

    this.executor = MoreExecutors.listeningDecorator(
        new InstrumentedScheduledExecutorService(executor, context.metricRegistry));
    this.maxParallelRepairs = maxParallelRepairs;
  }

  @VisibleForTesting
  static RepairManager create(
      AppContext context,
      ClusterFacade clusterFacadeSupplier,
      ScheduledExecutorService executor,
      long repairTimeout,
      TimeUnit repairTimeoutTimeUnit,
      long retryDelay,
      TimeUnit retryDelayTimeUnit,
      int maxParallelRepairs
  ) throws ReaperException {

    return new RepairManager(
        context,
        clusterFacadeSupplier,
        executor,
        repairTimeout,
        repairTimeoutTimeUnit,
        retryDelay,
        retryDelayTimeUnit,
        maxParallelRepairs);
  }

  public static RepairManager create(
      AppContext context,
      ScheduledExecutorService executor,
      long repairTimeout,
      TimeUnit repairTimeoutTimeUnit,
      long retryDelay,
      TimeUnit retryDelayTimeUnit,
      int maxParallelRepairs
  ) throws ReaperException {

    return create(
        context,
        ClusterFacade.create(context),
        executor,
        repairTimeout,
        repairTimeoutTimeUnit,
        retryDelay,
        retryDelayTimeUnit,
        maxParallelRepairs);
  }

  long getRepairTimeoutMillis() {
    return repairTimeoutMillis;
  }

  /**
   * Consult storage to see if any repairs are running, and resume those repair runs.
   */
  public void resumeRunningRepairRuns() throws ReaperException {
    try {
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

  private void abortAllRunningSegmentsWithNoLeader(Collection<RepairRun> runningRepairRuns) {
    runningRepairRuns
        .forEach((repairRun) -> {
          Collection<RepairSegment> runningSegments = context.storage.getSegmentsWithState(repairRun.getId(),
              RepairSegment.State.RUNNING);
          Collection<RepairSegment> startedSegments = context.storage.getSegmentsWithState(repairRun.getId(),
              RepairSegment.State.STARTED);

          abortSegmentsWithNoLeader(repairRun, runningSegments);
          abortSegmentsWithNoLeader(repairRun, startedSegments);
        });
  }

  private void resumeUnkownRunningRepairRuns(Collection<RepairRun> runningRepairRuns) throws ReaperException {
    try {
      repairRunnersLock.lock();
      for (RepairRun repairRun : runningRepairRuns) {
        if (!repairRunners.containsKey(repairRun.getId())) {
          LOG.info("Restarting run id {} that has no runner", repairRun.getId());
          // it may be that this repair is already "running" actively on other reaper instances
          // nonetheless we need to make it actively running on this reaper instance as well
          // so to help in running the queued segments
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
            // Abort all running and started segments for paused repair runs
            Collection<RepairSegment> runningSegments = context.storage.getSegmentsWithState(pausedRepairRun.getId(),
                RepairSegment.State.RUNNING);
            Collection<RepairSegment> startedSegments = context.storage.getSegmentsWithState(pausedRepairRun.getId(),
                RepairSegment.State.STARTED);

            abortSegments(runningSegments, pausedRepairRun);
            abortSegments(startedSegments, pausedRepairRun);
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
          .forEachOrdered((pausedRepairRun) -> startRunner(pausedRepairRun));
    } finally {
      repairRunnersLock.unlock();
    }
  }

  private void abortSegmentsWithNoLeader(RepairRun repairRun, Collection<RepairSegment> runningSegments) {
    RepairUnit repairUnit = context.storage.getRepairUnit(repairRun.getRepairUnitId());
    if (repairUnit.getIncrementalRepair()) {
      abortSegmentsWithNoLeaderIncremental(repairRun, runningSegments);
    } else {
      abortSegmentsWithNoLeaderNonIncremental(repairRun, runningSegments);
    }
  }

  private void abortSegmentsWithNoLeaderIncremental(RepairRun repairRun, Collection<RepairSegment> runningSegments) {

    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Checking leadership on the following segments : {}",
          runningSegments.stream().map(seg -> seg.getId()).collect(Collectors.toList()));
    }
    try {
      repairRunnersLock.lock();
      if (context.storage instanceof IDistributedStorage || !repairRunners.containsKey(repairRun.getId())) {
        // When multiple Reapers are in use, we can get stuck segments when one instance is rebooted
        // Any segment in RUNNING or STARTED state but with no leader should be killed
        List<UUID> leaders = context.storage instanceof IDistributedStorage
            ? ((IDistributedStorage) context.storage).getLeaders()
            : Collections.emptyList();

        Collection<RepairSegment> orphanedSegments = runningSegments
            .stream()
            .filter(segment -> !leaders.contains(segment.getId()) && !leaders.contains(segment.getRunId()))
            .collect(Collectors.toSet());

        LOG.debug("No leader on the following segments : {}", orphanedSegments);
        abortSegments(orphanedSegments, repairRun);
      }
    } finally {
      repairRunnersLock.unlock();
    }
  }

  private void abortSegmentsWithNoLeaderNonIncremental(RepairRun repairRun, Collection<RepairSegment> runningSegments) {

    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Checking leadership on the following segments : {}",
          runningSegments.stream().map(seg -> seg.getId()).collect(Collectors.toList()));
    }
    try {
      repairRunnersLock.lock();
      if (context.storage instanceof IDistributedStorage || !repairRunners.containsKey(repairRun.getId())) {
        // When multiple Reapers are in use, we can get stuck segments when one instance is rebooted
        // Any segment in RUNNING or STARTED state but with no leader should be killed
        Set<UUID> leaders = context.storage instanceof IDistributedStorage
            ? ((IDistributedStorage) context.storage).getLockedSegmentsForRun(repairRun.getId())
            : Collections.emptySet();

        Collection<RepairSegment> orphanedSegments = runningSegments
            .stream()
            .filter(segment -> !leaders.contains(segment.getId()))
            .collect(Collectors.toSet());

        LOG.debug("No leader on the following segments : {}", orphanedSegments);
        abortSegments(orphanedSegments, repairRun);
      }
    } finally {
      repairRunnersLock.unlock();
    }
  }

  public RepairSegment abortSegment(UUID runId, UUID segmentId) throws ReaperException {
    RepairSegment segment = context.storage.getRepairSegment(runId, segmentId).get();
    try {
      if (null == segment.getCoordinatorHost() || RepairSegment.State.DONE == segment.getState()) {
        RepairUnit repairUnit = context.storage.getRepairUnit(segment.getRepairUnitId());
        UUID leaderElectionId = repairUnit.getIncrementalRepair() ? runId : segmentId;
        boolean tookLead;
        if (tookLead = takeLead(context, leaderElectionId) || renewLead(context, leaderElectionId)) {
          try {
            SegmentRunner.postponeSegment(context, segment);
          } finally {
            if (tookLead) {
              releaseLead(context, leaderElectionId);
            }
          }
        }
      } else {
        abortSegments(Arrays.asList(segment), context.storage.getRepairRun(runId).get());
      }
      return context.storage.getRepairSegment(runId, segmentId).get();
    } catch (AssertionError error) {
      throw new ReaperException("lead is already taken on " + runId + ":" + segmentId, new Exception(error));
    }
  }

  void abortSegments(Collection<RepairSegment> runningSegments, RepairRun repairRun) {
    RepairUnit repairUnit = context.storage.getRepairUnit(repairRun.getRepairUnitId());
    for (RepairSegment segment : runningSegments) {
      LOG.debug("Trying to abort stuck segment {} in repair run {}", segment.getId(), repairRun.getId());
      SegmentRunner.postponeSegment(context, segment);
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
        startRunner(updatedRun);
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
        startRunner(runToBeStarted);
        return runToBeStarted;
      case ERROR: {
        RepairRun updatedRun = runToBeStarted.with().runState(RepairRun.RunState.RUNNING).endTime(null).build(
            runToBeStarted.getId());
        if (!context.storage.updateRepairRun(updatedRun)) {
          throw new ReaperException("failed updating repair run " + updatedRun.getId());
        }
        startRunner(updatedRun);
        return updatedRun;
      }
      default:
        throw new ReaperException("cannot start run with state: " + runToBeStarted.getRunState());
    }
  }

  public RepairRun updateRepairRunIntensity(RepairRun repairRun, Double intensity) throws ReaperException {
    RepairRun updatedRun = repairRun.with().intensity(intensity).build(repairRun.getId());
    if (!context.storage.updateRepairRun(updatedRun, Optional.of(false))) {
      throw new ReaperException("failed updating repair run " + updatedRun.getId());
    }
    return updatedRun;
  }

  private void startRunner(RepairRun run) {
    try {
      repairRunnersLock.lock();

      Preconditions.checkState(
          !repairRunners.containsKey(run.getId()),
          "there is already a repair runner for run with id " + run.getId() + ". This should not happen.");

      LOG.debug("scheduling repair for repair run #{}", run.getId());

      if (countRepairRunnersForCluster(run.getClusterName()) >= maxParallelRepairs) {
        LOG.debug("Maximum parallel repairs reached ({}). Postponing repair run with id {}",
            maxParallelRepairs, run.getId());
        // Update the last event on the repair run
        RepairRun postponedRun = run.with().lastEvent("Maximum parallel repairs reached, postponing repair run.").build(
            run.getId());
        if (!context.storage.updateRepairRun(postponedRun, Optional.of(false))) {
          LOG.warn("Failed to updating repair run #" + run.getId());
        }

      } else {
        try {
          RepairRunner newRunner = RepairRunner.create(context, run.getId(), clusterFacade);
          repairRunners.put(run.getId(), newRunner);
          executor.submit(newRunner);
        } catch (ReaperException e) {
          LOG.warn("Failed to schedule repair for repair run #" + run.getId(), e);
        }
      }
    } finally {
      repairRunnersLock.unlock();
    }
  }

  @VisibleForTesting
  public int countRepairRunnersForCluster(String clusterName) {
    Long repairRunnersForCluster
        = repairRunners.entrySet()
          .stream()
          .map(entry -> entry.getValue())
          .filter(runner -> runner.getCluster().getName().equals(clusterName))
          .count();
    return repairRunnersForCluster.intValue();
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
        .endTime(DateTime.now())
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
    executor.shutdownNow();
  }
}
