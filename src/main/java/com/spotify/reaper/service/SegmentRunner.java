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
package com.spotify.reaper.service;

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

import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.commons.lang3.concurrent.ConcurrentException;
import org.apache.commons.lang3.concurrent.LazyInitializer;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.spotify.reaper.AppContext;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxConnectionFactory;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.cassandra.RepairStatusHandler;
import com.spotify.reaper.core.HostMetrics;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.core.RepairUnit;
import com.spotify.reaper.storage.IDistributedStorage;
import com.spotify.reaper.utils.SimpleCondition;
import com.sun.management.UnixOperatingSystemMXBean;

public final class SegmentRunner implements RepairStatusHandler, Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(SegmentRunner.class);

  private static final int MAX_PENDING_COMPACTIONS = 20;
  private static final int MAX_TIMEOUT_EXTENSIONS = 10;
  private static final Pattern REPAIR_UUID_PATTERN =
      Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
  private static final long SLEEP_TIME_AFTER_POSTPONE_IN_MS = 10000;

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
  private AtomicBoolean timedOut;
  private static final ExecutorService metricsGrabberExecutor = Executors.newFixedThreadPool(10);

  // Caching all active SegmentRunners.
  @VisibleForTesting
  public static Map<UUID, SegmentRunner> segmentRunners = Maps.newConcurrentMap();

  public SegmentRunner(AppContext context, UUID segmentId, Collection<String> potentialCoordinators,
      long timeoutMillis, double intensity, RepairParallelism validationParallelism,
      String clusterName, RepairUnit repairUnit, RepairRunner repairRunner) {

    assert !segmentRunners.containsKey(segmentId) : "SegmentRunner already exists for segment with ID: " + segmentId;
    this.context = context;
    this.segmentId = segmentId;
    this.potentialCoordinators = potentialCoordinators;
    this.timeoutMillis = timeoutMillis;
    this.intensity = intensity;
    this.validationParallelism = validationParallelism;
    this.clusterName = clusterName;
    this.repairUnit = repairUnit;
    this.repairRunner = repairRunner;
    this.timedOut = new AtomicBoolean(false);
  }

  @Override
  public void run() {
    final RepairSegment segment = context.storage.getRepairSegment(repairRunner.getRepairRunId(), segmentId).get();
    Thread.currentThread().setName(clusterName + ":" + segment.getRunId() + ":" + segmentId);

    if (renewLeadOnSegment(segmentId)) {
      if(runRepair()) {
        long delay = intensityBasedDelayMillis(intensity);
        try {
          Thread.sleep(delay);
        } catch (InterruptedException e) {
          LOG.warn("Slept shorter than intended delay.");
        }
      }      
      releaseLeadOnSegment(segmentId);
    }
  }

  public static void postpone(AppContext context, RepairSegment segment, Optional<RepairUnit> repairUnit) {
    LOG.info("Postponing segment {}", segment.getId());
    context.storage.updateRepairSegment(segment.with()
        .state(RepairSegment.State.NOT_STARTED)
        .coordinatorHost(repairUnit.isPresent() && repairUnit.get().getIncrementalRepair()? segment.getCoordinatorHost():null) // set coordinator host to null only for full repairs
        .repairCommandId(null)
        .startTime(null)
        .failCount(segment.getFailCount() + 1)
        .build(segment.getId()));
    segmentRunners.remove(segment.getId());
  }

  public static void abort(AppContext context, RepairSegment segment, JmxProxy jmxConnection) {
    postpone(context, segment, context.storage.getRepairUnit(segment.getRepairUnitId()));
    LOG.info("Aborting repair on segment with id {} on coordinator {}",
        segment.getId(), segment.getCoordinatorHost());
    jmxConnection.cancelAllRepairs();
  }

  /**
   * Remember to call method postponeCurrentSegment() outside of synchronized(condition) block.
   */
  public void postponeCurrentSegment() {
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
   * This method is intended to be temporary, until we find the root issue of too many open files
   * issue.
   */
  private long getOpenFilesAmount() {
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
    try (JmxProxy coordinator = context.jmxConnectionFactory
        .connectAny(Optional.<RepairStatusHandler>fromNullable(this), potentialCoordinators, context.config.getJmxConnectionTimeoutInSeconds())) {

      if (segmentRunners.containsKey(segmentId)) {
        LOG.error("SegmentRunner already exists for segment with ID: {}", segmentId);
        closeJmxConnection(Optional.fromNullable(coordinator));
        throw new ReaperException("SegmentRunner already exists for segment with ID: " + segmentId);
      }
      segmentRunners.put(segmentId, this);

      RepairUnit repairUnit = context.storage.getRepairUnit(segment.getRepairUnitId()).get();
      String keyspace = repairUnit.getKeyspaceName();
      boolean fullRepair = !repairUnit.getIncrementalRepair();

      // If this segment is blocked by other repairs on the hosts involved, we will want to double-
      // check with storage, whether those hosts really should be busy with repairs. This listing of
      // busy hosts isn't a cheap operation, so only do it (once) when repairs block the segment.
      LazyInitializer<Set<String>> busyHosts = new LazyInitializer<Set<String>>() {
        @Override
        protected Set<String> initialize() {
          Collection<RepairParameters> ongoingRepairs =
              context.storage.getOngoingRepairsInCluster(clusterName);
          Set<String> busyHosts = Sets.newHashSet();
          for (RepairParameters ongoingRepair : ongoingRepairs) {
            busyHosts.addAll(coordinator.tokenRangeToEndpoint(ongoingRepair.keyspaceName,
                ongoingRepair.tokenRange));
          }
          return busyHosts;
        }
      };
      if (!canRepair(segment, keyspace, coordinator, busyHosts)) {
        postponeCurrentSegment();
        closeJmxConnection(Optional.fromNullable(coordinator));
        return false;
      }

      LOG.debug("Enter synchronized section with segment ID {}", segmentId);
      synchronized (condition) {
        commandId = coordinator.triggerRepair(segment.getStartToken(), segment.getEndToken(),
            keyspace, validationParallelism, repairUnit.getColumnFamilies(), fullRepair);

        if (commandId == 0) {
          // From cassandra source in "forceRepairAsync":
          //if (ranges.isEmpty() || Keyspace.open(keyspace).getReplicationStrategy().getReplicationFactor() < 2)
          //  return 0;
          LOG.info("Nothing to repair for keyspace {}", keyspace);
          context.storage.updateRepairSegment(segment.with()
              .coordinatorHost(coordinator.getHost())
              .state(RepairSegment.State.DONE)
              .build(segmentId));
          segmentRunners.remove(segment.getId());
          closeJmxConnection(Optional.fromNullable(coordinator));
          return true;
        }

        LOG.debug("Triggered repair with command id {}", commandId);

        // incremental repair can take way more time for a segment so we're extending the timeout MAX_TIMEOUT_EXTENSIONS times
        long timeout = repairUnit.getIncrementalRepair()?timeoutMillis*MAX_TIMEOUT_EXTENSIONS:timeoutMillis;
        context.storage.updateRepairSegment(segment.with()
            .coordinatorHost(coordinator.getHost())
            .repairCommandId(commandId)
            .build(segmentId));
        String eventMsg = String.format("Triggered repair of segment %s via host %s",
            segment.getId(), coordinator.getHost());
        repairRunner.updateLastEvent(eventMsg);
        LOG.info("Repair for segment {} started, status wait will timeout in {} millis", segmentId,
            timeout);
        try {
          long startTime = System.currentTimeMillis();
          long maxTime = startTime + timeoutMillis;

          // If timeout is lower than 1mn, use timeout, otherwise we'll loop every minute to renew lead on segment
          long waitTime = timeoutMillis<60000?timeoutMillis:60000;

          long lastLoopTime = System.currentTimeMillis();
          while (System.currentTimeMillis() < maxTime) {
            condition.await(waitTime, TimeUnit.MILLISECONDS);
            if(lastLoopTime + 60_000 > System.currentTimeMillis() || context.storage.getRepairSegment(segment.getRunId(), segmentId).get().getState() == RepairSegment.State.DONE){
              // The condition has been interrupted, meaning the repair might be over
              break;
            }

            // Repair is still running, we'll renew lead on the segment when using Cassandra as storage backend
            renewLeadOnSegment(segmentId);
            lastLoopTime = System.currentTimeMillis();
          }
        } catch (InterruptedException e) {
          LOG.warn("Repair command {} on segment {} interrupted", commandId, segmentId, e);
        } finally {
          RepairSegment resultingSegment = context.storage.getRepairSegment(repairRunner.getRepairRunId(), segmentId).get();
          LOG.info("Repair command {} on segment {} returned with state {}", commandId, segmentId,
              resultingSegment.getState());
          if (resultingSegment.getState() == RepairSegment.State.RUNNING) {
            LOG.info("Repair command {} on segment {} has been cancelled while running", commandId,
                segmentId);
            timedOut.set(true);
            abort(resultingSegment, coordinator);
          } else if (resultingSegment.getState() == RepairSegment.State.DONE) {
            segmentRunners.remove(resultingSegment.getId());
          }
        }
      }
      closeJmxConnection(Optional.fromNullable(coordinator));
    } catch (ReaperException e) {
      LOG.warn("Failed to connect to a coordinator node for segment {}", segmentId, e);
      String msg = "Postponed a segment because no coordinator was reachable";
      repairRunner.updateLastEvent(msg);
      postponeCurrentSegment();
      LOG.warn("Open files amount for process: " + getOpenFilesAmount());
      return false;
    }
    LOG.debug("Exiting synchronized section with segment ID {}", segmentId);
    return true;
  }

  private void closeJmxConnection(Optional<JmxProxy> jmxProxy) {
    if(jmxProxy.isPresent())
      try {
        jmxProxy.get().close();
      } catch (ReaperException e) {
        LOG.warn("Could not close JMX connection to {}. Potential leak...", jmxProxy.get().getHost());
      }
  }

  private void declineRun() {
    LOG.info("SegmentRunner declined to repair segment {} because only one segment is allowed "
        + "at once for incremental repairs", segmentId);
    String msg = "Postponed due to already running segment";
    repairRunner.updateLastEvent(msg);
  }

  boolean canRepair(RepairSegment segment, String keyspace, JmxProxy coordinator,
      LazyInitializer<Set<String>> busyHosts) {
    Collection<String> allHosts;
    if(repairUnit.getIncrementalRepair()){
    	// In incremental repairs, only one segment is allowed at once (one segment == the full primary range of one node)
    	if(repairHasSegmentRunning(segment.getRunId())) {
    	  declineRun();
        return false;
    	}

    	if (IsRepairRunningOnOneNode(segment)) {
    	  declineRun();
    	  return false;
    	}


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

    boolean gotMetricsForAllHosts = true;

    List<Callable<Optional<HostMetrics>>> getMetricsTasks = allHosts.stream()
        .map(host -> getMetricsForHost(host))
        .collect(Collectors.toList());

    List<Future<Optional<HostMetrics>>> nodesMetrics = Lists.newArrayList();

    try {
      nodesMetrics = this.metricsGrabberExecutor.invokeAll(getMetricsTasks);
    } catch (Exception e) {
      LOG.debug("failed grabbing nodes metrics", e);
    }

    for (Future<Optional<HostMetrics>> nodeMetricsFuture : nodesMetrics) {
      Optional<HostMetrics> hostMetrics = Optional.absent();
      try {
        hostMetrics = nodeMetricsFuture.get();
        if(!hostMetrics.isPresent()) {
          gotMetricsForAllHosts = false;
        }
        else {
          int pendingCompactions = hostMetrics.get().getPendingCompactions();
          if (pendingCompactions > MAX_PENDING_COMPACTIONS) {
            LOG.info("SegmentRunner declined to repair segment {} because of too many pending "
                     + "compactions (> {}) on host \"{}\"", segmentId, MAX_PENDING_COMPACTIONS,
                     hostMetrics.get().getHostAddress());
            String msg = String.format("Postponed due to pending compactions (%d)",
                pendingCompactions);
            repairRunner.updateLastEvent(msg);

            return false;
          }
          if (hostMetrics.get().hasRepairRunning()) {
            LOG.info("SegmentRunner declined to repair segment {} because one of the hosts ({}) was "
                     + "already involved in a repair", segmentId, hostMetrics.get().getHostAddress());
            String msg = "Postponed due to affected hosts already doing repairs";
            repairRunner.updateLastEvent(msg);
            handlePotentialStuckRepairs(busyHosts, hostMetrics.get().getHostAddress());
            return false;
          }
        }
      } catch (InterruptedException | ExecutionException | ConcurrentException e) {
        LOG.warn("Failed grabbing metrics from at least one node. Cannot repair segment :'(", e);
        gotMetricsForAllHosts = false;
      }
    }


    if(gotMetricsForAllHosts) {
      LOG.info("It is ok to repair segment '{}' on repair run with id '{}'",
          segment.getId(), segment.getRunId());
    }
    else {
      LOG.info("Not ok to repair segment '{}' on repair run with id '{}' because we couldn't get all hosts metrics :'(",
          segment.getId(), segment.getRunId());
    }

    return gotMetricsForAllHosts; // check if we should postpone when we cannot get all metrics, or just drop the lead
  }

  private void handlePotentialStuckRepairs(LazyInitializer<Set<String>> busyHosts, String hostName) throws ConcurrentException {
    if (!busyHosts.get().contains(hostName) && context.storage instanceof IDistributedStorage) {
      LOG.warn("A host ({}) reported that it is involved in a repair, but there is no record "
          + "of any ongoing repair involving the host. Sending command to abort all repairs "
          + "on the host.", hostName);
      try (JmxProxy hostProxy = context.jmxConnectionFactory.connect(hostName, context.config.getJmxConnectionTimeoutInSeconds())) {
        hostProxy.cancelAllRepairs();
        hostProxy.close();
      } catch (Exception e) {
        LOG.debug("failed to cancel repairs on host {}", hostName, e);
      }
    }
  }

  Callable<Optional<HostMetrics>> getMetricsForHost(String hostName) {
    return () -> {
      try (JmxProxy hostProxy = context.jmxConnectionFactory.connect(hostName, context.config.getJmxConnectionTimeoutInSeconds())) {
        int pendingCompactions = hostProxy.getPendingCompactions();
        boolean hasRepairRunning = hostProxy.isRepairRunning();

        HostMetrics metrics = HostMetrics.builder().withHostAddress(hostName)
                                                   .withPendingCompactions(pendingCompactions)
                                                   .withHasRepairRunning(hasRepairRunning)
                                                   .withActiveAnticompactions(0) // for future use
                                                   .build();
        storeHostMetrics(metrics);
        hostProxy.close();
        return Optional.fromNullable(metrics);
      } catch (Exception e) {
        LOG.debug("failed to query metrics for host {}, trying to get metrics from storage...", hostName, e);
        return getHostMetrics(hostName);
      }
    };
  }

  private boolean IsRepairRunningOnOneNode(RepairSegment segment) {
    for(RepairSegment segmentInRun:context.storage.getRepairSegmentsForRun(segment.getRunId())){
      try (JmxProxy hostProxy = context.jmxConnectionFactory.connect(segmentInRun.getCoordinatorHost(), context.config.getJmxConnectionTimeoutInSeconds())) {
        if(hostProxy.isRepairRunning()) {
          return true;
        }
      } catch (ReaperException e) {
        LOG.error("Unreachable node when trying to determine if repair is running on a node. Crossing fingers and continuing...", e);
      }
    }

    return false;

  }

  private boolean repairHasSegmentRunning(UUID repairRunId) {
	  Collection<RepairSegment> segments = context.storage.getRepairSegmentsForRun(repairRunId);
	  for(RepairSegment segment:segments) {
		  if(segment.getState() == RepairSegment.State.RUNNING) {
			  LOG.info("segment '{}' is running on host '{}' and with a fail count of {}",
				        segment.getId(), segment.getCoordinatorHost(), segment.getFailCount());
			  return true;
		  }
	  }

	  return false;
}

private void abort(RepairSegment segment, JmxProxy jmxConnection) {
    abort(context, segment, jmxConnection);
  }

  /**
   * Called when there is an event coming either from JMX or this runner regarding on-going
   * repairs.
   *
   * @param repairNumber repair sequence number, obtained when triggering a repair
   * @param status       new status of the repair
   * @param message      additional information about the repair
   */
  @Override
  public void handle(int repairNumber, Optional<ActiveRepairService.Status> status, Optional<ProgressEventType> progress, String message) {
    final RepairSegment segment = context.storage.getRepairSegment(repairRunner.getRepairRunId(), segmentId).get();
    Thread.currentThread().setName(clusterName + ":" + segment.getRunId() + ":" + segmentId);
    LOG.debug(
        "handle called for repairCommandId {}, outcome {} / {} and message: {}",
        repairNumber, status, progress, message);
    if (repairNumber != commandId) {
      LOG.debug("Handler for command id {} not handling message with number {}",
          commandId, repairNumber);
      return;
    }

    boolean failOutsideSynchronizedBlock = false;
    // DO NOT ADD EXTERNAL CALLS INSIDE THIS SYNCHRONIZED BLOCK (JMX PROXY ETC)
    synchronized (condition) {
      RepairSegment currentSegment = context.storage.getRepairSegment(repairRunner.getRepairRunId(), segmentId).get();
      // See status explanations at: https://wiki.apache.org/cassandra/RepairAsyncAPI
      // Old repair API
      if(status.isPresent()) {
        switch (status.get()) {
          case STARTED:
            DateTime now = DateTime.now();
            context.storage.updateRepairSegment(currentSegment.with()
                .state(RepairSegment.State.RUNNING)
                .startTime(now)
                .build(segmentId));
            renewLeadOnSegment(segmentId);
            LOG.debug("updated segment {} with state {}", segmentId, RepairSegment.State.RUNNING);
            break;

          case SESSION_SUCCESS:
            if (timedOut.get()) {
              LOG.debug("Got SESSION_SUCCESS for segment with id '{}' and repair number '{}', " +
                        "but it had already timed out",
                        segmentId, repairNumber);
            } else {
              LOG.debug("repair session succeeded for segment with id '{}' and repair number '{}'",
                      segmentId, repairNumber);
              context.storage.updateRepairSegment(currentSegment.with()
                  .state(RepairSegment.State.DONE)
                  .endTime(DateTime.now())
                  .build(segmentId));
            }
            break;

          case SESSION_FAILED:
            LOG.warn("repair session failed for segment with id '{}' and repair number '{}'",
                segmentId, repairNumber);
            failOutsideSynchronizedBlock = true;
            break;

          case FINISHED:
            // This gets called through the JMX proxy at the end
            // regardless of succeeded or failed sessions.
            LOG.debug("repair session finished for segment with id '{}' and repair number '{}'",
                segmentId, repairNumber);
            condition.signalAll();
            break;
        }
      }
   // New repair API
      if(progress.isPresent()) {
        switch (progress.get()) {
          case START:
            DateTime now = DateTime.now();
            context.storage.updateRepairSegment(currentSegment.with()
                .state(RepairSegment.State.RUNNING)
                .startTime(now)
                .build(segmentId));
            renewLeadOnSegment(segmentId);
            LOG.debug("updated segment {} with state {}", segmentId, RepairSegment.State.RUNNING);
            break;

          case SUCCESS:
            LOG.debug("repair session succeeded for segment with id '{}' and repair number '{}'",
                segmentId, repairNumber);
            context.storage.updateRepairSegment(currentSegment.with()
                .state(RepairSegment.State.DONE)
                .endTime(DateTime.now())
                .build(segmentId));
            break;

          case ERROR:
          case ABORT:
            LOG.warn("repair session failed for segment with id '{}' and repair number '{}'",
                segmentId, repairNumber);
            failOutsideSynchronizedBlock = true;
            break;

          case COMPLETE:
            // This gets called through the JMX proxy at the end
            // regardless of succeeded or failed sessions.
            LOG.debug("repair session finished for segment with id '{}' and repair number '{}'",
                segmentId, repairNumber);
            condition.signalAll();
            break;
        default:
          LOG.debug("Unidentified progressStatus {} for segment with id '{}' and repair number '{}'",
              progress.get(), segmentId, repairNumber);
          break;
        }
      }
    }

    if (failOutsideSynchronizedBlock) {
      postponeCurrentSegment();
      tryClearSnapshots(message);
    }
  }

  /**
   * Attempts to clear snapshots that are possibly left behind after failed repair sessions.
   */
  @VisibleForTesting
  protected void tryClearSnapshots(String message) {
    String keyspace = repairUnit.getKeyspaceName();
    String repairId = parseRepairId(message);
    if (repairId != null) {
      for (String involvedNode : potentialCoordinators) {
        try (JmxProxy jmx = new JmxConnectionFactory().connect(involvedNode, context.config.getJmxConnectionTimeoutInSeconds())) {
          // there is no way of telling if the snapshot was cleared or not :(
          jmx.clearSnapshot(repairId, keyspace);
          jmx.close();
        } catch (ReaperException e) {
          LOG.warn("Failed to clear snapshot after failed session for host {}, keyspace {}: {}",
              involvedNode, keyspace, e.getMessage(), e);
        }
      }
    }
  }

  public static String parseRepairId(String message) {
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
      return delay*nbRunningReapers;
    } else {
      LOG.error("Segment {} returned with startTime {} and endTime {}. This should not happen."
                + "Intensity cannot apply, so next run will start immediately.",
          repairSegment.getId(), repairSegment.getStartTime(), repairSegment.getEndTime());
      return 0;
    }
  }

    private boolean renewLeadOnSegment(UUID segmentId) {
        return context.storage instanceof IDistributedStorage
            ? ((IDistributedStorage)context.storage).renewLeadOnSegment(segmentId)
            : true;
    }

    private void releaseLeadOnSegment(UUID segmentId) {
        if (context.storage instanceof IDistributedStorage) {
            ((IDistributedStorage)context.storage).releaseLeadOnSegment(segmentId);
        }
    }

    private void storeHostMetrics(HostMetrics metrics) {
        if (context.storage instanceof IDistributedStorage) {
            ((IDistributedStorage)context.storage).storeHostMetrics(metrics);
        }
    }

    private Optional<HostMetrics> getHostMetrics(String hostName) {
        return context.storage instanceof IDistributedStorage
            ? ((IDistributedStorage)context.storage).getHostMetrics(hostName)
            : Optional.absent();
    }

    private int countRunningReapers() {
        return context.storage instanceof IDistributedStorage
            ? ((IDistributedStorage)context.storage).countRunningReapers()
            : 1;
    }

}
