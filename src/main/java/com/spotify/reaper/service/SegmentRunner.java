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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.commons.lang3.concurrent.ConcurrentException;
import org.apache.commons.lang3.concurrent.LazyInitializer;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.spotify.reaper.AppContext;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxConnectionFactory;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.cassandra.RepairStatusHandler;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.core.RepairUnit;
import com.spotify.reaper.utils.SimpleCondition;
import com.sun.management.UnixOperatingSystemMXBean;

public final class SegmentRunner implements RepairStatusHandler, Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(SegmentRunner.class);

  private static final int MAX_PENDING_COMPACTIONS = 20;
  private static final Pattern REPAIR_UUID_PATTERN =
      Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");

  private final AppContext context;
  private final long segmentId;
  private final Condition condition = new SimpleCondition();
  private final Collection<String> potentialCoordinators;
  private final long timeoutMillis;
  private final double intensity;
  private final RepairParallelism validationParallelism;
  private final String clusterName;
  private final RepairRunner repairRunner;
  private final RepairUnit repairUnit;
  private int commandId;

  // Caching all active SegmentRunners.
  @VisibleForTesting
  public static Map<Long, SegmentRunner> segmentRunners = Maps.newConcurrentMap();

  public SegmentRunner(AppContext context, long segmentId, Collection<String> potentialCoordinators,
      long timeoutMillis, double intensity, RepairParallelism validationParallelism,
      String clusterName, RepairUnit repairUnit, RepairRunner repairRunner) {
    this.context = context;
    this.segmentId = segmentId;
    this.potentialCoordinators = potentialCoordinators;
    this.timeoutMillis = timeoutMillis;
    this.intensity = intensity;
    this.validationParallelism = validationParallelism;
    this.clusterName = clusterName;
    this.repairUnit = repairUnit;
    this.repairRunner = repairRunner;
  }

  @Override
  public void run() {
    final RepairSegment segment = context.storage.getRepairSegment(segmentId).get();
    Thread.currentThread().setName(clusterName + ":" + segment.getRunId() + ":" + segmentId);

    runRepair();
    long delay = intensityBasedDelayMillis(intensity);
    try {
      Thread.sleep(delay);
    } catch (InterruptedException e) {
      LOG.warn("Slept shorter than intended delay.");
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
    postpone(context, segment, Optional.fromNullable((RepairUnit) null));
    LOG.info("Aborting repair on segment with id {} on coordinator {}",
        segment.getId(), segment.getCoordinatorHost());
    jmxConnection.cancelAllRepairs();
  }

  /**
   * Remember to call method postponeCurrentSegment() outside of synchronized(condition) block.
   */
  public void postponeCurrentSegment() {
    synchronized (condition) {
      RepairSegment segment = context.storage.getRepairSegment(segmentId).get();
      postpone(context, segment, context.storage.getRepairUnit(segment.getRepairUnitId()));
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

  private void runRepair() {
    LOG.debug("Run repair for segment #{}", segmentId);
    final RepairSegment segment = context.storage.getRepairSegment(segmentId).get();
    try (JmxProxy coordinator = context.jmxConnectionFactory
        .connectAny(Optional.<RepairStatusHandler>of(this), potentialCoordinators)) {

      if (segmentRunners.containsKey(segmentId)) {
        LOG.error("SegmentRunner already exists for segment with ID: " + segmentId);
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
        return;
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
          return;
        }

        LOG.debug("Triggered repair with command id {}", commandId);
        context.storage.updateRepairSegment(segment.with()
            .coordinatorHost(coordinator.getHost())
            .repairCommandId(commandId)
            .build(segmentId));
        String eventMsg = String.format("Triggered repair of segment %d via host %s",
            segment.getId(), coordinator.getHost());
        repairRunner.updateLastEvent(eventMsg);
        LOG.info("Repair for segment {} started, status wait will timeout in {} millis", segmentId,
            timeoutMillis);
        try {
          condition.await(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          LOG.warn("Repair command {} on segment {} interrupted", commandId, segmentId);
        } finally {
          RepairSegment resultingSegment = context.storage.getRepairSegment(segmentId).get();
          LOG.info("Repair command {} on segment {} returned with state {}", commandId, segmentId,
              resultingSegment.getState());
          if (resultingSegment.getState() == RepairSegment.State.RUNNING) {
            LOG.info("Repair command {} on segment {} has been cancelled while running", commandId,
                segmentId);
            abort(resultingSegment, coordinator);
          } else if (resultingSegment.getState() == RepairSegment.State.DONE) {
            LOG.debug("Repair segment with id '{}' was repaired in {} seconds",
                resultingSegment.getId(),
                Seconds.secondsBetween(
                    resultingSegment.getStartTime(),
                    resultingSegment.getEndTime()).getSeconds());
            segmentRunners.remove(resultingSegment.getId());
          }
        }
      }
    } catch (ReaperException e) {
      LOG.warn("Failed to connect to a coordinator node for segment {}", segmentId);
      LOG.warn(e.getMessage());
      String msg = "Postponed a segment because no coordinator was reachable";
      repairRunner.updateLastEvent(msg);
      postponeCurrentSegment();
      LOG.warn("Open files amount for process: " + getOpenFilesAmount());
    }
    LOG.debug("Exiting synchronized section with segment ID {}", segmentId);
  }

  boolean canRepair(RepairSegment segment, String keyspace, JmxProxy coordinator,
      LazyInitializer<Set<String>> busyHosts) {
    Collection<String> allHosts;
    if(repairUnit.getIncrementalRepair()){
    	// In incremental repairs, only one segment is allowed at once (one segment == the full primary range of one node)
    	if(repairHasSegmentRunning(segment.getRunId())) {
    		LOG.info("SegmentRunner declined to repair segment {} because only one segment is allowed "
                    + "at once for incremental repairs", segmentId);
           String msg = String.format("Postponed due to already running segment");
           repairRunner.updateLastEvent(msg);
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

    for (String hostName : allHosts) {
      LOG.debug("checking host '{}' for pending compactions and other repairs (can repair?)"
                + " Run id '{}'", hostName, segment.getRunId());
      try (JmxProxy hostProxy = context.jmxConnectionFactory.connect(hostName)) {
        int pendingCompactions = hostProxy.getPendingCompactions();
        if (pendingCompactions > MAX_PENDING_COMPACTIONS) {
          LOG.info("SegmentRunner declined to repair segment {} because of too many pending "
                   + "compactions (> {}) on host \"{}\"", segmentId, MAX_PENDING_COMPACTIONS,
              hostProxy.getHost());
          String msg = String.format("Postponed due to pending compactions (%d)",
              pendingCompactions);
          repairRunner.updateLastEvent(msg);
          return false;
        }
        if (hostProxy.isRepairRunning()) {
          LOG.info("SegmentRunner declined to repair segment {} because one of the hosts ({}) was "
                   + "already involved in a repair", segmentId, hostProxy.getHost());
          String msg = "Postponed due to affected hosts already doing repairs";
          repairRunner.updateLastEvent(msg);
          if (!busyHosts.get().contains(hostName)) {
            LOG.warn("A host ({}) reported that it is involved in a repair, but there is no record "
                + "of any ongoing repair involving the host. Sending command to abort all repairs "
                + "on the host.", hostProxy.getHost());
            hostProxy.cancelAllRepairs();
          }
          return false;
        }
      } catch (ReaperException e) {
        LOG.warn("SegmentRunner declined to repair segment {} because one of the hosts ({}) could "
                 + "not be connected with", segmentId, hostName);
        String msg = String.format("Postponed due to inability to connect host %s", hostName);
        repairRunner.updateLastEvent(msg);
        return false;
      } catch (RuntimeException e) {
        LOG.warn("SegmentRunner declined to repair segment {} because of an error collecting "
                 + "information from one of the hosts ({}): {}", segmentId, hostName, e);
        String msg = String.format("Postponed due to inability to collect "
                                   + "information from host %s", hostName);
        repairRunner.updateLastEvent(msg);
        LOG.warn("Open files amount for process: " + getOpenFilesAmount());
        return false;
      } catch (ConcurrentException e) {
        LOG.warn("Exception thrown while listing all nodes in cluster \"{}\" with ongoing repairs: "
            + "{}", clusterName, e);
        return false;
      }
    }
    LOG.info("It is ok to repair segment '{}' on repair run with id '{}'",
        segment.getId(), segment.getRunId());
    return true;
  }

  private boolean repairHasSegmentRunning(long repairRunId) {
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
  public void handle(int repairNumber, ActiveRepairService.Status status, String message) {
    final RepairSegment segment = context.storage.getRepairSegment(segmentId).get();
    Thread.currentThread().setName(clusterName + ":" + segment.getRunId() + ":" + segmentId);
    LOG.debug(
        "handle called for repairCommandId {}, outcome {} and message: {}",
        repairNumber, status, message);
    if (repairNumber != commandId) {
      LOG.debug("Handler for command id {} not handling message with number {}",
          commandId, repairNumber);
      return;
    }

    boolean failOutsideSynchronizedBlock = false;
    // DO NOT ADD EXTERNAL CALLS INSIDE THIS SYNCHRONIZED BLOCK (JMX PROXY ETC)
    synchronized (condition) {
      RepairSegment currentSegment = context.storage.getRepairSegment(segmentId).get();
      // See status explanations at: https://wiki.apache.org/cassandra/RepairAsyncAPI
      switch (status) {
        case STARTED:
          DateTime now = DateTime.now();
          context.storage.updateRepairSegment(currentSegment.with()
              .state(RepairSegment.State.RUNNING)
              .startTime(now)
              .build(segmentId));
          LOG.debug("updated segment {} with state {}", segmentId, RepairSegment.State.RUNNING);
          break;

        case SESSION_SUCCESS:
          LOG.debug("repair session succeeded for segment with id '{}' and repair number '{}'",
              segmentId, repairNumber);
          context.storage.updateRepairSegment(currentSegment.with()
              .state(RepairSegment.State.DONE)
              .endTime(DateTime.now())
              .build(segmentId));
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
        try (JmxProxy jmx = new JmxConnectionFactory().connect(involvedNode)) {
          // there is no way of telling if the snapshot was cleared or not :(
          jmx.clearSnapshot(repairId, keyspace);
        } catch (ReaperException e) {
          LOG.warn("Failed to clear snapshot after failed session for host {}, keyspace {}: {}",
              involvedNode, keyspace, e.getMessage());
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
    RepairSegment repairSegment = context.storage.getRepairSegment(segmentId).get();
    if (repairSegment.getEndTime() == null && repairSegment.getStartTime() == null) {
      return 0;
    } else if (repairSegment.getEndTime() != null && repairSegment.getStartTime() != null) {
      long repairEnd = repairSegment.getEndTime().getMillis();
      long repairStart = repairSegment.getStartTime().getMillis();
      long repairDuration = repairEnd - repairStart;
      long delay = (long) (repairDuration / intensity - repairDuration);
      LOG.debug("Scheduling next runner run() with delay {} ms", delay);
      return delay;
    } else {
      LOG.error("Segment {} returned with startTime {} and endTime {}. This should not happen."
                + "Intensity cannot apply, so next run will start immediately.",
          repairSegment.getId(), repairSegment.getStartTime(), repairSegment.getEndTime());
      return 0;
    }
  }

}
