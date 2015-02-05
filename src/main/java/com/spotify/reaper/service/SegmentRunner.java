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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import com.spotify.reaper.AppContext;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.cassandra.RepairStatusHandler;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.core.RepairUnit;

import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.SimpleCondition;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

public final class SegmentRunner implements RepairStatusHandler {

  private static final Logger LOG = LoggerFactory.getLogger(SegmentRunner.class);
  private static final int MAX_PENDING_COMPACTIONS = 20;

  private final AppContext context;
  private final long segmentId;
  private final Condition condition = new SimpleCondition();
  private int commandId;

  // Caching all active SegmentRunners.
  @VisibleForTesting
  public static Map<Long, SegmentRunner> segmentRunners = Maps.newConcurrentMap();

  private SegmentRunner(AppContext context, long segmentId) {
    this.context = context;
    this.segmentId = segmentId;
  }

  /**
   * Triggers a repair for a segment. Is blocking call.
   */
  public static void triggerRepair(AppContext context, long segmentId,
                                   Collection<String> potentialCoordinators, long timeoutMillis)
      throws ReaperException {
    if (segmentRunners.containsKey(segmentId)) {
      throw new ReaperException("SegmentRunner already exists for segment with ID: " + segmentId);
    }
    SegmentRunner newSegmentRunner = new SegmentRunner(context, segmentId);
    segmentRunners.put(segmentId, newSegmentRunner);
    newSegmentRunner.runRepair(potentialCoordinators, timeoutMillis);
  }

  public static void postpone(AppContext context, RepairSegment segment) {
    LOG.warn("Postponing segment {}", segment.getId());
    context.storage.updateRepairSegment(segment.with()
                                            .state(RepairSegment.State.NOT_STARTED)
                                            .coordinatorHost(null)
                                            .repairCommandId(null)
                                            .startTime(null)
                                            .failCount(segment.getFailCount() + 1)
                                            .build(segment.getId()));
    segmentRunners.remove(segment.getId());
  }

  public static void abort(AppContext context, RepairSegment segment, JmxProxy jmxConnection) {
    postpone(context, segment);
    LOG.info("Aborting repair on segment with id {} on coordinator {}",
             segment.getId(), segment.getCoordinatorHost());
    jmxConnection.cancelAllRepairs();
  }

  @VisibleForTesting
  public int getCurrentCommandId() {
    return this.commandId;
  }

  private void runRepair(Collection<String> potentialCoordinators, long timeoutMillis) {
    final RepairSegment segment = context.storage.getRepairSegment(segmentId).get();
    final RepairRun repairRun = context.storage.getRepairRun(segment.getRunId()).get();
    try (JmxProxy coordinator = context.jmxConnectionFactory
        .connectAny(Optional.<RepairStatusHandler>of(this), potentialCoordinators)) {
      RepairUnit repairUnit = context.storage.getRepairUnit(segment.getRepairUnitId()).get();
      String keyspace = repairUnit.getKeyspaceName();

      if (!canRepair(segment, keyspace, coordinator)) {
        postpone(segment);
        return;
      }

      synchronized (condition) {
        commandId = coordinator.triggerRepair(segment.getStartToken(), segment.getEndToken(),
                                              keyspace, repairRun.getRepairParallelism(),
                                              repairUnit.getColumnFamilies());
        LOG.debug("Triggered repair with command id {}", commandId);
        context.storage.updateRepairSegment(segment.with()
                                                .coordinatorHost(coordinator.getHost())
                                                .repairCommandId(commandId)
                                                .build(segmentId));
        String eventMsg = String.format("Triggered repair of segment %d via host %s",
                                        segment.getId(), coordinator.getHost());
        context.storage.updateRepairRun(
            repairRun.with().lastEvent(eventMsg).build(repairRun.getId()));
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
          if (resultingSegment.getState().equals(RepairSegment.State.RUNNING)) {
            LOG.info("Repair command {} on segment {} has been cancelled while running", commandId,
                     segmentId);
            abort(resultingSegment, coordinator);
          } else if (resultingSegment.getState().equals(RepairSegment.State.DONE)) {
            LOG.debug("Repair segment with id '{}' was repaired in {} seconds",
                      resultingSegment.getId(),
                      Seconds.secondsBetween(resultingSegment.getEndTime(),
                                             resultingSegment.getStartTime()));
            segmentRunners.remove(resultingSegment.getId());
          }
        }
      }
    } catch (ReaperException e) {
      LOG.warn("Failed to connect to a coordinator node for segment {}", segmentId);
      String msg = String.format("Postponed because couldn't any of the coordinators");
      context.storage.updateRepairRun(repairRun.with().lastEvent(msg).build(repairRun.getId()));
      postpone(segment);
    }
  }

  boolean canRepair(RepairSegment segment, String keyspace, JmxProxy coordinator)
      throws ReaperException {
    Collection<String> allHosts =
        coordinator.tokenRangeToEndpoint(keyspace, segment.getTokenRange());
    for (String hostName : allHosts) {
      try (JmxProxy hostProxy = context.jmxConnectionFactory.connect(hostName)) {
        LOG.debug("checking host '{}' for pending compactions and other repairs (can repair?)"
                  + " Run id '{}'", hostName, segment.getRunId());
        int pendingCompactions = hostProxy.getPendingCompactions();
        if (pendingCompactions > MAX_PENDING_COMPACTIONS) {
          LOG.warn("SegmentRunner declined to repair segment {} because of too many pending "
                   + "compactions (> {}) on host \"{}\"", segmentId, MAX_PENDING_COMPACTIONS,
                   hostProxy.getHost());
          String msg = String.format("Postponed due to pending compactions (%d)",
                                     pendingCompactions);
          RepairRun repairRun = context.storage.getRepairRun(segment.getRunId()).get();
          context.storage.updateRepairRun(repairRun.with().lastEvent(msg).build(repairRun.getId()));
          return false;
        }
        if (hostProxy.isRepairRunning()) {
          LOG.warn("SegmentRunner declined to repair segment {} because one of the hosts ({}) was "
                   + "already involved in a repair", segmentId, hostProxy.getHost());
          String msg = String.format("Postponed due to affected hosts already doing repairs");
          RepairRun repairRun = context.storage.getRepairRun(segment.getRunId()).get();
          context.storage.updateRepairRun(repairRun.with().lastEvent(msg).build(repairRun.getId()));
          return false;
        }
      }
    }
    LOG.info("It is ok to repair segment '{}' om repair run with id '{}'",
             segment.getId(), segment.getRunId());
    return true;
  }

  private void postpone(RepairSegment segment) {
    postpone(context, segment);
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
    synchronized (condition) {
      LOG.debug(
          "handle called for repairCommandId {}, outcome {} and message: {}",
          repairNumber, status, message);
      if (repairNumber != commandId) {
        LOG.debug("Handler for command id {} not handling message with number {}",
                  commandId, repairNumber);
        return;
      }

      RepairSegment currentSegment = context.storage.getRepairSegment(segmentId).get();
      // See status explanations from: https://wiki.apache.org/cassandra/RepairAsyncAPI
      switch (status) {
        case STARTED:
          DateTime now = DateTime.now();
          context.storage.updateRepairSegment(currentSegment.with()
                                                  .state(RepairSegment.State.RUNNING)
                                                  .startTime(now)
                                                  .build(segmentId));
          break;
        case SESSION_FAILED:
          LOG.warn("repair session failed for segment with id '{}' and repair number '{}'",
                   segmentId, repairNumber);
          postpone(currentSegment);
          condition.signalAll();
          break;
        case SESSION_SUCCESS:
          // Do nothing, wait for FINISHED.
          break;
        case FINISHED:
          context.storage.updateRepairSegment(currentSegment.with()
                                                  .state(RepairSegment.State.DONE)
                                                  .endTime(DateTime.now())
                                                  .build(segmentId));
          condition.signalAll();
          break;
      }
    }
  }
}
