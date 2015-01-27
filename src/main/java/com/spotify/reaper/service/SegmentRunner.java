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

import com.google.common.base.Optional;

import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxConnectionFactory;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.cassandra.RepairStatusHandler;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.core.RepairUnit;
import com.spotify.reaper.storage.IStorage;

import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.SimpleCondition;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

public final class SegmentRunner implements RepairStatusHandler {

  private static final Logger LOG = LoggerFactory.getLogger(SegmentRunner.class);
  private static final int MAX_PENDING_COMPACTIONS = 20;

  private final IStorage storage;
  private final long segmentId;
  private final Condition condition = new SimpleCondition();
  private int commandId;


  private SegmentRunner(IStorage storage, long segmentId) {
    this.storage = storage;
    this.segmentId = segmentId;
  }

  public static void triggerRepair(IStorage storage, long segmentId,
                                   Collection<String> potentialCoordinators, long timeoutMillis,
                                   JmxConnectionFactory jmxConnectionFactory) {
    new SegmentRunner(storage, segmentId)
        .runRepair(potentialCoordinators, jmxConnectionFactory, timeoutMillis);
  }

  public static void postpone(IStorage storage, RepairSegment segment) {
    LOG.warn("Postponing segment {}", segment.getId());
    storage.updateRepairSegment(segment.with()
                                    .state(RepairSegment.State.NOT_STARTED)
                                    .coordinatorHost(null)
                                    .repairCommandId(null)
                                    .startTime(null)
                                    .failCount(segment.getFailCount() + 1)
                                    .build(segment.getId()));
  }

  public static void abort(IStorage storage, RepairSegment segment, JmxProxy jmxConnection) {
    postpone(storage, segment);
    LOG.warn("Aborting command {} on segment {}", segment.getRepairCommandId(), segment.getId());
    jmxConnection.cancelAllRepairs();
  }

  private void runRepair(Collection<String> potentialCoordinators,
                         JmxConnectionFactory jmxConnectionFactory, long timeoutMillis) {
    final RepairSegment segment = storage.getRepairSegment(segmentId).get();
    try (JmxProxy coordinator = jmxConnectionFactory
        .connectAny(Optional.<RepairStatusHandler>of(this), potentialCoordinators)) {
      RepairUnit repairUnit = storage.getRepairUnit(segment.getRepairUnitId()).get();
      String keyspace = repairUnit.getKeyspaceName();

      if (!canRepair(segment, keyspace, coordinator, jmxConnectionFactory)) {
        postpone(segment);
        return;
      }

      synchronized (condition) {
        commandId = coordinator.triggerRepair(segment.getStartToken(), segment.getEndToken(),
                                              keyspace, repairUnit.getRepairParallelism(),
                                              repairUnit.getColumnFamilies());
        LOG.debug("Triggered repair with command id {}", commandId);
        storage.updateRepairSegment(segment.with()
                                        .coordinatorHost(coordinator.getHost())
                                        .repairCommandId(commandId)
                                        .build(segmentId));
        LOG.info("Repair for segment {} started, status wait will timeout in {} millis",
                 segmentId, timeoutMillis);

        try {
          condition.await(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          LOG.warn("Repair command {} on segment {} interrupted", commandId, segmentId);
        } finally {
          RepairSegment resultingSegment = storage.getRepairSegment(segmentId).get();
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
          }
        }
      }
    } catch (ReaperException e) {
      LOG.warn("Failed to connect to a coordinator node for segment {}", segmentId);
      postpone(segment);
    }
  }

  boolean canRepair(RepairSegment segment, String keyspace, JmxProxy coordinator,
                    JmxConnectionFactory factory) throws ReaperException {
    Collection<String> allHosts =
        coordinator.tokenRangeToEndpoint(keyspace, segment.getTokenRange());
    for (String hostName : allHosts) {
      try (JmxProxy hostProxy = factory.connect(hostName)) {
        LOG.debug("checking host '{}' for pending compactions and other repairs (can repair?)",
                  hostName);
        if (hostProxy.getPendingCompactions() > MAX_PENDING_COMPACTIONS) {
          LOG.warn("SegmentRunner declined to repair segment {} because of too many pending "
                   + "compactions (> {}) on host \"{}\"", segmentId, MAX_PENDING_COMPACTIONS,
                   hostProxy.getHost());
          return false;
        }
        if (hostProxy.isRepairRunning()) {
          LOG.warn("SegmentRunner declined to repair segment {} because one of the hosts ({}) was "
                   + "already involved in a repair", segmentId, hostProxy.getHost());
          return false;
        }
      }
    }
    return true;
  }

  private void postpone(RepairSegment segment) {
    postpone(storage, segment);
  }

  private void abort(RepairSegment segment, JmxProxy jmxConnection) {
    abort(storage, segment, jmxConnection);
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

      RepairSegment currentSegment = storage.getRepairSegment(segmentId).get();
      // See status explanations from: https://wiki.apache.org/cassandra/RepairAsyncAPI
      switch (status) {
        case STARTED:
          DateTime now = DateTime.now();
          storage.updateRepairSegment(currentSegment.with()
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
          storage.updateRepairSegment(currentSegment.with()
                                          .state(RepairSegment.State.DONE)
                                          .endTime(DateTime.now())
                                          .build(segmentId));
          condition.signalAll();
          break;
      }
    }
  }
}
