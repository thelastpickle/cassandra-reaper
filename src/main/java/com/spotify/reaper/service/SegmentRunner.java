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
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.cassandra.RepairStatusHandler;
import com.spotify.reaper.core.ColumnFamily;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.storage.IStorage;

import org.apache.cassandra.service.ActiveRepairService;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public final class SegmentRunner implements RepairStatusHandler {

  private static final Logger LOG = LoggerFactory.getLogger(SegmentRunner.class);

  private final IStorage storage;
  private final long segmentId;
  private final int commandId;
  private final JmxProxy jmxConnection;


  public static void triggerRepair(IStorage storage, long segmentId,
                                   Collection<String> potentialCoordinators, long timeoutMillis,
                                   JmxConnectionFactory jmxConnectionFactory)
      throws ReaperException, InterruptedException {
    new SegmentRunner(storage, segmentId, potentialCoordinators, jmxConnectionFactory)
        .awaitOutcome(timeoutMillis);
  }

  private SegmentRunner(IStorage storage, long segmentId, Collection<String> potentialCoordinators,
                        JmxConnectionFactory jmxConnectionFactory)
      throws ReaperException {
    this.storage = storage;
    this.segmentId = segmentId;

    // TODO: don't trigger the repair in the constructor. The change will force commandId to be
    // TODO: mutable, but that's better than this.
    synchronized (this) {
      jmxConnection = jmxConnectionFactory
          .connectAny(Optional.<RepairStatusHandler>of(this), potentialCoordinators);

      RepairSegment segment = storage.getRepairSegment(segmentId);
      ColumnFamily columnFamily =
          storage.getColumnFamily(segment.getColumnFamilyId());
      String keyspace = columnFamily.getKeyspaceName();

      assert !segment.getState().equals(RepairSegment.State.RUNNING);
      commandId = jmxConnection
          .triggerRepair(segment.getStartToken(), segment.getEndToken(), keyspace,
                         columnFamily.getName());
      LOG.debug("Triggered repair with command id {}", commandId);
      LOG.info("Repair for segment {} started", segmentId);
      storage.updateRepairSegment(segment.with()
                                      .state(RepairSegment.State.RUNNING)
                                      .repairCommandId(commandId)
                                      .build(segmentId));
    }
  }

  private synchronized void awaitOutcome(long timeoutMillis)
      throws InterruptedException, ReaperException {
    long abortTime = (System.nanoTime() / 1000000) + timeoutMillis;
    while (true) {
      RepairSegment segment = storage.getRepairSegment(segmentId);
      if (!segment.getState().equals(RepairSegment.State.RUNNING)) {
        LOG.info("Repair command {} on segment {} finished", commandId, segmentId);
        break;
      }
      long milliTime = System.nanoTime() / 1000000;
      if (milliTime < abortTime) {
        wait(abortTime - milliTime);
      } else {
        LOG.warn("Repair command {} on segment {} timed out", commandId, segmentId);
        abort(segment);
        break;
      }
    }
    jmxConnection.close();
  }

  private synchronized void abort(RepairSegment segment) {
    LOG.warn("Aborting command {} on segment {}", commandId, segmentId);
    storage.updateRepairSegment(segment.with()
                                    .startTime(null)
                                    .repairCommandId(null)
                                    .state(RepairSegment.State.NOT_STARTED)
                                    .build(segmentId));
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
  public synchronized void handle(int repairNumber, ActiveRepairService.Status status,
                                  String message) {
    LOG.debug(
        "handleRepairOutcome called for repairCommandId {}, outcome {} and message: {}",
        repairNumber, status, message);
    if (repairNumber != commandId) {
      LOG.debug("Handler for command id {} not handling message with number {}",
          commandId, repairNumber);
      return;
    }

    RepairSegment currentSegment = storage.getRepairSegment(segmentId);
    // See status explanations from: https://wiki.apache.org/cassandra/RepairAsyncAPI
    switch (status) {
      case STARTED:
        DateTime now = DateTime.now();
        storage.updateRepairSegment(currentSegment.with()
                                        .startTime(now)
                                        .build(segmentId));
        // We already set the state of the segment to RUNNING.
        break;
      case SESSION_FAILED:
        // TODO: Bj0rn: How should we handle this? Here, it's almost treated like a success.
        storage.updateRepairSegment(currentSegment.with()
                                        .state(RepairSegment.State.ERROR)
                                        .endTime(DateTime.now())
                                        .build(segmentId));
        notify();
        break;
      case SESSION_SUCCESS:
        // Do nothing, wait for FINISHED.
        break;
      case FINISHED:
        storage.updateRepairSegment(currentSegment.with()
                                        .state(RepairSegment.State.DONE)
                                        .endTime(DateTime.now())
                                        .build(segmentId));
        notify();
        break;
    }
  }
}
