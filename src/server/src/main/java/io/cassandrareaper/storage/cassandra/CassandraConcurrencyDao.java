/*
 * Copyright 2016-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
 * Copyright 2020-2020 DataStax, Inc.
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

package io.cassandrareaper.storage.cassandra;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.core.RepairSegment;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraConcurrencyDao {
  private static final int LEAD_DURATION = 90;
  /* Simple stmts */
  private static final String SELECT_LEADERS = "SELECT * FROM leader";
  private static final String SELECT_RUNNING_REAPERS =
      "SELECT reaper_instance_id FROM running_reapers";
  private static final Logger LOG = LoggerFactory.getLogger(CassandraConcurrencyDao.class);
  private final Version version;
  private final UUID reaperInstanceId;
  private final CqlSession session;
  private PreparedStatement takeLeadPrepStmt;
  private PreparedStatement renewLeadPrepStmt;
  private PreparedStatement releaseLeadPrepStmt;
  private PreparedStatement getRunningReapersCountPrepStmt;
  private PreparedStatement setRunningRepairsPrepStmt;
  private PreparedStatement getRunningRepairsPrepStmt;

  public CassandraConcurrencyDao(Version version, UUID reaperInstanceId, CqlSession session) {
    this.version = version;
    this.reaperInstanceId = reaperInstanceId;
    this.session = session;
    prepareStatements();
  }

  private void prepareStatements() {
    takeLeadPrepStmt =
        session.prepare(
            SimpleStatement.builder(
                    "INSERT INTO leader(leader_id, reaper_instance_id,"
                        + "reaper_instance_host, last_heartbeat)"
                        + "VALUES(?, ?, ?, toTimestamp(now())) IF NOT EXISTS USING TTL ?")
                .setConsistencyLevel(ConsistencyLevel.QUORUM)
                .build());
    renewLeadPrepStmt =
        session.prepare(
            SimpleStatement.builder(
                    "UPDATE leader USING TTL ? SET reaper_instance_id = ?, reaper_instance_host = ?,"
                        + " last_heartbeat = toTimestamp(now()) WHERE leader_id = ? IF reaper_instance_id = ?")
                .setConsistencyLevel(ConsistencyLevel.QUORUM)
                .build());
    releaseLeadPrepStmt =
        session.prepare(
            SimpleStatement.builder(
                    "DELETE FROM leader WHERE leader_id = ? IF reaper_instance_id = ?")
                .setConsistencyLevel(ConsistencyLevel.QUORUM)
                .build());

    getRunningRepairsPrepStmt =
        session.prepare(
            SimpleStatement.builder(
                    "select repair_id, node, reaper_instance_host, reaper_instance_id, segment_id"
                        + " FROM running_repairs"
                        + " WHERE repair_id = ?")
                .setConsistencyLevel(ConsistencyLevel.QUORUM)
                .build());

    setRunningRepairsPrepStmt =
        session.prepare(
            SimpleStatement.builder(
                    "UPDATE running_repairs USING TTL ?"
                        + " SET reaper_instance_host = ?, reaper_instance_id = ?, segment_id = ?"
                        + " WHERE repair_id = ? AND node = ? IF reaper_instance_id = ?")
                .setSerialConsistencyLevel(ConsistencyLevel.SERIAL)
                .setConsistencyLevel(ConsistencyLevel.QUORUM)
                .setIdempotence(false)
                .build());
    getRunningReapersCountPrepStmt = session.prepare(SELECT_RUNNING_REAPERS);
  }

  public boolean takeLead(UUID leaderId) {
    return takeLead(leaderId, LEAD_DURATION);
  }

  public boolean takeLead(UUID leaderId, int ttl) {
    LOG.debug("Trying to take lead on segment {}", leaderId);
    ResultSet lwtResult =
        session.execute(
            takeLeadPrepStmt.bind(
                leaderId, reaperInstanceId, AppContext.REAPER_INSTANCE_ADDRESS, ttl));

    if (lwtResult.wasApplied()) {
      LOG.debug("Took lead on segment {}", leaderId);
      return true;
    }

    // Another instance took the lead on the segment
    LOG.debug("Could not take lead on segment {}", leaderId);
    return false;
  }

  public boolean renewLead(UUID leaderId) {
    return renewLead(leaderId, LEAD_DURATION);
  }

  public boolean renewLead(UUID leaderId, int ttl) {
    ResultSet lwtResult =
        session.execute(
            renewLeadPrepStmt.bind(
                ttl,
                reaperInstanceId,
                AppContext.REAPER_INSTANCE_ADDRESS,
                leaderId,
                reaperInstanceId));

    if (lwtResult.wasApplied()) {
      LOG.debug("Renewed lead on segment {}", leaderId);
      return true;
    }
    assert false : "Could not renew lead on segment " + leaderId;
    LOG.error("Failed to renew lead on segment {}", leaderId);
    return false;
  }

  public List<UUID> getLeaders() {
    return session.execute(SimpleStatement.newInstance(SELECT_LEADERS)).all().stream()
        .map(leader -> leader.getUuid("leader_id"))
        .collect(Collectors.toList());
  }

  public void releaseLead(UUID leaderId) {
    Preconditions.checkNotNull(leaderId);
    ResultSet lwtResult = session.execute(releaseLeadPrepStmt.bind(leaderId, reaperInstanceId));
    LOG.info("Trying to release lead on segment {} for instance {}", leaderId, reaperInstanceId);
    if (lwtResult.wasApplied()) {
      LOG.info("Released lead on segment {}", leaderId);
    } else {
      assert false : "Could not release lead on segment " + leaderId;
      LOG.error("Could not release lead on segment {}", leaderId);
    }
  }

  public boolean hasLeadOnSegment(RepairSegment segment) {
    return renewRunningRepairsForNodes(
        segment.getRunId(), segment.getId(), segment.getReplicas().keySet());
  }

  public boolean hasLeadOnSegment(UUID leaderId) {
    ResultSet lwtResult =
        session.execute(
            renewLeadPrepStmt.bind(
                LEAD_DURATION,
                reaperInstanceId,
                AppContext.REAPER_INSTANCE_ADDRESS,
                leaderId,
                reaperInstanceId));

    return lwtResult.wasApplied();
  }

  public int countRunningReapers() {
    int runningReapers = getRunningReapers().size();
    LOG.debug("Running reapers = {}", runningReapers);
    return runningReapers > 0 ? runningReapers : 1;
  }

  public List<UUID> getRunningReapers() {
    ResultSet result = session.execute(getRunningReapersCountPrepStmt.bind());
    return result.all().stream()
        .map(row -> row.getUuid("reaper_instance_id"))
        .collect(Collectors.toList());
  }

  public boolean lockRunningRepairsForNodes(UUID repairId, UUID segmentId, Set<String> replicas) {

    // Attempt to lock all the nodes involved in the segment
    BatchStatementBuilder batch = BatchStatement.builder(BatchType.LOGGED);
    for (String replica : replicas) {
      batch.addStatement(
          setRunningRepairsPrepStmt.bind(
              LEAD_DURATION,
              AppContext.REAPER_INSTANCE_ADDRESS,
              reaperInstanceId,
              segmentId,
              repairId,
              replica,
              null));
    }

    ResultSet results = session.execute(batch.build());
    if (!results.wasApplied()) {
      logFailedLead(results, repairId, segmentId);
    }

    return results.wasApplied();
  }

  public boolean renewRunningRepairsForNodes(UUID repairId, UUID segmentId, Set<String> replicas) {
    // Attempt to renew lock on all the nodes involved in the segment
    BatchStatementBuilder batch = BatchStatement.builder(BatchType.LOGGED);
    for (String replica : replicas) {
      batch.addStatement(
          setRunningRepairsPrepStmt.bind(
              LEAD_DURATION,
              AppContext.REAPER_INSTANCE_ADDRESS,
              reaperInstanceId,
              segmentId,
              repairId,
              replica,
              reaperInstanceId));
    }

    ResultSet results = session.execute(batch.build());
    if (!results.wasApplied()) {
      logFailedLead(results, repairId, segmentId);
    }

    return results.wasApplied();
  }

  void logFailedLead(ResultSet results, UUID repairId, UUID segmentId) {
    LOG.debug(
        "Failed taking/renewing lock for repair {} and segment {} "
            + "because segments are already running for some nodes.",
        repairId,
        segmentId);
    for (Row row : results) {
      LOG.debug(
          "node {} is locked by {}/{} for segment {}",
          row.getColumnDefinitions().contains("node") ? row.getString("node") : "unknown",
          row.getColumnDefinitions().contains("reaper_instance_host")
              ? row.getString("reaper_instance_host")
              : "unknown",
          row.getColumnDefinitions().contains("reaper_instance_id")
              ? row.getUuid("reaper_instance_id")
              : "unknown",
          row.getColumnDefinitions().contains("segment_id")
              ? row.getUuid("segment_id")
              : "unknown");
    }
  }

  public boolean releaseRunningRepairsForNodes(
      UUID repairId, UUID segmentId, Set<String> replicas) {
    // Attempt to release all the nodes involved in the segment
    BatchStatementBuilder batch = BatchStatement.builder(BatchType.LOGGED);
    for (String replica : replicas) {
      batch.addStatement(
          // reaperInstanceId, AppContext.REAPER_INSTANCE_ADDRESS
          setRunningRepairsPrepStmt.bind(
              LEAD_DURATION, null, null, null, repairId, replica, reaperInstanceId));
    }

    ResultSet results = session.execute(batch.build());
    if (!results.wasApplied()) {
      logFailedLead(results, repairId, segmentId);
    }

    return results.wasApplied();
  }

  public Set<UUID> getLockedSegmentsForRun(UUID runId) {
    ResultSet results = session.execute(getRunningRepairsPrepStmt.bind(runId));

    Set<UUID> lockedSegments =
        results.all().stream()
            .filter(row -> row.getUuid("reaper_instance_id") != null)
            .map(row -> row.getUuid("segment_id"))
            .collect(Collectors.toSet());
    return lockedSegments;
  }

  public Set<String> getLockedNodesForRun(UUID runId) {
    ResultSet results = session.execute(getRunningRepairsPrepStmt.bind(runId));

    Set<String> lockedNodes =
        results.all().stream()
            .filter(row -> row.getUuid("reaper_instance_id") != null)
            .map(row -> row.getString("node"))
            .collect(Collectors.toSet());
    return lockedNodes;
  }
}
