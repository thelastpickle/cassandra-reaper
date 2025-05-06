/*
 * Copyright 2016-2017 Spotify AB Copyright 2016-2019 The Last Pickle Ltd Copyright 2020-2020
 * DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cassandrareaper.storage.cassandra;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.GenericMetric;
import io.cassandrareaper.core.PercentRepairedMetric;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.service.RingRange;
import io.cassandrareaper.storage.IDistributedStorage;
import io.cassandrareaper.storage.IStorageDao;
import io.cassandrareaper.storage.cluster.CassandraClusterDao;
import io.cassandrareaper.storage.cluster.IClusterDao;
import io.cassandrareaper.storage.events.CassandraEventsDao;
import io.cassandrareaper.storage.events.IEventsDao;
import io.cassandrareaper.storage.metrics.CassandraMetricsDao;
import io.cassandrareaper.storage.operations.CassandraOperationsDao;
import io.cassandrareaper.storage.operations.IOperationsDao;
import io.cassandrareaper.storage.repairrun.CassandraRepairRunDao;
import io.cassandrareaper.storage.repairrun.IRepairRunDao;
import io.cassandrareaper.storage.repairschedule.CassandraRepairScheduleDao;
import io.cassandrareaper.storage.repairschedule.IRepairScheduleDao;
import io.cassandrareaper.storage.repairsegment.CassandraRepairSegmentDao;
import io.cassandrareaper.storage.repairsegment.IRepairSegmentDao;
import io.cassandrareaper.storage.repairunit.CassandraRepairUnitDao;
import io.cassandrareaper.storage.repairunit.IRepairUnitDao;
import io.cassandrareaper.storage.snapshot.CassandraSnapshotDao;
import io.cassandrareaper.storage.snapshot.ISnapshotDao;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import brave.Tracing;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.dropwizard.cassandra.CassandraFactory;
import io.dropwizard.cassandra.request.RequestOptionsFactory;
import io.dropwizard.core.setup.Environment;
import org.jetbrains.annotations.NotNull;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class CassandraStorageFacade implements IStorageDao, IDistributedStorage {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraStorageFacade.class);
  private static final AtomicBoolean UNINITIALISED = new AtomicBoolean(true);
  public final CassandraRepairSegmentDao cassRepairSegmentDao;
  public final int defaultTimeout;
  final Version version;
  final UUID reaperInstanceId;
  private final CqlSession cassandra;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final CassandraRepairRunDao cassRepairRunDao;
  private final CassandraRepairUnitDao cassRepairUnitDao;
  private final CassandraRepairScheduleDao cassRepairScheduleDao;
  private final CassandraClusterDao cassClusterDao;
  private final CassandraEventsDao cassEventsDao;
  private final CassandraMetricsDao cassMetricsDao;
  private final CassandraConcurrencyDao concurrency;
  private final CassandraSnapshotDao cassSnapshotDao;
  private final CassandraOperationsDao operationsDao;
  private PreparedStatement saveHeartbeatPrepStmt;
  private PreparedStatement deleteHeartbeatPrepStmt;

  public CassandraStorageFacade(UUID reaperInstanceId, ReaperApplicationConfiguration config,
      Environment environment, CassandraMode mode) throws ReaperException {

    this.reaperInstanceId = reaperInstanceId;
    this.defaultTimeout = config.getHangingRepairTimeoutMins();
    CassandraFactory cassandraFactory = config.getCassandraFactory();
    overrideQueryOptions(cassandraFactory, mode);
    overrideRetryPolicy(cassandraFactory);

    // https://docs.datastax.com/en/developer/java-driver/3.5/manual/metrics/#metrics-4-compatibility
    // cassandraFactory.setJmxEnabled(false);
    if (!CassandraStorageFacade.UNINITIALISED.compareAndSet(true, false)) {
      // If there's been a past connection attempt, metrics are already registered
      cassandraFactory.setMetricsEnabled(false);
    }

    cassandraFactory.setSessionName("main-" + Uuids.random());
    cassandra = cassandraFactory.build(environment.metrics(), environment.lifecycle(),
        environment.healthChecks(), Tracing.newBuilder().build());

    version = cassandra.getMetadata().getNodes().entrySet().stream()
        .map(h -> h.getValue().getCassandraVersion()).min(Version::compareTo).get();

    boolean skipMigration = System.getenv().containsKey("REAPER_SKIP_SCHEMA_MIGRATION")
        ? Boolean.parseBoolean(System.getenv("REAPER_SKIP_SCHEMA_MIGRATION"))
        : Boolean.FALSE;

    if (skipMigration) {
      LOG.info("Skipping schema migration as requested.");
    } else {
      MigrationManager.initializeAndUpgradeSchema(cassandraFactory, environment, config, version,
          mode);
    }

    this.cassEventsDao = new CassandraEventsDao(cassandra);
    this.cassMetricsDao = new CassandraMetricsDao(cassandra);
    this.cassSnapshotDao = new CassandraSnapshotDao(cassandra);
    this.operationsDao = new CassandraOperationsDao(cassandra);
    this.concurrency = new CassandraConcurrencyDao(version, reaperInstanceId, cassandra);
    this.cassRepairUnitDao = new CassandraRepairUnitDao(defaultTimeout, cassandra);
    this.cassRepairSegmentDao =
        new CassandraRepairSegmentDao(concurrency, cassRepairUnitDao, cassandra);
    this.cassRepairScheduleDao = new CassandraRepairScheduleDao(cassRepairUnitDao, cassandra);
    this.cassClusterDao = new CassandraClusterDao(cassRepairScheduleDao, cassRepairUnitDao,
        cassEventsDao, cassandra, objectMapper);
    this.cassRepairRunDao = new CassandraRepairRunDao(cassRepairUnitDao, cassClusterDao,
        cassRepairSegmentDao, cassandra, objectMapper);
    prepareStatements();
  }

  private static void overrideQueryOptions(CassandraFactory cassandraFactory, CassandraMode mode) {
    RequestOptionsFactory requestOptionsFactory = new RequestOptionsFactory();
    requestOptionsFactory.setRequestConsistency(ConsistencyLevel.LOCAL_ONE.toString());
    requestOptionsFactory.setRequestDefaultIdempotence(Boolean.TRUE);
  }

  private static void overrideRetryPolicy(CassandraFactory cassandraFactory) {
    if (cassandraFactory.getRetryPolicy() != null) {
      LOG.warn("Customization of cassandra's retry policy is not supported and will be overridden");
    }
    cassandraFactory.setRetryPolicy(new ReaperRetryPolicyFactory());
  }

  private void prepareStatements() {
    saveHeartbeatPrepStmt = cassandra.prepare(SimpleStatement
        .builder("INSERT INTO running_reapers(reaper_instance_id,"
            + " reaper_instance_host, last_heartbeat)" + " VALUES(?,?,toTimestamp(now()))")
        .setIdempotence(false).build());
    deleteHeartbeatPrepStmt = cassandra
        .prepare(SimpleStatement.builder("DELETE FROM running_reapers WHERE reaper_instance_id = ?")
            .setIdempotence(true).build());

  }

  @Override
  public boolean isStorageConnected() {
    return cassandra != null && !cassandra.isClosed();
  }

  @Override
  public List<RepairSegment> getNextFreeSegmentsForRanges(UUID runId, List<RingRange> ranges) {

    return cassRepairSegmentDao.getNextFreeSegmentsForRanges(runId, ranges);
  }

  @Override
  public boolean takeLead(UUID leaderId) {
    return concurrency.takeLead(leaderId);
  }

  @Override
  public boolean takeLead(UUID leaderId, int ttl) {

    // Another instance took the lead on the segment
    return concurrency.takeLead(leaderId, ttl);
  }

  @Override
  public boolean renewLead(UUID leaderId) {
    return concurrency.renewLead(leaderId);
  }

  @Override
  public boolean renewLead(UUID leaderId, int ttl) {

    return concurrency.renewLead(leaderId, ttl);
  }

  @Override
  public List<UUID> getLeaders() {
    return concurrency.getLeaders();
  }

  @Override
  public void releaseLead(UUID leaderId) {
    concurrency.releaseLead(leaderId);
  }

  boolean hasLeadOnSegment(RepairSegment segment) {
    return concurrency.hasLeadOnSegment(segment);
  }

  boolean hasLeadOnSegment(UUID leaderId) {

    return concurrency.hasLeadOnSegment(leaderId);
  }

  @Override
  public int countRunningReapers() {
    return concurrency.countRunningReapers();
  }

  @Override
  public List<UUID> getRunningReapers() {
    return concurrency.getRunningReapers();
  }

  @Override
  public void saveHeartbeat() {
    cassandra.executeAsync(
        saveHeartbeatPrepStmt.bind(reaperInstanceId, AppContext.REAPER_INSTANCE_ADDRESS));
  }

  @Override
  public List<GenericMetric> getMetrics(String clusterName, Optional<String> host,
      String metricDomain, String metricType, long since) {
    return cassMetricsDao.getMetrics(clusterName, host, metricDomain, metricType, since);
  }

  @Override
  public void storeMetrics(List<GenericMetric> metrics) {

    cassMetricsDao.storeMetrics(metrics);
  }

  /**
   * Truncates a metric date time to the closest partition based on the definesd partition sizes
   *
   * @param metricTime the time of the metric
   * @return the time truncated to the closest partition
   */
  private DateTime computeMetricsPartition(DateTime metricTime) {
    return cassMetricsDao.computeMetricsPartition(metricTime);
  }

  @Override
  public void purgeMetrics() {
    cassMetricsDao.purgeMetrics();
  }

  @Override
  public boolean lockRunningRepairsForNodes(UUID repairId, UUID segmentId, Set<String> replicas) {

    // Attempt to lock all the nodes involved in the segment

    return concurrency.lockRunningRepairsForNodes(repairId, segmentId, replicas);
  }

  @Override
  public boolean renewRunningRepairsForNodes(UUID repairId, UUID segmentId, Set<String> replicas) {
    // Attempt to renew lock on all the nodes involved in the segment

    return concurrency.renewRunningRepairsForNodes(repairId, segmentId, replicas);
  }

  private void logFailedLead(ResultSet results, UUID repairId, UUID segmentId) {
    concurrency.logFailedLead(results, repairId, segmentId);
  }

  @Override
  public boolean releaseRunningRepairsForNodes(UUID repairId, UUID segmentId,
      Set<String> replicas) {
    // Attempt to release all the nodes involved in the segment

    return concurrency.releaseRunningRepairsForNodes(repairId, segmentId, replicas);
  }

  @Override
  public Set<UUID> getLockedSegmentsForRun(UUID runId) {

    return concurrency.getLockedSegmentsForRun(runId);
  }

  public Set<String> getLockedNodesForRun(UUID runId) {

    return concurrency.getLockedNodesForRun(runId);
  }

  @Override
  public List<PercentRepairedMetric> getPercentRepairedMetrics(String clusterName,
      UUID repairScheduleId, Long since) {

    return cassMetricsDao.getPercentRepairedMetrics(clusterName, repairScheduleId, since);
  }

  @Override
  public void storePercentRepairedMetric(PercentRepairedMetric metric) {
    cassMetricsDao.storePercentRepairedMetric(metric);
  }

  @Override
  public void start() {
    // no-op
  }

  @Override
  public void stop() {
    // Statements executed when the server shuts down.
    LOG.info("Reaper is stopping, removing this instance from running reapers...");
    cassandra.execute(deleteHeartbeatPrepStmt.bind(reaperInstanceId));
  }

  @Override
  public IEventsDao getEventsDao() {
    return this.cassEventsDao;
  }

  @Override
  public ISnapshotDao getSnapshotDao() {
    return this.cassSnapshotDao;
  }

  @Override
  public IRepairRunDao getRepairRunDao() {
    return this.cassRepairRunDao;
  }

  @Override
  public IRepairSegmentDao getRepairSegmentDao() {
    return this.cassRepairSegmentDao;
  }

  @Override
  public IRepairUnitDao getRepairUnitDao() {
    return this.cassRepairUnitDao;
  }

  @Override
  public IRepairScheduleDao getRepairScheduleDao() {
    return this.cassRepairScheduleDao;
  }

  @Override
  public IClusterDao getClusterDao() {
    return this.cassClusterDao;
  }

  @Override
  public IOperationsDao getOperationsDao() {
    return this.operationsDao;
  }

  public enum CassandraMode {
    CASSANDRA
  }
}
