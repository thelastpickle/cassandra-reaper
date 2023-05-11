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
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.DiagEventSubscription;
import io.cassandrareaper.core.GenericMetric;
import io.cassandrareaper.core.PercentRepairedMetric;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairRun.Builder;
import io.cassandrareaper.core.RepairRun.RunState;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairSegment.State;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.core.Snapshot;
import io.cassandrareaper.resources.view.RepairRunStatus;
import io.cassandrareaper.resources.view.RepairScheduleStatus;
import io.cassandrareaper.service.RingRange;
import io.cassandrareaper.storage.IDistributedStorage;
import io.cassandrareaper.storage.IStorage;
import io.cassandrareaper.storage.OpType;
import io.cassandrareaper.storage.cassandra.codecs.DateTimeCodec;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryLogger;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.VersionNumber;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.Duration;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import systems.composable.dropwizard.cassandra.CassandraFactory;
import systems.composable.dropwizard.cassandra.pooling.PoolingOptionsFactory;
import systems.composable.dropwizard.cassandra.retry.RetryPolicyFactory;


public final class CassandraStorage implements IStorage, IDistributedStorage {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraStorage.class);
  private static final AtomicBoolean UNINITIALISED = new AtomicBoolean(true);
  public final RepairSegmentDao repairSegmentDao;
  public final int defaultTimeout;
  final VersionNumber version;
  final UUID reaperInstanceId;
  private final com.datastax.driver.core.Cluster cassandra;
  private final Session session;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final RepairRunDao repairRunDao;
  private PreparedStatement saveHeartbeatPrepStmt;
  private PreparedStatement deleteHeartbeatPrepStmt;

  private final RepairUnitDao repairUnitDao;
  private final RepairScheduleDao repairScheduleDao;
  private final ClusterDao clusterDao;
  private final EventsDao eventsDao;
  private final MetricsDao metricsDao;
  private final Concurrency concurrency;
  private final SnapshotDao snapshotDao;
  private final OperationsDao operationsDao;

  public CassandraStorage(
      UUID reaperInstanceId,
      ReaperApplicationConfiguration config,
      Environment environment,
      CassandraMode mode) throws ReaperException {

    this.reaperInstanceId = reaperInstanceId;
    this.defaultTimeout = config.getHangingRepairTimeoutMins();
    CassandraFactory cassandraFactory = config.getCassandraFactory();
    overrideQueryOptions(cassandraFactory, mode);
    overrideRetryPolicy(cassandraFactory);
    overridePoolingOptions(cassandraFactory);

    // https://docs.datastax.com/en/developer/java-driver/3.5/manual/metrics/#metrics-4-compatibility
    cassandraFactory.setJmxEnabled(false);
    if (!CassandraStorage.UNINITIALISED.compareAndSet(true, false)) {
      // If there's been a past connection attempt, metrics are already registered
      cassandraFactory.setMetricsEnabled(false);
    }

    cassandra = cassandraFactory.build(environment);
    if (config.getActivateQueryLogger()) {
      cassandra.register(QueryLogger.builder().build());
    }
    CodecRegistry codecRegistry = cassandra.getConfiguration().getCodecRegistry();
    codecRegistry.register(new DateTimeCodec());
    session = cassandra.connect(config.getCassandraFactory().getKeyspace());
    version = cassandra.getMetadata().getAllHosts()
        .stream()
        .map(h -> h.getCassandraVersion())
        .min(VersionNumber::compareTo)
        .get();

    boolean skipMigration = System.getenv().containsKey("REAPER_SKIP_SCHEMA_MIGRATION")
        ? Boolean.parseBoolean(System.getenv("REAPER_SKIP_SCHEMA_MIGRATION"))
        : Boolean.FALSE;

    if (skipMigration) {
      LOG.info("Skipping schema migration as requested.");
    } else {
      MigrationManager.initializeAndUpgradeSchema(cassandra, session, config, version, mode);
    }

    this.eventsDao =  new EventsDao(session);
    this.metricsDao  = new MetricsDao(session);
    this.snapshotDao = new SnapshotDao(session);
    this.operationsDao  = new OperationsDao(session);
    this.concurrency = new Concurrency(version, reaperInstanceId, session);
    this.repairUnitDao  = new RepairUnitDao(defaultTimeout, session);
    this.repairSegmentDao = new RepairSegmentDao(concurrency, repairUnitDao, session);
    this.repairScheduleDao = new RepairScheduleDao(repairUnitDao, session);
    this.clusterDao  = new ClusterDao(repairScheduleDao, repairUnitDao, eventsDao, session, objectMapper);
    this.repairRunDao =  new RepairRunDao(repairUnitDao, clusterDao, repairSegmentDao, session);
    prepareStatements();
  }

  private static void overrideQueryOptions(CassandraFactory cassandraFactory, CassandraMode mode) {
    // all INSERT and DELETE stmt prepared in this class are idempotent
    ConsistencyLevel requiredCl = mode.equals(CassandraMode.ASTRA)
        ? ConsistencyLevel.LOCAL_QUORUM
        : ConsistencyLevel.LOCAL_ONE;
    if (cassandraFactory.getQueryOptions().isPresent()
        && ConsistencyLevel.LOCAL_ONE != cassandraFactory.getQueryOptions().get().getConsistencyLevel()) {
      LOG.warn("Customization of cassandra's queryOptions is not supported and will be overridden");
    }
    cassandraFactory.setQueryOptions(java.util.Optional.of(
        new QueryOptions()
          .setConsistencyLevel(requiredCl)
          .setDefaultIdempotence(true)));
  }

  private static void overrideRetryPolicy(CassandraFactory cassandraFactory) {
    if (cassandraFactory.getRetryPolicy().isPresent()) {
      LOG.warn("Customization of cassandra's retry policy is not supported and will be overridden");
    }
    cassandraFactory.setRetryPolicy(java.util.Optional.of((RetryPolicyFactory) () -> new RetryPolicyImpl()));
  }

  private static void overridePoolingOptions(CassandraFactory cassandraFactory) {
    PoolingOptionsFactory newPoolingOptionsFactory = new PoolingOptionsFactory() {
      @Override
      public PoolingOptions build() {
        if (null == getPoolTimeout()) {
          setPoolTimeout(Duration.minutes(2));
        }
        return super.build().setMaxQueueSize(40960);
      }
    };
    cassandraFactory.getPoolingOptions().ifPresent((originalPoolingOptions) -> {
      newPoolingOptionsFactory.setHeartbeatInterval(originalPoolingOptions.getHeartbeatInterval());
      newPoolingOptionsFactory.setIdleTimeout(originalPoolingOptions.getIdleTimeout());
      newPoolingOptionsFactory.setLocal(originalPoolingOptions.getLocal());
      newPoolingOptionsFactory.setRemote(originalPoolingOptions.getRemote());
      newPoolingOptionsFactory.setPoolTimeout(originalPoolingOptions.getPoolTimeout());
    });
    cassandraFactory.setPoolingOptions(java.util.Optional.of(newPoolingOptionsFactory));
  }

  private static boolean withinRange(RepairSegment segment, Optional<RingRange> range) {
    return !range.isPresent() || RepairSegmentDao.segmentIsWithinRange(segment, range.get());
  }

  private void prepareStatements() {
    saveHeartbeatPrepStmt = session
        .prepare(
            "INSERT INTO running_reapers(reaper_instance_id, reaper_instance_host, last_heartbeat)"
                + " VALUES(?,?,toTimestamp(now()))")
        .setIdempotent(false);
    deleteHeartbeatPrepStmt = session
        .prepare(
            "DELETE FROM running_reapers WHERE reaper_instance_id = ?")
        .setIdempotent(true);

  }

  @Override
  public boolean isStorageConnected() {
    return session != null && !session.isClosed();
  }

  @Override
  public Collection<Cluster> getClusters() {
    // cache the clusters list for ten seconds
    return clusterDao.getClusters();
  }

  @Override
  public boolean addCluster(Cluster cluster) {
    return clusterDao.addCluster(cluster);
  }

  @Override
  public boolean updateCluster(Cluster newCluster) {
    return clusterDao.updateCluster(newCluster);
  }

  private boolean addClusterAssertions(Cluster cluster) {

    // assert we're not overwriting a cluster with the same name but different node list

    return clusterDao.addClusterAssertions(cluster);
  }

  @Override
  public Cluster getCluster(String clusterName) {
    return clusterDao.getCluster(clusterName);
  }

  private Cluster parseCluster(Row row) throws IOException {

    return clusterDao.parseCluster(row);
  }

  @Override
  public Cluster deleteCluster(String clusterName) {

    return clusterDao.deleteCluster(clusterName);
  }

  @Override
  public RepairRun addRepairRun(Builder repairRun, Collection<RepairSegment.Builder> newSegments) {

    return repairRunDao.addRepairRun(repairRun, newSegments, objectMapper);
  }

  @Override
  public boolean updateRepairRun(RepairRun repairRun) {
    return repairRunDao.updateRepairRun(repairRun);
  }

  @Override
  public boolean updateRepairRun(RepairRun repairRun, Optional<Boolean> updateRepairState) {

    return repairRunDao.updateRepairRun(repairRun, updateRepairState);
  }

  @Override
  public Optional<RepairRun> getRepairRun(UUID id) {
    return repairRunDao.getRepairRun(id);
  }

  @Override
  public Collection<RepairRun> getRepairRunsForCluster(String clusterName, Optional<Integer> limit) {

    // Grab all ids for the given cluster name
    // Grab repair runs asynchronously for all the ids returned by the index table

    return repairRunDao.getRepairRunsForCluster(clusterName, limit);
  }

  @Override
  public List<RepairRun> getRepairRunsForClusterPrioritiseRunning(String clusterName, Optional<Integer> limit) {
    // We've set up the RunState enum so that values are declared in order of "interestingness",
    // we iterate over the table via the secondary index according to that ordering.

    // Flatten the UUIDs from each status down into a single array.
    // Merge the two lists and trim.

    // Run an async query on each UUID in the flattened list, against the main repair_run table with
    // all columns required as an input to `buildRepairRunFromRow`.
    // Defuture the repair_run rows and build the strongly typed RepairRun objects from the contents.
    return repairRunDao.getRepairRunsForClusterPrioritiseRunning(clusterName, limit);
  }

  @Override
  public int getSegmentAmountForRepairRun(UUID runId) {
    return repairSegmentDao.getSegmentAmountForRepairRun(runId);
  }

  @Override
  public int getSegmentAmountForRepairRunWithState(UUID runId, RepairSegment.State state) {
    return repairSegmentDao.getSegmentAmountForRepairRunWithState(runId, state);
  }

  @Override
  public Collection<RepairRun> getRepairRunsForUnit(UUID repairUnitId) {

    // Grab all ids for the given cluster name

    // Grab repair runs asynchronously for all the ids returned by the index table

    return repairRunDao.getRepairRunsForUnit(repairUnitId);
  }

  /**
   * Create a collection of RepairRun objects out of a list of ResultSetFuture. Used to handle async queries on the
   * repair_run table with a list of ids.
   */
  private Collection<RepairRun> getRepairRunsAsync(List<ResultSetFuture> repairRunFutures) {

    return repairRunDao.getRepairRunsAsync(repairRunFutures);
  }

  @Override
  public Collection<RepairRun> getRepairRunsWithState(RunState runState) {

    return repairRunDao.getRepairRunsWithState(runState);
  }

  private Collection<? extends RepairRun> getRepairRunsWithStateForCluster(
      Collection<UUID> clusterRepairRunsId,
      RunState runState) {

    return repairRunDao.getRepairRunsWithStateForCluster(clusterRepairRunsId, runState);
  }

  @Override
  public Optional<RepairRun> deleteRepairRun(UUID id) {
    return repairRunDao.deleteRepairRun(id);
  }

  @Override
  public RepairUnit addRepairUnit(RepairUnit.Builder newRepairUnit) {

    return repairUnitDao.addRepairUnit(newRepairUnit);
  }

  @Override
  public void updateRepairUnit(RepairUnit updatedRepairUnit) {
    repairUnitDao.updateRepairUnit(updatedRepairUnit);
  }

  private RepairUnit getRepairUnitImpl(UUID id) {
    return repairUnitDao.getRepairUnitImpl(id);
  }

  @Override
  public RepairUnit getRepairUnit(UUID id) {
    return repairUnitDao.getRepairUnit(id);
  }

  @Override
  public Optional<RepairUnit> getRepairUnit(RepairUnit.Builder params) {
    // brute force again

    return repairUnitDao.getRepairUnit(params);
  }

  @Override
  public boolean updateRepairSegment(RepairSegment segment) {

    return repairSegmentDao.updateRepairSegment(segment);
  }

  @Override
  public boolean updateRepairSegmentUnsafe(RepairSegment segment) {

    return repairSegmentDao.updateRepairSegmentUnsafe(segment);
  }

  @Override
  public Optional<RepairSegment> getRepairSegment(UUID runId, UUID segmentId) {

    return repairSegmentDao.getRepairSegment(runId, segmentId);
  }

  @Override
  public Collection<RepairSegment> getRepairSegmentsForRun(UUID runId) {
    // First gather segments ids

    return repairSegmentDao.getRepairSegmentsForRun(runId);
  }

  @Override
  public List<RepairSegment> getNextFreeSegments(UUID runId) {

    return repairSegmentDao.getNextFreeSegments(runId);
  }

  @Override
  public List<RepairSegment> getNextFreeSegmentsForRanges(
      UUID runId,
      List<RingRange> ranges) {

    return repairSegmentDao.getNextFreeSegmentsForRanges(runId, ranges);
  }

  private boolean segmentIsWithinRanges(RepairSegment seg, List<RingRange> ranges) {

    return repairSegmentDao.segmentIsWithinRanges(seg, ranges);
  }

  private boolean segmentIsCandidate(RepairSegment seg, Set<String> lockedNodes) {
    return repairSegmentDao.segmentIsCandidate(seg, lockedNodes);
  }

  @Override
  public Collection<RepairSegment> getSegmentsWithState(UUID runId, State segmentState) {

    return repairSegmentDao.getSegmentsWithState(runId, segmentState);
  }

  @Override
  public SortedSet<UUID> getRepairRunIdsForCluster(String clusterName, Optional<Integer> limit) {

    return repairRunDao.getRepairRunIdsForCluster(clusterName, limit);
  }

  private SortedSet<UUID> getRepairRunIdsForClusterWithState(String clusterName, RunState runState) {


    return repairRunDao.getRepairRunIdsForClusterWithState(clusterName, runState);
  }

  @Override
  public RepairSchedule addRepairSchedule(io.cassandrareaper.core.RepairSchedule.Builder repairSchedule) {

    return repairScheduleDao.addRepairSchedule(repairSchedule);
  }

  @Override
  public Optional<RepairSchedule> getRepairSchedule(UUID repairScheduleId) {

    return repairScheduleDao.getRepairSchedule(repairScheduleId);
  }

  private RepairSchedule createRepairScheduleFromRow(Row repairScheduleRow) {
    return repairScheduleDao.createRepairScheduleFromRow(repairScheduleRow);
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForCluster(String clusterName) {

    return repairScheduleDao.getRepairSchedulesForCluster(clusterName);
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForCluster(String clusterName, boolean incremental) {
    return repairScheduleDao.getRepairSchedulesForCluster(clusterName, incremental);
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForKeyspace(String keyspaceName) {

    return repairScheduleDao.getRepairSchedulesForKeyspace(keyspaceName);
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForClusterAndKeyspace(String clusterName, String keyspaceName) {

    return repairScheduleDao.getRepairSchedulesForClusterAndKeyspace(clusterName, keyspaceName);
  }

  @Override
  public Collection<RepairSchedule> getAllRepairSchedules() {

    return repairScheduleDao.getAllRepairSchedules();
  }

  @Override
  public boolean updateRepairSchedule(RepairSchedule newRepairSchedule) {

    return repairScheduleDao.updateRepairSchedule(newRepairSchedule);
  }

  @Override
  public Optional<RepairSchedule> deleteRepairSchedule(UUID id) {

    return repairScheduleDao.deleteRepairSchedule(id);
  }

  @Override
  public Collection<RepairRunStatus> getClusterRunStatuses(String clusterName, int limit) {
    Collection<RepairRunStatus> repairRunStatuses = Lists.<RepairRunStatus>newArrayList();
    Collection<RepairRun> repairRuns = repairRunDao.getRepairRunsForCluster(clusterName, Optional.of(limit));
    for (RepairRun repairRun : repairRuns) {
      Collection<RepairSegment> segments = repairSegmentDao.getRepairSegmentsForRun(repairRun.getId());
      RepairUnit repairUnit = repairUnitDao.getRepairUnit(repairRun.getRepairUnitId());

      int segmentsRepaired
          = (int) segments.stream().filter(seg -> seg.getState().equals(RepairSegment.State.DONE)).count();

      repairRunStatuses.add(new RepairRunStatus(repairRun, repairUnit, segmentsRepaired));
    }

    return repairRunStatuses;
  }

  @Override
  public Collection<RepairScheduleStatus> getClusterScheduleStatuses(String clusterName) {
    Collection<RepairSchedule> repairSchedules = repairScheduleDao.getRepairSchedulesForCluster(clusterName);

    Collection<RepairScheduleStatus> repairScheduleStatuses = repairSchedules
        .stream()
        .map(sched -> new RepairScheduleStatus(sched, repairUnitDao.getRepairUnit(sched.getRepairUnitId())))
        .collect(Collectors.toList());

    return repairScheduleStatuses;
  }

  private RepairRun buildRepairRunFromRow(Row repairRunResult, UUID id) {
    return repairRunDao.buildRepairRunFromRow(repairRunResult, id);
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
    session.executeAsync(
        saveHeartbeatPrepStmt.bind(reaperInstanceId, AppContext.REAPER_INSTANCE_ADDRESS));
  }

  @Override
  public Collection<DiagEventSubscription> getEventSubscriptions() {
    return eventsDao.getEventSubscriptions();
  }

  @Override
  public Collection<DiagEventSubscription> getEventSubscriptions(String clusterName) {

    return eventsDao.getEventSubscriptions(clusterName);
  }

  @Override
  public DiagEventSubscription getEventSubscription(UUID id) {
    return eventsDao.getEventSubscription(id);
  }

  @Override
  public DiagEventSubscription addEventSubscription(DiagEventSubscription subscription) {

    return eventsDao.addEventSubscription(subscription);
  }

  @Override
  public boolean deleteEventSubscription(UUID id) {
    return eventsDao.deleteEventSubscription(id);
  }

  @Override
  public boolean saveSnapshot(Snapshot snapshot) {

    return snapshotDao.saveSnapshot(snapshot);
  }

  @Override
  public boolean deleteSnapshot(Snapshot snapshot) {
    return snapshotDao.deleteSnapshot(snapshot);
  }

  @Override
  public Snapshot getSnapshot(String clusterName, String snapshotName) {

    return snapshotDao.getSnapshot(clusterName, snapshotName);
  }

  @Override
  public List<GenericMetric> getMetrics(
      String clusterName,
      Optional<String> host,
      String metricDomain,
      String metricType,
      long since) {

    // Compute the hourly buckets since the requested lower bound timestamp


    return metricsDao.getMetrics(clusterName, host, metricDomain, metricType, since);
  }

  @Override
  public void storeMetrics(List<GenericMetric> metrics) {

    metricsDao.storeMetrics(metrics);
  }

  /**
   * Truncates a metric date time to the closest partition based on the definesd partition sizes
   * @param metricTime the time of the metric
   * @return the time truncated to the closest partition
   */
  private DateTime computeMetricsPartition(DateTime metricTime) {
    return metricsDao.computeMetricsPartition(metricTime);
  }

  @Override
  public void purgeMetrics() {
    metricsDao.purgeMetrics();
  }

  @Override
  public void storeOperations(String clusterName, OpType operationType, String host, String operationsJson) {
    operationsDao.storeOperations(clusterName, operationType, host, operationsJson);
  }

  @Override
  public String listOperations(String clusterName, OpType operationType, String host) {

    return operationsDao.listOperations(clusterName, operationType, host);
  }

  @Override
  public void purgeNodeOperations() {
    operationsDao.purgeNodeOperations();
  }

  @Override
  public boolean lockRunningRepairsForNodes(
      UUID repairId,
      UUID segmentId,
      Set<String> replicas) {

    // Attempt to lock all the nodes involved in the segment

    return concurrency.lockRunningRepairsForNodes(repairId, segmentId, replicas);
  }

  @Override
  public boolean renewRunningRepairsForNodes(
      UUID repairId,
      UUID segmentId,
      Set<String> replicas) {
    // Attempt to renew lock on all the nodes involved in the segment

    return concurrency.renewRunningRepairsForNodes(repairId, segmentId, replicas);
  }

  private void logFailedLead(ResultSet results, UUID repairId, UUID segmentId) {
    concurrency.logFailedLead(results, repairId, segmentId);
  }

  @Override
  public boolean releaseRunningRepairsForNodes(
      UUID repairId,
      UUID segmentId,
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
  public List<PercentRepairedMetric> getPercentRepairedMetrics(String clusterName, UUID repairScheduleId, Long since) {
    // Compute the ten minutes buckets since the requested lower bound timestamp
    return metricsDao.getPercentRepairedMetrics(clusterName, repairScheduleId, since);
  }

  @Override
  public void storePercentRepairedMetric(PercentRepairedMetric metric) {
    metricsDao.storePercentRepairedMetric(metric);
  }

  @Override
  public void start() {
    // no-op
  }

  @Override
  public void stop() {
    // Statements executed when the server shuts down.
    LOG.info("Reaper is stopping, removing this instance from running reapers...");
    session.execute(deleteHeartbeatPrepStmt.bind(reaperInstanceId));
  }

  public enum CassandraMode {
    CASSANDRA,
    ASTRA
  }

  /**
   * Retry all statements.
   *
   * <p>
   * All reaper statements are idempotent. Reaper generates few read and writes requests, so it's ok to keep
   * retrying.
   *
   * <p>
   * Sleep 100 milliseconds in between subsequent read retries. Fail after the tenth read retry.
   *
   * <p>
   * Writes keep retrying forever.
   */
  private static class RetryPolicyImpl implements RetryPolicy {

    @Override
    public RetryDecision onReadTimeout(
        Statement stmt,
        ConsistencyLevel cl,
        int required,
        int received,
        boolean retrieved,
        int retry) {

      if (retry > 1) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException expected) { }
      }
      return null != stmt && !Objects.equals(Boolean.FALSE, stmt.isIdempotent())
          ? retry < 10 ? RetryDecision.retry(cl) : RetryDecision.rethrow()
          : DefaultRetryPolicy.INSTANCE.onReadTimeout(stmt, cl, required, received, retrieved, retry);
    }

    @Override
    public RetryDecision onWriteTimeout(
        Statement stmt,
        ConsistencyLevel cl,
        WriteType type,
        int required,
        int received,
        int retry) {

      Preconditions.checkState(WriteType.CAS != type ||  ConsistencyLevel.SERIAL == cl);

      return null != stmt && !Objects.equals(Boolean.FALSE, stmt.isIdempotent())
          ? RetryDecision.retry(cl)
          : DefaultRetryPolicy.INSTANCE.onWriteTimeout(stmt, cl, type, required, received, retry);
    }

    @Override
    public RetryDecision onUnavailable(Statement stmt, ConsistencyLevel cl, int required, int aliveReplica, int retry) {
      return DefaultRetryPolicy.INSTANCE.onUnavailable(stmt, cl, required, aliveReplica, retry == 1 ? 0 : retry);
    }

    @Override
    public RetryDecision onRequestError(Statement stmt, ConsistencyLevel cl, DriverException ex, int nbRetry) {
      return DefaultRetryPolicy.INSTANCE.onRequestError(stmt, cl, ex, nbRetry);
    }

    @Override
    public void init(com.datastax.driver.core.Cluster cluster) {
    }

    @Override
    public void close() {
    }
  }
}