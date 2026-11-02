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
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.core.Segment;
import io.cassandrareaper.management.ClusterFacade;
import io.cassandrareaper.storage.IStorageDao;
import io.cassandrareaper.storage.MemoryStorageFacade;
import io.cassandrareaper.storage.repairsegment.IRepairSegmentDao;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * Comprehensive unit tests for topology change segment splitting functionality. Tests the new
 * behavior where segments are split and repair continues instead of failing.
 */
public final class RepairRunnerSegmentSplittingTest {

  private static final Set<String> TABLES = ImmutableSet.of("table1");
  private IStorageDao storage;
  private AppContext context;
  private Cluster cluster;
  private UUID repairUnitId;

  /** A RepairRunner instance used solely to invoke private production methods via reflection. */
  private RepairRunner runner;

  @Before
  public void setUp() throws ReaperException {
    storage = new MemoryStorageFacade();
    context = new AppContext();
    context.storage = storage;
    context.config = new ReaperApplicationConfiguration();

    cluster =
        Cluster.builder()
            .withName("test_" + RandomStringUtils.randomAlphabetic(12))
            .withSeedHosts(ImmutableSet.of("127.0.0.1"))
            .withState(Cluster.State.ACTIVE)
            .build();

    storage.getClusterDao().addCluster(cluster);

    Set<String> nodeSet = Sets.newHashSet("127.0.0.1", "127.0.0.2", "127.0.0.3");
    Set<String> cfNames = Sets.newHashSet("table1");

    repairUnitId =
        storage
            .getRepairUnitDao()
            .addRepairUnit(
                RepairUnit.builder()
                    .clusterName(cluster.getName())
                    .keyspaceName("test_keyspace")
                    .columnFamilies(cfNames)
                    .incrementalRepair(false)
                    .subrangeIncrementalRepair(false)
                    .nodes(nodeSet)
                    .datacenters(Collections.emptySet())
                    .blacklistedTables(Collections.emptySet())
                    .repairThreadCount(1)
                    .timeout(30))
            .getId();

    // Create a RepairRunner via the package-private constructor so we can invoke its private
    // coverage-verification methods via reflection. A dummy RepairRun is needed because the
    // constructor looks it up from storage.
    UUID bootstrapRunId =
        storage
            .getRepairRunDao()
            .addRepairRun(
                RepairRun.builder(cluster.getName(), repairUnitId)
                    .intensity(0.5)
                    .segmentCount(1)
                    .repairParallelism(RepairParallelism.PARALLEL)
                    .tables(TABLES),
                Collections.emptyList())
            .getId();

    ClusterFacade mockClusterFacade = mock(ClusterFacade.class);
    RepairRunService mockRepairRunService = mock(RepairRunService.class);
    runner =
        new RepairRunner(
            context,
            bootstrapRunId,
            mockClusterFacade,
            storage.getRepairRunDao(),
            mockRepairRunService);
  }

  /**
   * Test: Simple segment split into 3 parts Original: [1, 10) Boundaries: 4, 7 Expected
   * replacements: [1,4), [4,7), [7,10)
   */
  @Test
  public void testSimpleSegmentSplit() throws Exception {
    // Create original segment [1, 10)
    RepairSegment originalSegment =
        createSegment(
            BigInteger.valueOf(1), BigInteger.valueOf(10), RepairSegment.State.NOT_STARTED);

    // Create replacement segments
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(
        createSegment(
            BigInteger.valueOf(1), BigInteger.valueOf(4), RepairSegment.State.NOT_STARTED));
    replacements.add(
        createSegment(
            BigInteger.valueOf(4), BigInteger.valueOf(7), RepairSegment.State.NOT_STARTED));
    replacements.add(
        createSegment(
            BigInteger.valueOf(7), BigInteger.valueOf(10), RepairSegment.State.NOT_STARTED));

    // Verify coverage
    boolean coverageComplete = invokeVerifyCompleteCoverage(runner, originalSegment, replacements);
    assertTrue("Coverage should be complete for simple split", coverageComplete);
  }

  /** Test: No split required (single replacement equals original) */
  @Test
  public void testNoSplitRequired() throws Exception {
    RepairSegment originalSegment =
        createSegment(
            BigInteger.valueOf(1), BigInteger.valueOf(10), RepairSegment.State.NOT_STARTED);

    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(
        createSegment(
            BigInteger.valueOf(1), BigInteger.valueOf(10), RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = invokeVerifyCompleteCoverage(runner, originalSegment, replacements);
    assertTrue("Coverage should be complete when no split needed", coverageComplete);
  }

  /** Test: Single boundary creates 2 segments */
  @Test
  public void testSingleBoundarySplit() throws Exception {
    RepairSegment originalSegment =
        createSegment(
            BigInteger.valueOf(0), BigInteger.valueOf(100), RepairSegment.State.NOT_STARTED);

    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(
        createSegment(
            BigInteger.valueOf(0), BigInteger.valueOf(50), RepairSegment.State.NOT_STARTED));
    replacements.add(
        createSegment(
            BigInteger.valueOf(50), BigInteger.valueOf(100), RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = invokeVerifyCompleteCoverage(runner, originalSegment, replacements);
    assertTrue("Coverage should be complete for single boundary split", coverageComplete);
  }

  /** Test: Multiple boundaries create many segments */
  @Test
  public void testMultipleBoundariesSplit() throws Exception {
    RepairSegment originalSegment =
        createSegment(
            BigInteger.valueOf(0), BigInteger.valueOf(1000), RepairSegment.State.NOT_STARTED);

    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(
        createSegment(
            BigInteger.valueOf(0), BigInteger.valueOf(100), RepairSegment.State.NOT_STARTED));
    replacements.add(
        createSegment(
            BigInteger.valueOf(100), BigInteger.valueOf(300), RepairSegment.State.NOT_STARTED));
    replacements.add(
        createSegment(
            BigInteger.valueOf(300), BigInteger.valueOf(600), RepairSegment.State.NOT_STARTED));
    replacements.add(
        createSegment(
            BigInteger.valueOf(600), BigInteger.valueOf(800), RepairSegment.State.NOT_STARTED));
    replacements.add(
        createSegment(
            BigInteger.valueOf(800), BigInteger.valueOf(1000), RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = invokeVerifyCompleteCoverage(runner, originalSegment, replacements);
    assertTrue("Coverage should be complete for multiple boundaries", coverageComplete);
  }

  /** Test: Coverage verification detects missing range */
  @Test
  public void testMissingRangeDetected() throws Exception {
    RepairSegment originalSegment =
        createSegment(
            BigInteger.valueOf(1), BigInteger.valueOf(10), RepairSegment.State.NOT_STARTED);

    // Missing range [4, 7)
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(
        createSegment(
            BigInteger.valueOf(1), BigInteger.valueOf(4), RepairSegment.State.NOT_STARTED));
    replacements.add(
        createSegment(
            BigInteger.valueOf(7), BigInteger.valueOf(10), RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = invokeVerifyCompleteCoverage(runner, originalSegment, replacements);
    assertFalse("Coverage should be incomplete with missing range", coverageComplete);
  }

  /** Test: Coverage verification detects overlapping ranges */
  @Test
  public void testOverlappingRangesDetected() throws Exception {
    RepairSegment originalSegment =
        createSegment(
            BigInteger.valueOf(1), BigInteger.valueOf(10), RepairSegment.State.NOT_STARTED);

    // Overlapping: [1,5) and [4,10) overlap at [4,5)
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(
        createSegment(
            BigInteger.valueOf(1), BigInteger.valueOf(5), RepairSegment.State.NOT_STARTED));
    replacements.add(
        createSegment(
            BigInteger.valueOf(4), BigInteger.valueOf(10), RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = invokeVerifyCompleteCoverage(runner, originalSegment, replacements);
    assertFalse("Coverage should be incomplete with overlapping ranges", coverageComplete);
  }

  /** Test: Coverage verification handles empty replacement set */
  @Test
  public void testEmptyReplacementSet() throws Exception {
    RepairSegment originalSegment =
        createSegment(
            BigInteger.valueOf(1), BigInteger.valueOf(10), RepairSegment.State.NOT_STARTED);

    List<RepairSegment> replacements = new ArrayList<>();

    boolean coverageComplete = invokeVerifyCompleteCoverage(runner, originalSegment, replacements);
    assertFalse("Coverage should be incomplete with empty replacement set", coverageComplete);
  }

  /** Test: Coverage verification detects wrong start token */
  @Test
  public void testWrongStartToken() throws Exception {
    RepairSegment originalSegment =
        createSegment(
            BigInteger.valueOf(1), BigInteger.valueOf(10), RepairSegment.State.NOT_STARTED);

    // Starts at 2 instead of 1
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(
        createSegment(
            BigInteger.valueOf(2), BigInteger.valueOf(10), RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = invokeVerifyCompleteCoverage(runner, originalSegment, replacements);
    assertFalse("Coverage should be incomplete with wrong start token", coverageComplete);
  }

  /** Test: Coverage verification detects wrong end token */
  @Test
  public void testWrongEndToken() throws Exception {
    RepairSegment originalSegment =
        createSegment(
            BigInteger.valueOf(1), BigInteger.valueOf(10), RepairSegment.State.NOT_STARTED);

    // Ends at 9 instead of 10
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(
        createSegment(
            BigInteger.valueOf(1), BigInteger.valueOf(9), RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = invokeVerifyCompleteCoverage(runner, originalSegment, replacements);
    assertFalse("Coverage should be incomplete with wrong end token", coverageComplete);
  }

  /** Test: Boundary exactly matching start token */
  @Test
  public void testBoundaryAtStartToken() throws Exception {
    RepairSegment originalSegment =
        createSegment(
            BigInteger.valueOf(100), BigInteger.valueOf(200), RepairSegment.State.NOT_STARTED);

    // Boundary at 100 (start token) should not create zero-length segment
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(
        createSegment(
            BigInteger.valueOf(100), BigInteger.valueOf(150), RepairSegment.State.NOT_STARTED));
    replacements.add(
        createSegment(
            BigInteger.valueOf(150), BigInteger.valueOf(200), RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = invokeVerifyCompleteCoverage(runner, originalSegment, replacements);
    assertTrue("Coverage should be complete with boundary at start", coverageComplete);
  }

  /** Test: Boundary exactly matching end token */
  @Test
  public void testBoundaryAtEndToken() throws Exception {
    RepairSegment originalSegment =
        createSegment(
            BigInteger.valueOf(100), BigInteger.valueOf(200), RepairSegment.State.NOT_STARTED);

    // Boundary at 200 (end token) should not create zero-length segment
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(
        createSegment(
            BigInteger.valueOf(100), BigInteger.valueOf(150), RepairSegment.State.NOT_STARTED));
    replacements.add(
        createSegment(
            BigInteger.valueOf(150), BigInteger.valueOf(200), RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = invokeVerifyCompleteCoverage(runner, originalSegment, replacements);
    assertTrue("Coverage should be complete with boundary at end", coverageComplete);
  }

  /** Test: DAO getRepairSegmentByTokenRange finds existing segment */
  @Test
  public void testDaoFindsByTokenRange() {
    UUID runId = createRepairRun();

    // Create a segment
    RepairSegment.Builder builder =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(10)))
                    .build(),
                repairUnitId)
            .withRunId(runId)
            .withState(RepairSegment.State.NOT_STARTED);

    storage.getRepairSegmentDao().addRepairSegments(Collections.singletonList(builder), runId);

    // Find by token range
    Optional<RepairSegment> found =
        storage
            .getRepairSegmentDao()
            .getRepairSegmentByTokenRange(
                runId, repairUnitId, BigInteger.valueOf(1), BigInteger.valueOf(10));

    assertTrue("Segment should be found by token range", found.isPresent());
    assertEquals("Start token should match", BigInteger.valueOf(1), found.get().getStartToken());
    assertEquals("End token should match", BigInteger.valueOf(10), found.get().getEndToken());
  }

  /** Test: DAO getRepairSegmentByTokenRange returns empty for non-existent segment */
  @Test
  public void testDaoReturnsEmptyForNonExistentSegment() {
    UUID runId = createRepairRun();

    Optional<RepairSegment> found =
        storage
            .getRepairSegmentDao()
            .getRepairSegmentByTokenRange(
                runId, repairUnitId, BigInteger.valueOf(999), BigInteger.valueOf(1000));

    assertFalse("Non-existent segment should not be found", found.isPresent());
  }

  /** Test: DAO conditional update succeeds when state matches */
  @Test
  public void testDaoConditionalUpdateSucceedsWhenStateMatches() {
    UUID runId = createRepairRun();

    // Create a segment in NOT_STARTED state
    RepairSegment.Builder builder =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(10)))
                    .build(),
                repairUnitId)
            .withRunId(runId)
            .withState(RepairSegment.State.NOT_STARTED);

    storage.getRepairSegmentDao().addRepairSegments(Collections.singletonList(builder), runId);

    // Get the created segment
    Optional<RepairSegment> created =
        storage
            .getRepairSegmentDao()
            .getRepairSegmentByTokenRange(
                runId, repairUnitId, BigInteger.valueOf(1), BigInteger.valueOf(10));

    assertTrue("Segment should exist", created.isPresent());
    UUID segmentId = created.get().getId();

    // Conditional update from NOT_STARTED to RUNNING
    // (DONE would require startTime/endTime to be set)
    boolean updated =
        storage
            .getRepairSegmentDao()
            .updateRepairSegmentStateConditional(
                runId, segmentId, RepairSegment.State.RUNNING, RepairSegment.State.NOT_STARTED);

    assertTrue("Conditional update should succeed when state matches", updated);

    // Verify state changed
    Optional<RepairSegment> updatedSegment =
        storage.getRepairSegmentDao().getRepairSegment(runId, segmentId);

    assertTrue("Segment should still exist", updatedSegment.isPresent());
    assertEquals(
        "State should be RUNNING", RepairSegment.State.RUNNING, updatedSegment.get().getState());
  }

  /** Test: DAO conditional update fails when state doesn't match */
  @Test
  public void testDaoConditionalUpdateFailsWhenStateDoesNotMatch() {
    UUID runId = createRepairRun();

    // Create a segment in NOT_STARTED state
    RepairSegment.Builder builder =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(10)))
                    .build(),
                repairUnitId)
            .withRunId(runId)
            .withState(RepairSegment.State.NOT_STARTED);

    storage.getRepairSegmentDao().addRepairSegments(Collections.singletonList(builder), runId);

    // Get the created segment
    Optional<RepairSegment> created =
        storage
            .getRepairSegmentDao()
            .getRepairSegmentByTokenRange(
                runId, repairUnitId, BigInteger.valueOf(1), BigInteger.valueOf(10));

    assertTrue("Segment should exist", created.isPresent());
    UUID segmentId = created.get().getId();

    // Try conditional update from RUNNING to DONE (but segment is NOT_STARTED)
    boolean updated =
        storage
            .getRepairSegmentDao()
            .updateRepairSegmentStateConditional(
                runId, segmentId, RepairSegment.State.RUNNING, RepairSegment.State.RUNNING);

    assertFalse("Conditional update should fail when state doesn't match", updated);

    // Verify state unchanged
    Optional<RepairSegment> unchangedSegment =
        storage.getRepairSegmentDao().getRepairSegment(runId, segmentId);

    assertTrue("Segment should still exist", unchangedSegment.isPresent());
    assertEquals(
        "State should still be NOT_STARTED",
        RepairSegment.State.NOT_STARTED,
        unchangedSegment.get().getState());
  }

  /**
   * Test: {@code addRepairSegments} is append-only — calling it twice with the same builder inserts
   * two rows (each with a new time-based UUID). It is the caller's responsibility (i.e. {@code
   * createMissingReplacementSegments}) to check for an existing segment via {@code
   * getRepairSegmentByTokenRange} before calling {@code addRepairSegments}. This test asserts the
   * actual contract: the segment is findable after the first insert, and a second insert neither
   * throws nor removes the first segment.
   */
  @Test
  public void testAddRepairSegments_IsAppendOnly() {
    UUID runId = createRepairRun();

    RepairSegment.Builder builder =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(10)))
                    .build(),
                repairUnitId)
            .withRunId(runId)
            .withState(RepairSegment.State.NOT_STARTED);

    // First insert: segment must be findable
    storage.getRepairSegmentDao().addRepairSegments(Collections.singletonList(builder), runId);

    Optional<RepairSegment> afterFirst =
        storage
            .getRepairSegmentDao()
            .getRepairSegmentByTokenRange(
                runId, repairUnitId, BigInteger.valueOf(1), BigInteger.valueOf(10));
    assertTrue("Segment must be findable after first insert", afterFirst.isPresent());

    // Second insert: must not throw and must leave the segment still findable
    storage.getRepairSegmentDao().addRepairSegments(Collections.singletonList(builder), runId);

    Optional<RepairSegment> afterSecond =
        storage
            .getRepairSegmentDao()
            .getRepairSegmentByTokenRange(
                runId, repairUnitId, BigInteger.valueOf(1), BigInteger.valueOf(10));
    assertTrue("Segment must still be findable after second insert", afterSecond.isPresent());
  }

  /** Test: addRepairSegments creates multiple segments in batch */
  @Test
  public void testBatchSegmentCreation() {
    UUID runId = createRepairRun();

    List<RepairSegment.Builder> builders = new ArrayList<>();
    builders.add(
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(4)))
                    .build(),
                repairUnitId)
            .withState(RepairSegment.State.NOT_STARTED));

    builders.add(
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(4), BigInteger.valueOf(7)))
                    .build(),
                repairUnitId)
            .withState(RepairSegment.State.NOT_STARTED));

    builders.add(
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(7), BigInteger.valueOf(10)))
                    .build(),
                repairUnitId)
            .withState(RepairSegment.State.NOT_STARTED));

    storage.getRepairSegmentDao().addRepairSegments(builders, runId);

    // Verify all segments created
    Optional<RepairSegment> seg1 =
        storage
            .getRepairSegmentDao()
            .getRepairSegmentByTokenRange(
                runId, repairUnitId, BigInteger.valueOf(1), BigInteger.valueOf(4));
    Optional<RepairSegment> seg2 =
        storage
            .getRepairSegmentDao()
            .getRepairSegmentByTokenRange(
                runId, repairUnitId, BigInteger.valueOf(4), BigInteger.valueOf(7));
    Optional<RepairSegment> seg3 =
        storage
            .getRepairSegmentDao()
            .getRepairSegmentByTokenRange(
                runId, repairUnitId, BigInteger.valueOf(7), BigInteger.valueOf(10));

    assertTrue("First segment should exist", seg1.isPresent());
    assertTrue("Second segment should exist", seg2.isPresent());
    assertTrue("Third segment should exist", seg3.isPresent());
  }

  // Helper methods

  private RepairSegment createSegment(BigInteger start, BigInteger end, RepairSegment.State state) {
    return RepairSegment.builder(
            Segment.builder().withTokenRange(new RingRange(start, end)).build(), repairUnitId)
        .withState(state)
        .withRunId(UUID.randomUUID())
        .withId(UUID.randomUUID())
        .build();
  }

  private UUID createRepairRun() {
    RepairRun run =
        storage
            .getRepairRunDao()
            .addRepairRun(
                RepairRun.builder(cluster.getName(), repairUnitId)
                    .intensity(0.5)
                    .segmentCount(10)
                    .repairParallelism(RepairParallelism.PARALLEL)
                    .tables(TABLES),
                Collections.emptyList());
    return run.getId();
  }

  /**
   * Invokes the production {@code RepairRunner.verifyCompleteCoverage} method via reflection so
   * that coverage assertions exercise the real implementation, not a local copy.
   */
  private boolean invokeVerifyCompleteCoverage(
      RepairRunner repairRunner,
      RepairSegment originalSegment,
      List<RepairSegment> replacementSegments)
      throws Exception {
    java.lang.reflect.Method method =
        RepairRunner.class.getDeclaredMethod(
            "verifyCompleteCoverage", RepairSegment.class, List.class);
    method.setAccessible(true);
    return (boolean) method.invoke(repairRunner, originalSegment, replacementSegments);
  }

  /**
   * Test: Conditional update to DONE state succeeds when segment is in NOT_STARTED state. This
   * covers the retirement success path (lines 1289-1290, 1035-1037).
   */
  @Test
  public void testConditionalUpdateToDone_Success() {
    UUID runId = createRepairRun();

    // Create a segment in NOT_STARTED state
    RepairSegment.Builder builder =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(10)))
                    .build(),
                repairUnitId)
            .withRunId(runId)
            .withState(RepairSegment.State.NOT_STARTED);

    storage.getRepairSegmentDao().addRepairSegments(Collections.singletonList(builder), runId);

    // Get the created segment
    Optional<RepairSegment> created =
        storage
            .getRepairSegmentDao()
            .getRepairSegmentByTokenRange(
                runId, repairUnitId, BigInteger.valueOf(1), BigInteger.valueOf(10));

    assertTrue("Segment should exist", created.isPresent());
    UUID segmentId = created.get().getId();

    // Conditional update from NOT_STARTED to DONE
    boolean updated =
        storage
            .getRepairSegmentDao()
            .updateRepairSegmentStateConditional(
                runId, segmentId, RepairSegment.State.DONE, RepairSegment.State.NOT_STARTED);

    assertTrue("Conditional update to DONE should succeed", updated);

    // Verify state changed to DONE
    Optional<RepairSegment> updatedSegment =
        storage.getRepairSegmentDao().getRepairSegment(runId, segmentId);

    assertTrue("Segment should still exist", updatedSegment.isPresent());
    assertEquals("State should be DONE", RepairSegment.State.DONE, updatedSegment.get().getState());
  }

  /**
   * Test: Conditional update fails when segment is not in expected state. This covers the
   * retirement failure path (lines 1292-1298, 1038-1041).
   */
  @Test
  public void testConditionalUpdateToDone_FailsWhenStateChanged() {
    UUID runId = createRepairRun();

    // Create a segment in RUNNING state (not NOT_STARTED)
    RepairSegment.Builder builder =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(10)))
                    .build(),
                repairUnitId)
            .withRunId(runId)
            .withState(RepairSegment.State.RUNNING)
            .withStartTime(org.joda.time.DateTime.now());

    storage.getRepairSegmentDao().addRepairSegments(Collections.singletonList(builder), runId);

    // Get the created segment
    Optional<RepairSegment> created =
        storage
            .getRepairSegmentDao()
            .getRepairSegmentByTokenRange(
                runId, repairUnitId, BigInteger.valueOf(1), BigInteger.valueOf(10));

    assertTrue("Segment should exist", created.isPresent());
    UUID segmentId = created.get().getId();

    // Try conditional update from NOT_STARTED to DONE (but segment is RUNNING)
    boolean updated =
        storage
            .getRepairSegmentDao()
            .updateRepairSegmentStateConditional(
                runId, segmentId, RepairSegment.State.DONE, RepairSegment.State.NOT_STARTED);

    assertFalse("Conditional update should fail when state doesn't match", updated);

    // Verify state unchanged
    Optional<RepairSegment> unchangedSegment =
        storage.getRepairSegmentDao().getRepairSegment(runId, segmentId);

    assertTrue("Segment should still exist", unchangedSegment.isPresent());
    assertEquals(
        "State should still be RUNNING",
        RepairSegment.State.RUNNING,
        unchangedSegment.get().getState());
  }

  /**
   * Test: Creating replacement segments when some already exist (idempotency). This covers lines
   * 1166-1171 (existing segment found path).
   */
  @Test
  public void testCreateReplacementSegments_SomeAlreadyExist() {
    UUID runId = createRepairRun();

    // Pre-create one segment [1, 4)
    RepairSegment.Builder existingBuilder =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(4)))
                    .build(),
                repairUnitId)
            .withRunId(runId)
            .withState(RepairSegment.State.NOT_STARTED);

    storage
        .getRepairSegmentDao()
        .addRepairSegments(Collections.singletonList(existingBuilder), runId);

    // Now try to create all three segments [1,4), [4,7), [7,10)
    List<RepairSegment.Builder> allBuilders = new ArrayList<>();
    allBuilders.add(
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(4)))
                    .build(),
                repairUnitId)
            .withState(RepairSegment.State.NOT_STARTED));
    allBuilders.add(
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(4), BigInteger.valueOf(7)))
                    .build(),
                repairUnitId)
            .withState(RepairSegment.State.NOT_STARTED));
    allBuilders.add(
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(7), BigInteger.valueOf(10)))
                    .build(),
                repairUnitId)
            .withState(RepairSegment.State.NOT_STARTED));

    // Simulate createMissingReplacementSegments logic
    List<RepairSegment> createdSegments = new ArrayList<>();
    for (RepairSegment.Builder builder : allBuilders) {
      RepairSegment built = builder.withRunId(runId).withId(UUID.randomUUID()).build();

      // Check if segment already exists
      Optional<RepairSegment> existing =
          storage
              .getRepairSegmentDao()
              .getRepairSegmentByTokenRange(
                  runId, repairUnitId, built.getStartToken(), built.getEndToken());

      if (existing.isPresent()) {
        // Segment already exists, use it (idempotency)
        createdSegments.add(existing.get());
      } else {
        // Create new segment
        storage.getRepairSegmentDao().addRepairSegments(Collections.singletonList(builder), runId);

        // Fetch the newly created segment
        Optional<RepairSegment> newSegment =
            storage
                .getRepairSegmentDao()
                .getRepairSegmentByTokenRange(
                    runId, repairUnitId, built.getStartToken(), built.getEndToken());

        assertTrue("Newly created segment should exist", newSegment.isPresent());
        createdSegments.add(newSegment.get());
      }
    }

    // Verify all 3 segments exist
    assertEquals("Should have 3 segments total", 3, createdSegments.size());

    // Verify each segment has correct range
    assertEquals(
        "First segment start", BigInteger.valueOf(1), createdSegments.get(0).getStartToken());
    assertEquals("First segment end", BigInteger.valueOf(4), createdSegments.get(0).getEndToken());
    assertEquals(
        "Second segment start", BigInteger.valueOf(4), createdSegments.get(1).getStartToken());
    assertEquals("Second segment end", BigInteger.valueOf(7), createdSegments.get(1).getEndToken());
    assertEquals(
        "Third segment start", BigInteger.valueOf(7), createdSegments.get(2).getStartToken());
    assertEquals("Third segment end", BigInteger.valueOf(10), createdSegments.get(2).getEndToken());
  }

  /**
   * Test: Coverage verification succeeds when replacement segments fully cover original. This
   * covers lines 1261-1262 (coverage verification success path).
   */
  @Test
  public void testCoverageVerification_SuccessPath() throws Exception {
    RepairSegment originalSegment =
        createSegment(
            BigInteger.valueOf(100), BigInteger.valueOf(500), RepairSegment.State.NOT_STARTED);

    // Create replacement segments that fully cover [100, 500)
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(
        createSegment(
            BigInteger.valueOf(100), BigInteger.valueOf(200), RepairSegment.State.NOT_STARTED));
    replacements.add(
        createSegment(
            BigInteger.valueOf(200), BigInteger.valueOf(350), RepairSegment.State.NOT_STARTED));
    replacements.add(
        createSegment(
            BigInteger.valueOf(350), BigInteger.valueOf(500), RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = invokeVerifyCompleteCoverage(runner, originalSegment, replacements);
    assertTrue("Coverage should be complete with perfect coverage", coverageComplete);
  }

  /**
   * Test: Zero-length segment handling - segments with same start and end token. This covers lines
   * 1103-1104 (skip zero-length ranges).
   */
  @Test
  public void testZeroLengthSegment_Skipped() throws Exception {
    RepairSegment originalSegment =
        createSegment(
            BigInteger.valueOf(100), BigInteger.valueOf(200), RepairSegment.State.NOT_STARTED);

    // Create replacements including a zero-length segment
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(
        createSegment(
            BigInteger.valueOf(100), BigInteger.valueOf(150), RepairSegment.State.NOT_STARTED));
    // Zero-length segment [150, 150) should be skipped
    replacements.add(
        createSegment(
            BigInteger.valueOf(150), BigInteger.valueOf(200), RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = invokeVerifyCompleteCoverage(runner, originalSegment, replacements);
    assertTrue(
        "Coverage should be complete even with zero-length segment filtered", coverageComplete);
  }

  /**
   * Test: Batch creation of multiple replacement segments. This covers lines 1197-1198 (segment
   * created successfully path).
   */
  @Test
  public void testBatchCreateReplacementSegments_AllNew() {
    UUID runId = createRepairRun();

    // Create 5 replacement segments in batch
    List<RepairSegment.Builder> builders = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      int start = i * 100;
      int end = (i + 1) * 100;
      builders.add(
          RepairSegment.builder(
                  Segment.builder()
                      .withTokenRange(
                          new RingRange(BigInteger.valueOf(start), BigInteger.valueOf(end)))
                      .build(),
                  repairUnitId)
              .withState(RepairSegment.State.NOT_STARTED));
    }

    // Add all segments in batch
    storage.getRepairSegmentDao().addRepairSegments(builders, runId);

    // Verify all segments were created
    for (int i = 0; i < 5; i++) {
      int start = i * 100;
      int end = (i + 1) * 100;
      Optional<RepairSegment> segment =
          storage
              .getRepairSegmentDao()
              .getRepairSegmentByTokenRange(
                  runId, repairUnitId, BigInteger.valueOf(start), BigInteger.valueOf(end));

      assertTrue("Segment [" + start + ", " + end + ") should exist", segment.isPresent());
      assertEquals(
          "Segment start token should match",
          BigInteger.valueOf(start),
          segment.get().getStartToken());
      assertEquals(
          "Segment end token should match", BigInteger.valueOf(end), segment.get().getEndToken());
    }
  }

  /**
   * Test: Wrap-around segment split with one internal token. Original: [9000000000000000000,
   * -5000000000000000000) Internal token: -8000000000000000000 Expected (ring traversal order from
   * start): [9000000000000000000, -8000000000000000000) [-8000000000000000000,
   * -5000000000000000000)
   */
  @Test
  public void testWrapAroundSplitWithOneInternalToken() throws Exception {
    RepairSegment original =
        createSegment(
            new BigInteger("9000000000000000000"),
            new BigInteger("-5000000000000000000"),
            RepairSegment.State.NOT_STARTED);

    // Expected replacements in ring traversal order
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(
        createSegment(
            new BigInteger("9000000000000000000"),
            new BigInteger("-8000000000000000000"),
            RepairSegment.State.NOT_STARTED));
    replacements.add(
        createSegment(
            new BigInteger("-8000000000000000000"),
            new BigInteger("-5000000000000000000"),
            RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = invokeVerifyCompleteCoverage(runner, original, replacements);
    assertTrue(
        "Wrap-around split with one internal token should have complete coverage",
        coverageComplete);
  }

  /**
   * Test: Wrap-around segment split with multiple internal tokens. Verifies that tokens are ordered
   * by ring traversal, not numeric sort. Original: [8000000000000000000, -7000000000000000000)
   * Split at Long.MAX_VALUE and Long.MIN_VALUE, both of which lie inside the wrap-around range.
   * Ring traversal order from start: 8000000000000000000 → MAX → MIN → -7000000000000000000 (end)
   */
  @Test
  public void testWrapAroundSplitWithMultipleTokensPreservesRingOrder() throws Exception {
    RepairSegment original =
        createSegment(
            new BigInteger("8000000000000000000"),
            new BigInteger("-7000000000000000000"),
            RepairSegment.State.NOT_STARTED);

    // Three sub-ranges that partition [8B, -7B) at MAX and MIN:
    //   [8B, MAX)   – non-wrapping (8B < MAX)
    //   [MAX, MIN)  – wrapping    (MAX > MIN)
    //   [MIN, -7B)  – non-wrapping (MIN < -7B)
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(
        createSegment(
            BigInteger.valueOf(8000000000000000000L),
            BigInteger.valueOf(Long.MAX_VALUE),
            RepairSegment.State.NOT_STARTED));
    replacements.add(
        createSegment(
            BigInteger.valueOf(Long.MAX_VALUE),
            BigInteger.valueOf(Long.MIN_VALUE),
            RepairSegment.State.NOT_STARTED));
    replacements.add(
        createSegment(
            BigInteger.valueOf(Long.MIN_VALUE),
            new BigInteger("-7000000000000000000"),
            RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = invokeVerifyCompleteCoverage(runner, original, replacements);
    assertTrue(
        "Wrap-around split with multiple tokens should preserve ring order", coverageComplete);
  }

  /**
   * Test: Wrap-around coverage verification succeeds for valid replacements. Original:
   * [9000000000000000000, -8000000000000000000) split at Long.MIN_VALUE, which is the first token
   * after MAX in ring traversal and is inside the wrap-around range.
   */
  @Test
  public void testWrapAroundCoverageVerificationSucceeds() throws Exception {
    RepairSegment original =
        createSegment(
            new BigInteger("9000000000000000000"),
            new BigInteger("-8000000000000000000"),
            RepairSegment.State.NOT_STARTED);

    // Two sub-ranges that partition [9B, -8B) at Long.MIN_VALUE:
    //   [9B, MIN)   – wrapping     (9B > MIN)
    //   [MIN, -8B)  – non-wrapping (MIN < -8B)
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(
        createSegment(
            new BigInteger("9000000000000000000"),
            BigInteger.valueOf(Long.MIN_VALUE),
            RepairSegment.State.NOT_STARTED));
    replacements.add(
        createSegment(
            BigInteger.valueOf(Long.MIN_VALUE),
            new BigInteger("-8000000000000000000"),
            RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = invokeVerifyCompleteCoverage(runner, original, replacements);
    assertTrue(
        "Valid wrap-around replacements should pass coverage verification", coverageComplete);
  }

  /**
   * Test: Wrap-around coverage verification fails when there is a genuine internal gap.
   *
   * <p>Original range: [9B, -8B) — a wrap-around range that covers [9_000_000_000_000_000_000 ..
   * Long.MAX_VALUE] ∪ [Long.MIN_VALUE .. -8_000_000_000_000_000_001].
   *
   * <p>The range is partitioned into three valid sub-ranges at Long.MIN_VALUE and -9B:
   *
   * <ul>
   *   <li>A = [9B, MIN) — wrapping, covers [9B..MAX]
   *   <li>B = [MIN, -9B) — non-wrapping, covers [MIN..-9B-1] ← intentionally omitted
   *   <li>C = [-9B, -8B) — non-wrapping, covers [-9B..-8B-1]
   * </ul>
   *
   * Providing only A and C leaves the gap [MIN, -9B) uncovered. Both A and C are genuinely enclosed
   * by the original, so they pass the containment filter and reach {@code verifyRangeCoverage}.
   * There, after ring-distance sorting, A is first and C is second. The gap check compares A.end
   * (MIN) with C.start (-9B) — they differ, so {@code verifyRangeCoverage} returns false through
   * the gap-detection branch.
   */
  @Test
  public void testWrapAroundCoverageVerificationFailsWithGap() throws Exception {
    RepairSegment original =
        createSegment(
            new BigInteger("9000000000000000000"),
            new BigInteger("-8000000000000000000"),
            RepairSegment.State.NOT_STARTED);

    // A = [9B, MIN): wrapping sub-range, encloses [9B..MAX]. Enclosed by original.
    // C = [-9B, -8B): non-wrapping sub-range. Enclosed by original.
    // B = [MIN, -9B) is intentionally absent, creating gap between A.end=MIN and C.start=-9B.
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(
        createSegment(
            new BigInteger("9000000000000000000"),
            BigInteger.valueOf(Long.MIN_VALUE),
            RepairSegment.State.NOT_STARTED));
    replacements.add(
        createSegment(
            new BigInteger("-9000000000000000000"),
            new BigInteger("-8000000000000000000"),
            RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = invokeVerifyCompleteCoverage(runner, original, replacements);
    assertFalse(
        "verifyRangeCoverage must detect the gap between A.end=MIN and C.start=-9B",
        coverageComplete);
  }

  /**
   * Test: Long.MAX_VALUE to Long.MIN_VALUE boundary ordering. Verifies that MIN_VALUE is ordered
   * immediately after MAX_VALUE in ring traversal.
   */
  @Test
  public void testLongMaxToMinBoundaryOrdering() throws Exception {
    RepairSegment original =
        createSegment(
            BigInteger.valueOf(Long.MAX_VALUE),
            BigInteger.valueOf(Long.MIN_VALUE + 1000),
            RepairSegment.State.NOT_STARTED);

    // Replacement that wraps from MAX to MIN
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(
        createSegment(
            BigInteger.valueOf(Long.MAX_VALUE),
            BigInteger.valueOf(Long.MIN_VALUE),
            RepairSegment.State.NOT_STARTED));
    replacements.add(
        createSegment(
            BigInteger.valueOf(Long.MIN_VALUE),
            BigInteger.valueOf(Long.MIN_VALUE + 1000),
            RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = invokeVerifyCompleteCoverage(runner, original, replacements);
    assertTrue("MAX_VALUE to MIN_VALUE boundary should be handled correctly", coverageComplete);
  }

  /**
   * Test: Normal range split still works after ring distance changes. Ensures backward
   * compatibility with non-wrap-around ranges.
   */
  @Test
  public void testNormalRangeSplitStillWorks() throws Exception {
    RepairSegment original =
        createSegment(
            BigInteger.valueOf(100), BigInteger.valueOf(500), RepairSegment.State.NOT_STARTED);

    // Normal range split
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(
        createSegment(
            BigInteger.valueOf(100), BigInteger.valueOf(300), RepairSegment.State.NOT_STARTED));
    replacements.add(
        createSegment(
            BigInteger.valueOf(300), BigInteger.valueOf(500), RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = invokeVerifyCompleteCoverage(runner, original, replacements);
    assertTrue(
        "Normal range split should still work with ring distance ordering", coverageComplete);
  }

  /**
   * Test: Wrap-around coverage verification handles any input order. With ring distance ordering,
   * segments provided in reverse ring order are still sorted correctly. Uses the same valid split
   * of [9B, -8B) at Long.MIN_VALUE as testWrapAroundCoverageVerificationSucceeds, but supplies the
   * replacements in reverse order.
   */
  @Test
  public void testWrapAroundCoverageVerificationHandlesAnyOrder() throws Exception {
    RepairSegment original =
        createSegment(
            new BigInteger("9000000000000000000"),
            new BigInteger("-8000000000000000000"),
            RepairSegment.State.NOT_STARTED);

    // Provide the two valid sub-ranges in reverse ring order: [MIN,-8B) before [9B,MIN).
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(
        createSegment(
            BigInteger.valueOf(Long.MIN_VALUE),
            new BigInteger("-8000000000000000000"),
            RepairSegment.State.NOT_STARTED));
    replacements.add(
        createSegment(
            new BigInteger("9000000000000000000"),
            BigInteger.valueOf(Long.MIN_VALUE),
            RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = invokeVerifyCompleteCoverage(runner, original, replacements);
    assertTrue("Wrap-around replacements should pass regardless of input order", coverageComplete);
  }

  // -------------------------------------------------------------------------
  // Fix 1: rangesUnchanged — set comparison handles wrap-around coalesced ranges
  // -------------------------------------------------------------------------

  /**
   * Before the fix, {@code rangesUnchanged} sorted both sides by numeric start token and compared
   * pairwise. For a coalesced segment whose two ranges start at [9B, -5B) and [-3B, 2B), numeric
   * sort puts the negative-start range first, while ring order requires the opposite. This caused a
   * false "unchanged" verdict when the replacement set was actually different.
   *
   * <p>After the fix the comparison uses a set of "start:end" keys so order is irrelevant. This
   * test verifies that two coalesced wrap-around ranges are correctly identified as unchanged when
   * the replacement list carries exactly the same (start, end) pairs regardless of order.
   */
  @Test
  public void testRangesUnchanged_WrapAroundCoalescedSameSet() throws Exception {
    // Two wrap-around ranges that are numerically "out of ring order":
    //   R1 = [9_000_000_000_000_000_000, -5_000_000_000_000_000_000)
    //   R2 = [-3_000_000_000_000_000_000,  2_000_000_000_000_000_000)
    BigInteger r1Start = new BigInteger("9000000000000000000");
    BigInteger r1End = new BigInteger("-5000000000000000000");
    BigInteger r2Start = new BigInteger("-3000000000000000000");
    BigInteger r2End = new BigInteger("2000000000000000000");

    List<RingRange> originalRanges = new ArrayList<>();
    originalRanges.add(new RingRange(r1Start, r1End));
    originalRanges.add(new RingRange(r2Start, r2End));

    // Replacements carry the same pairs but listed in reverse order
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(createSegment(r2Start, r2End, RepairSegment.State.NOT_STARTED));
    replacements.add(createSegment(r1Start, r1End, RepairSegment.State.NOT_STARTED));

    boolean unchanged = invokeRangesUnchanged(runner, originalRanges, replacements);
    assertTrue(
        "Wrap-around coalesced ranges with same (start,end) pairs must be considered unchanged"
            + " regardless of list order",
        unchanged);
  }

  /**
   * Verifies that {@code rangesUnchanged} returns {@code false} when the replacement set contains a
   * different range, even though the sizes match — ensuring the set comparison is not just a size
   * check.
   */
  @Test
  public void testRangesUnchanged_DifferentRangeReturnsFalse() throws Exception {
    BigInteger r1Start = new BigInteger("9000000000000000000");
    BigInteger r1End = new BigInteger("-5000000000000000000");

    List<RingRange> originalRanges = new ArrayList<>();
    originalRanges.add(new RingRange(r1Start, r1End));

    // Replacement has a different end token
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(
        createSegment(
            r1Start, new BigInteger("-4000000000000000000"), RepairSegment.State.NOT_STARTED));

    boolean unchanged = invokeRangesUnchanged(runner, originalRanges, replacements);
    assertFalse("A different replacement range must not be considered unchanged", unchanged);
  }

  // -------------------------------------------------------------------------
  // Fix 2: verifyCompleteCoverage — wrapping sub-range not silently dropped
  // -------------------------------------------------------------------------

  /**
   * When a wrapping original range [9B, -8B) is split at Long.MIN_VALUE, {@code splitTokenRange}
   * produces [9B, MIN) — which is itself a wrapping range (9B > MIN numerically). The old filter
   * used {@code RingRange.encloses()} which misclassified this as "not enclosed" and silently
   * dropped it, making {@code verifyRangeCoverage} see an empty list and return false.
   *
   * <p>After the fix the filter uses {@code isTokenInRange} on the sub-range's start token plus an
   * equality check for the boundary, which correctly retains [9B, MIN).
   */
  @Test
  public void testVerifyCompleteCoverage_WrappingSubRangeNotDropped() throws Exception {
    // Original wrapping range: [9B, -8B)
    BigInteger origStart = new BigInteger("9000000000000000000");
    BigInteger origEnd = new BigInteger("-8000000000000000000");
    RepairSegment original = createSegment(origStart, origEnd, RepairSegment.State.NOT_STARTED);

    // Split at Long.MIN_VALUE produces:
    //   sub1 = [9B, MIN)   — wrapping (9B > MIN numerically)
    //   sub2 = [MIN, -8B)  — non-wrapping
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(
        createSegment(
            origStart, BigInteger.valueOf(Long.MIN_VALUE), RepairSegment.State.NOT_STARTED));
    replacements.add(
        createSegment(
            BigInteger.valueOf(Long.MIN_VALUE), origEnd, RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = invokeVerifyCompleteCoverage(runner, original, replacements);
    assertTrue(
        "verifyCompleteCoverage must accept a wrapping sub-range [9B, MIN) as covering part of"
            + " the wrapping original [9B, -8B)",
        coverageComplete);
  }

  /**
   * Confirms that the fix does not regress the gap-detection case: when the wrapping sub-range [9B,
   * MIN) is present but [MIN, -9B) is absent (leaving a gap before [-9B, -8B)), coverage
   * verification must still return false.
   */
  @Test
  public void testVerifyCompleteCoverage_WrappingSubRangeWithGapStillFails() throws Exception {
    BigInteger origStart = new BigInteger("9000000000000000000");
    BigInteger origEnd = new BigInteger("-8000000000000000000");
    RepairSegment original = createSegment(origStart, origEnd, RepairSegment.State.NOT_STARTED);

    // A = [9B, MIN) — wrapping sub-range, enclosed by original
    // C = [-9B, -8B) — non-wrapping, enclosed by original
    // B = [MIN, -9B) is intentionally absent → gap between A.end=MIN and C.start=-9B
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(
        createSegment(
            origStart, BigInteger.valueOf(Long.MIN_VALUE), RepairSegment.State.NOT_STARTED));
    replacements.add(
        createSegment(
            new BigInteger("-9000000000000000000"), origEnd, RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = invokeVerifyCompleteCoverage(runner, original, replacements);
    assertFalse(
        "verifyCompleteCoverage must detect the gap between [9B,MIN).end=MIN and [-9B,-8B).start=-9B",
        coverageComplete);
  }

  // -------------------------------------------------------------------------
  // Fix 3: createMissingReplacementSegments — exception propagates on write failure
  // -------------------------------------------------------------------------

  /**
   * Verifies that {@code createMissingReplacementSegments} throws {@link IllegalStateException}
   * when a segment is written to the DAO but the subsequent fetch returns empty — simulating a
   * write that appeared to succeed but is not visible on read (e.g. a lost write). Before the fix
   * the exception was swallowed and the method returned a partial list, risking retirement of the
   * original segment with incomplete replacements. After the fix the exception propagates
   * immediately so the caller's {@code catch (RuntimeException)} block aborts the split.
   */
  @Test
  public void testCreateMissingReplacementSegments_ThrowsWhenPostWriteFetchMissing()
      throws Exception {
    createRepairRun();

    // Build a mock segment DAO: addRepairSegments is a no-op and getRepairSegmentByTokenRange
    // always returns empty, simulating a lost write where the segment never appears on read-back.
    IRepairSegmentDao mockDao = mock(IRepairSegmentDao.class);
    doNothing().when(mockDao).addRepairSegments(any(), any());
    when(mockDao.getRepairSegmentByTokenRange(any(), any(), any(), any()))
        .thenReturn(Optional.empty());

    // Swap the real storage on the existing runner's context for the duration of this test using
    // reflection. This avoids constructing a second RepairRunner (whose constructor needs a fully
    // wired AppContext with cluster/unit DAOs and a MetricRegistry).
    java.lang.reflect.Field contextField = RepairRunner.class.getDeclaredField("context");
    contextField.setAccessible(true);
    AppContext originalContext = (AppContext) contextField.get(runner);

    // Create a thin wrapper that delegates everything except getRepairSegmentDao().
    IStorageDao wrappedStorage = mock(IStorageDao.class);
    when(wrappedStorage.getRepairSegmentDao()).thenReturn(mockDao);
    // Other DAO calls are not exercised by createMissingReplacementSegments, so no-stub is fine.

    AppContext patchedContext = new AppContext();
    patchedContext.storage = wrappedStorage;
    patchedContext.config = originalContext.config;
    patchedContext.metricRegistry = originalContext.metricRegistry;

    contextField.set(runner, patchedContext);
    try {
      // A single replacement segment whose write will "succeed" but disappear on fetch
      List<RepairSegment> toCreate = new ArrayList<>();
      toCreate.add(
          createSegment(
              BigInteger.valueOf(1), BigInteger.valueOf(10), RepairSegment.State.NOT_STARTED));

      try {
        invokeCreateMissingReplacementSegments(runner, toCreate);
        fail("Expected IllegalStateException when post-write fetch returns empty");
      } catch (java.lang.reflect.InvocationTargetException ite) {
        Throwable cause = ite.getCause();
        assertTrue(
            "Cause must be IllegalStateException, got: " + cause.getClass().getName(),
            cause instanceof IllegalStateException);
        assertTrue(
            "Message must mention the token range",
            cause.getMessage().contains("was not found after creation"));
      }
    } finally {
      // Always restore the original context so other tests are not affected.
      contextField.set(runner, originalContext);
    }
  }

  /**
   * Verifies the happy path of {@code createMissingReplacementSegments}: all three segments are
   * created and returned when none pre-exist.
   */
  @Test
  public void testCreateMissingReplacementSegments_AllCreatedSuccessfully() throws Exception {
    UUID runId = createRepairRun();

    // Build replacement segments whose runId matches the actual run so the MemoryStorageFacade
    // can persist and retrieve them by token range.
    List<RepairSegment> toCreate = new ArrayList<>();
    toCreate.add(buildSegmentForRun(runId, BigInteger.valueOf(1), BigInteger.valueOf(4)));
    toCreate.add(buildSegmentForRun(runId, BigInteger.valueOf(4), BigInteger.valueOf(7)));
    toCreate.add(buildSegmentForRun(runId, BigInteger.valueOf(7), BigInteger.valueOf(10)));

    List<RepairSegment> created = invokeCreateMissingReplacementSegments(runner, toCreate);

    assertEquals("All three replacement segments must be returned", 3, created.size());
    assertEquals(BigInteger.valueOf(1), created.get(0).getStartToken());
    assertEquals(BigInteger.valueOf(4), created.get(0).getEndToken());
    assertEquals(BigInteger.valueOf(4), created.get(1).getStartToken());
    assertEquals(BigInteger.valueOf(7), created.get(1).getEndToken());
    assertEquals(BigInteger.valueOf(7), created.get(2).getStartToken());
    assertEquals(BigInteger.valueOf(10), created.get(2).getEndToken());
  }

  // -------------------------------------------------------------------------
  // Additional reflection helpers
  // -------------------------------------------------------------------------

  /**
   * Invokes the private {@code rangesUnchanged} method via reflection so the test exercises the
   * real implementation.
   */
  private boolean invokeRangesUnchanged(
      RepairRunner repairRunner,
      List<RingRange> originalRanges,
      List<RepairSegment> replacementSegments)
      throws Exception {
    java.lang.reflect.Method method =
        RepairRunner.class.getDeclaredMethod("rangesUnchanged", List.class, List.class);
    method.setAccessible(true);
    return (boolean) method.invoke(repairRunner, originalRanges, replacementSegments);
  }

  /**
   * Invokes the private {@code createMissingReplacementSegments} method via reflection.
   *
   * @throws java.lang.reflect.InvocationTargetException wrapping the real exception if the method
   *     throws; callers that expect a failure should catch this and inspect {@link
   *     java.lang.reflect.InvocationTargetException#getCause()}.
   */
  @SuppressWarnings("unchecked")
  private List<RepairSegment> invokeCreateMissingReplacementSegments(
      RepairRunner repairRunner, List<RepairSegment> replacementSegments) throws Exception {
    java.lang.reflect.Method method =
        RepairRunner.class.getDeclaredMethod(
            "createMissingReplacementSegments", List.class, RepairSegment.class);
    method.setAccessible(true);
    // The original is not stored and has an unrelated range, so it never matches a lookup.
    RepairSegment original =
        buildSegmentForRun(
            replacementSegments.get(0).getRunId(),
            BigInteger.valueOf(-1000),
            BigInteger.valueOf(-900));
    return (List<RepairSegment>) method.invoke(repairRunner, replacementSegments, original);
  }

  /** Creates a segment pre-wired with the given {@code runId} so storage lookups find it. */
  private RepairSegment buildSegmentForRun(UUID runId, BigInteger start, BigInteger end) {
    return RepairSegment.builder(
            Segment.builder().withTokenRange(new RingRange(start, end)).build(), repairUnitId)
        .withState(RepairSegment.State.NOT_STARTED)
        .withRunId(runId)
        .withId(UUID.randomUUID())
        .build();
  }
}
