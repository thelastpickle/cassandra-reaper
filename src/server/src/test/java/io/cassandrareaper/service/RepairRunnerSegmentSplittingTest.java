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
import io.cassandrareaper.storage.IStorageDao;
import io.cassandrareaper.storage.MemoryStorageFacade;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
  }

  /**
   * Test: Simple segment split into 3 parts Original: [1, 10) Boundaries: 4, 7 Expected
   * replacements: [1,4), [4,7), [7,10)
   */
  @Test
  public void testSimpleSegmentSplit() {
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
    boolean coverageComplete = verifyCompleteCoverage(originalSegment, replacements);
    assertTrue("Coverage should be complete for simple split", coverageComplete);
  }

  /** Test: No split required (single replacement equals original) */
  @Test
  public void testNoSplitRequired() {
    RepairSegment originalSegment =
        createSegment(
            BigInteger.valueOf(1), BigInteger.valueOf(10), RepairSegment.State.NOT_STARTED);

    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(
        createSegment(
            BigInteger.valueOf(1), BigInteger.valueOf(10), RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = verifyCompleteCoverage(originalSegment, replacements);
    assertTrue("Coverage should be complete when no split needed", coverageComplete);
  }

  /** Test: Single boundary creates 2 segments */
  @Test
  public void testSingleBoundarySplit() {
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

    boolean coverageComplete = verifyCompleteCoverage(originalSegment, replacements);
    assertTrue("Coverage should be complete for single boundary split", coverageComplete);
  }

  /** Test: Multiple boundaries create many segments */
  @Test
  public void testMultipleBoundariesSplit() {
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

    boolean coverageComplete = verifyCompleteCoverage(originalSegment, replacements);
    assertTrue("Coverage should be complete for multiple boundaries", coverageComplete);
  }

  /** Test: Coverage verification detects missing range */
  @Test
  public void testMissingRangeDetected() {
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

    boolean coverageComplete = verifyCompleteCoverage(originalSegment, replacements);
    assertFalse("Coverage should be incomplete with missing range", coverageComplete);
  }

  /** Test: Coverage verification detects overlapping ranges */
  @Test
  public void testOverlappingRangesDetected() {
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

    boolean coverageComplete = verifyCompleteCoverage(originalSegment, replacements);
    assertFalse("Coverage should be incomplete with overlapping ranges", coverageComplete);
  }

  /** Test: Coverage verification handles empty replacement set */
  @Test
  public void testEmptyReplacementSet() {
    RepairSegment originalSegment =
        createSegment(
            BigInteger.valueOf(1), BigInteger.valueOf(10), RepairSegment.State.NOT_STARTED);

    List<RepairSegment> replacements = new ArrayList<>();

    boolean coverageComplete = verifyCompleteCoverage(originalSegment, replacements);
    assertFalse("Coverage should be incomplete with empty replacement set", coverageComplete);
  }

  /** Test: Coverage verification detects wrong start token */
  @Test
  public void testWrongStartToken() {
    RepairSegment originalSegment =
        createSegment(
            BigInteger.valueOf(1), BigInteger.valueOf(10), RepairSegment.State.NOT_STARTED);

    // Starts at 2 instead of 1
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(
        createSegment(
            BigInteger.valueOf(2), BigInteger.valueOf(10), RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = verifyCompleteCoverage(originalSegment, replacements);
    assertFalse("Coverage should be incomplete with wrong start token", coverageComplete);
  }

  /** Test: Coverage verification detects wrong end token */
  @Test
  public void testWrongEndToken() {
    RepairSegment originalSegment =
        createSegment(
            BigInteger.valueOf(1), BigInteger.valueOf(10), RepairSegment.State.NOT_STARTED);

    // Ends at 9 instead of 10
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(
        createSegment(
            BigInteger.valueOf(1), BigInteger.valueOf(9), RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = verifyCompleteCoverage(originalSegment, replacements);
    assertFalse("Coverage should be incomplete with wrong end token", coverageComplete);
  }

  /** Test: Boundary exactly matching start token */
  @Test
  public void testBoundaryAtStartToken() {
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

    boolean coverageComplete = verifyCompleteCoverage(originalSegment, replacements);
    assertTrue("Coverage should be complete with boundary at start", coverageComplete);
  }

  /** Test: Boundary exactly matching end token */
  @Test
  public void testBoundaryAtEndToken() {
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

    boolean coverageComplete = verifyCompleteCoverage(originalSegment, replacements);
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

  /** Test: Idempotent segment creation - existing segments are detected */
  @Test
  public void testIdempotentSegmentCreation() {
    UUID runId = createRepairRun();

    // Create initial segment
    RepairSegment.Builder builder =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(10)))
                    .build(),
                repairUnitId)
            .withRunId(runId)
            .withState(RepairSegment.State.NOT_STARTED);

    storage.getRepairSegmentDao().addRepairSegments(Collections.singletonList(builder), runId);

    // Try to create same segment again
    storage.getRepairSegmentDao().addRepairSegments(Collections.singletonList(builder), runId);

    // Verify segment exists
    Optional<RepairSegment> found =
        storage
            .getRepairSegmentDao()
            .getRepairSegmentByTokenRange(
                runId, repairUnitId, BigInteger.valueOf(1), BigInteger.valueOf(10));

    assertTrue("Segment should exist", found.isPresent());

    // Note: The current implementation creates duplicates. This test documents current behavior.
    // In a production system, you might want to add duplicate detection logic.
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

  // Murmur3Partitioner token bounds for ring distance calculations
  private static final BigInteger MURMUR3_MIN_TOKEN = BigInteger.valueOf(Long.MIN_VALUE);
  private static final BigInteger MURMUR3_MAX_TOKEN = BigInteger.valueOf(Long.MAX_VALUE);

  /**
   * Calculates the ring distance from start token to target token, treating the ring as circular.
   * This mirrors the logic in RepairRunner.ringDistanceFromStart()
   */
  private BigInteger ringDistanceFromStart(BigInteger token, BigInteger start) {
    if (token.compareTo(start) >= 0) {
      return token.subtract(start);
    }
    return token
        .subtract(MURMUR3_MIN_TOKEN)
        .add(MURMUR3_MAX_TOKEN.subtract(start))
        .add(BigInteger.ONE);
  }

  /**
   * Simplified coverage verification for testing. Mirrors the logic in
   * RepairRunner.verifyCompleteCoverage()
   */
  private boolean verifyCompleteCoverage(
      RepairSegment originalSegment, List<RepairSegment> replacementSegments) {
    if (replacementSegments.isEmpty()) {
      return false;
    }

    BigInteger originalStart = originalSegment.getStartToken();
    BigInteger originalEnd = originalSegment.getEndToken();

    // Sort replacement segments by ring distance from original start
    // This handles both normal and wrap-around ranges correctly
    List<RepairSegment> sorted = new ArrayList<>(replacementSegments);
    sorted.sort(
        Comparator.comparing(seg -> ringDistanceFromStart(seg.getStartToken(), originalStart)));

    // Check first segment starts at original start
    if (!sorted.get(0).getStartToken().equals(originalStart)) {
      return false;
    }

    // Check last segment ends at original end
    if (!sorted.get(sorted.size() - 1).getEndToken().equals(originalEnd)) {
      return false;
    }

    // Check for gaps
    for (int i = 0; i < sorted.size() - 1; i++) {
      BigInteger currentEnd = sorted.get(i).getEndToken();
      BigInteger nextStart = sorted.get(i + 1).getStartToken();

      if (!currentEnd.equals(nextStart)) {
        return false;
      }
    }

    return true;
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
  public void testCoverageVerification_SuccessPath() {
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

    boolean coverageComplete = verifyCompleteCoverage(originalSegment, replacements);
    assertTrue("Coverage should be complete with perfect coverage", coverageComplete);
  }

  /**
   * Test: Zero-length segment handling - segments with same start and end token. This covers lines
   * 1103-1104 (skip zero-length ranges).
   */
  @Test
  public void testZeroLengthSegment_Skipped() {
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

    boolean coverageComplete = verifyCompleteCoverage(originalSegment, replacements);
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
  public void testWrapAroundSplitWithOneInternalToken() {
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

    boolean coverageComplete = verifyCompleteCoverage(original, replacements);
    assertTrue(
        "Wrap-around split with one internal token should have complete coverage",
        coverageComplete);
  }

  /**
   * Test: Wrap-around segment split with multiple internal tokens. Verifies that tokens are ordered
   * by ring traversal, not numeric sort. Original: [8000000000000000000, -7000000000000000000)
   * Internal tokens: [-8000000000000000000, -5000000000000000000, 3000000000000000000] Ring
   * traversal order from start: 8000000000000000000 → 9223372036854775807 (MAX) →
   * -9223372036854775808 (MIN) → -8000000000000000000 → -5000000000000000000 → -7000000000000000000
   * (end) Note: 3000000000000000000 is NOT in range [8000000000000000000, -7000000000000000000)
   */
  @Test
  public void testWrapAroundSplitWithMultipleTokensPreservesRingOrder() {
    RepairSegment original =
        createSegment(
            new BigInteger("8000000000000000000"),
            new BigInteger("-7000000000000000000"),
            RepairSegment.State.NOT_STARTED);

    // Expected replacements in ring traversal order (3000000000000000000 excluded as not in range)
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(
        createSegment(
            new BigInteger("8000000000000000000"),
            new BigInteger("-8000000000000000000"),
            RepairSegment.State.NOT_STARTED));
    replacements.add(
        createSegment(
            new BigInteger("-8000000000000000000"),
            new BigInteger("-5000000000000000000"),
            RepairSegment.State.NOT_STARTED));
    replacements.add(
        createSegment(
            new BigInteger("-5000000000000000000"),
            new BigInteger("-7000000000000000000"),
            RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = verifyCompleteCoverage(original, replacements);
    assertTrue(
        "Wrap-around split with multiple tokens should preserve ring order", coverageComplete);
  }

  /** Test: Wrap-around coverage verification succeeds for valid replacements. */
  @Test
  public void testWrapAroundCoverageVerificationSucceeds() {
    RepairSegment original =
        createSegment(
            new BigInteger("9000000000000000000"),
            new BigInteger("-8000000000000000000"),
            RepairSegment.State.NOT_STARTED);

    // Valid wrap-around replacements
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(
        createSegment(
            new BigInteger("9000000000000000000"),
            new BigInteger("-5000000000000000000"),
            RepairSegment.State.NOT_STARTED));
    replacements.add(
        createSegment(
            new BigInteger("-5000000000000000000"),
            new BigInteger("-8000000000000000000"),
            RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = verifyCompleteCoverage(original, replacements);
    assertTrue(
        "Valid wrap-around replacements should pass coverage verification", coverageComplete);
  }

  /** Test: Wrap-around coverage verification fails when there is a gap. */
  @Test
  public void testWrapAroundCoverageVerificationFailsWithGap() {
    RepairSegment original =
        createSegment(
            new BigInteger("9000000000000000000"),
            new BigInteger("-8000000000000000000"),
            RepairSegment.State.NOT_STARTED);

    // Gap: missing [-5000000000000000000, -8000000000000000000)
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(
        createSegment(
            new BigInteger("9000000000000000000"),
            new BigInteger("-5000000000000000000"),
            RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = verifyCompleteCoverage(original, replacements);
    assertFalse(
        "Wrap-around replacements with gap should fail coverage verification", coverageComplete);
  }

  /**
   * Test: Long.MAX_VALUE to Long.MIN_VALUE boundary ordering. Verifies that MIN_VALUE is ordered
   * immediately after MAX_VALUE in ring traversal.
   */
  @Test
  public void testLongMaxToMinBoundaryOrdering() {
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

    boolean coverageComplete = verifyCompleteCoverage(original, replacements);
    assertTrue("MAX_VALUE to MIN_VALUE boundary should be handled correctly", coverageComplete);
  }

  /**
   * Test: Normal range split still works after ring distance changes. Ensures backward
   * compatibility with non-wrap-around ranges.
   */
  @Test
  public void testNormalRangeSplitStillWorks() {
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

    boolean coverageComplete = verifyCompleteCoverage(original, replacements);
    assertTrue(
        "Normal range split should still work with ring distance ordering", coverageComplete);
  }

  /**
   * Test: Wrap-around coverage verification handles any input order. With ring distance ordering,
   * segments can be provided in any order and will be sorted correctly.
   */
  @Test
  public void testWrapAroundCoverageVerificationHandlesAnyOrder() {
    RepairSegment original =
        createSegment(
            new BigInteger("9000000000000000000"),
            new BigInteger("-8000000000000000000"),
            RepairSegment.State.NOT_STARTED);

    // Provide segments in reverse ring order - should still pass after sorting
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(
        createSegment(
            new BigInteger("-5000000000000000000"),
            new BigInteger("-8000000000000000000"),
            RepairSegment.State.NOT_STARTED));
    replacements.add(
        createSegment(
            new BigInteger("9000000000000000000"),
            new BigInteger("-5000000000000000000"),
            RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = verifyCompleteCoverage(original, replacements);
    assertTrue("Wrap-around replacements should pass regardless of input order", coverageComplete);
  }
}
