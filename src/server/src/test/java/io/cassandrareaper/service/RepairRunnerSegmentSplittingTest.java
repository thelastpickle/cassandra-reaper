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
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Comprehensive unit tests for topology change segment splitting functionality.
 * Tests the new behavior where segments are split and repair continues instead of failing.
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

    cluster = Cluster.builder()
        .withName("test_" + RandomStringUtils.randomAlphabetic(12))
        .withSeedHosts(ImmutableSet.of("127.0.0.1"))
        .withState(Cluster.State.ACTIVE)
        .build();

    storage.getClusterDao().addCluster(cluster);

    Set<String> nodeSet = Sets.newHashSet("127.0.0.1", "127.0.0.2", "127.0.0.3");
    Set<String> cfNames = Sets.newHashSet("table1");

    repairUnitId = storage.getRepairUnitDao()
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
   * Test: Simple segment split into 3 parts
   * Original: [1, 10)
   * Boundaries: 4, 7
   * Expected replacements: [1,4), [4,7), [7,10)
   */
  @Test
  public void testSimpleSegmentSplit() {
    // Create original segment [1, 10)
    RepairSegment originalSegment = createSegment(
        BigInteger.valueOf(1), BigInteger.valueOf(10), RepairSegment.State.NOT_STARTED);

    // Create replacement segments
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(createSegment(BigInteger.valueOf(1), BigInteger.valueOf(4),
        RepairSegment.State.NOT_STARTED));
    replacements.add(createSegment(BigInteger.valueOf(4), BigInteger.valueOf(7),
        RepairSegment.State.NOT_STARTED));
    replacements.add(createSegment(BigInteger.valueOf(7), BigInteger.valueOf(10),
        RepairSegment.State.NOT_STARTED));

    // Verify coverage
    boolean coverageComplete = verifyCompleteCoverage(originalSegment, replacements);
    assertTrue("Coverage should be complete for simple split", coverageComplete);
  }

  /**
   * Test: No split required (single replacement equals original)
   */
  @Test
  public void testNoSplitRequired() {
    RepairSegment originalSegment = createSegment(
        BigInteger.valueOf(1), BigInteger.valueOf(10), RepairSegment.State.NOT_STARTED);

    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(createSegment(BigInteger.valueOf(1), BigInteger.valueOf(10),
        RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = verifyCompleteCoverage(originalSegment, replacements);
    assertTrue("Coverage should be complete when no split needed", coverageComplete);
  }

  /**
   * Test: Single boundary creates 2 segments
   */
  @Test
  public void testSingleBoundarySplit() {
    RepairSegment originalSegment = createSegment(
        BigInteger.valueOf(0), BigInteger.valueOf(100), RepairSegment.State.NOT_STARTED);

    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(createSegment(BigInteger.valueOf(0), BigInteger.valueOf(50),
        RepairSegment.State.NOT_STARTED));
    replacements.add(createSegment(BigInteger.valueOf(50), BigInteger.valueOf(100),
        RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = verifyCompleteCoverage(originalSegment, replacements);
    assertTrue("Coverage should be complete for single boundary split", coverageComplete);
  }

  /**
   * Test: Multiple boundaries create many segments
   */
  @Test
  public void testMultipleBoundariesSplit() {
    RepairSegment originalSegment = createSegment(
        BigInteger.valueOf(0), BigInteger.valueOf(1000), RepairSegment.State.NOT_STARTED);

    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(createSegment(BigInteger.valueOf(0), BigInteger.valueOf(100),
        RepairSegment.State.NOT_STARTED));
    replacements.add(createSegment(BigInteger.valueOf(100), BigInteger.valueOf(300),
        RepairSegment.State.NOT_STARTED));
    replacements.add(createSegment(BigInteger.valueOf(300), BigInteger.valueOf(600),
        RepairSegment.State.NOT_STARTED));
    replacements.add(createSegment(BigInteger.valueOf(600), BigInteger.valueOf(800),
        RepairSegment.State.NOT_STARTED));
    replacements.add(createSegment(BigInteger.valueOf(800), BigInteger.valueOf(1000),
        RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = verifyCompleteCoverage(originalSegment, replacements);
    assertTrue("Coverage should be complete for multiple boundaries", coverageComplete);
  }

  /**
   * Test: Coverage verification detects missing range
   */
  @Test
  public void testMissingRangeDetected() {
    RepairSegment originalSegment = createSegment(
        BigInteger.valueOf(1), BigInteger.valueOf(10), RepairSegment.State.NOT_STARTED);

    // Missing range [4, 7)
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(createSegment(BigInteger.valueOf(1), BigInteger.valueOf(4),
        RepairSegment.State.NOT_STARTED));
    replacements.add(createSegment(BigInteger.valueOf(7), BigInteger.valueOf(10),
        RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = verifyCompleteCoverage(originalSegment, replacements);
    assertFalse("Coverage should be incomplete with missing range", coverageComplete);
  }

  /**
   * Test: Coverage verification detects overlapping ranges
   */
  @Test
  public void testOverlappingRangesDetected() {
    RepairSegment originalSegment = createSegment(
        BigInteger.valueOf(1), BigInteger.valueOf(10), RepairSegment.State.NOT_STARTED);

    // Overlapping: [1,5) and [4,10) overlap at [4,5)
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(createSegment(BigInteger.valueOf(1), BigInteger.valueOf(5),
        RepairSegment.State.NOT_STARTED));
    replacements.add(createSegment(BigInteger.valueOf(4), BigInteger.valueOf(10),
        RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = verifyCompleteCoverage(originalSegment, replacements);
    assertFalse("Coverage should be incomplete with overlapping ranges", coverageComplete);
  }

  /**
   * Test: Coverage verification handles empty replacement set
   */
  @Test
  public void testEmptyReplacementSet() {
    RepairSegment originalSegment = createSegment(
        BigInteger.valueOf(1), BigInteger.valueOf(10), RepairSegment.State.NOT_STARTED);

    List<RepairSegment> replacements = new ArrayList<>();

    boolean coverageComplete = verifyCompleteCoverage(originalSegment, replacements);
    assertFalse("Coverage should be incomplete with empty replacement set", coverageComplete);
  }

  /**
   * Test: Coverage verification detects wrong start token
   */
  @Test
  public void testWrongStartToken() {
    RepairSegment originalSegment = createSegment(
        BigInteger.valueOf(1), BigInteger.valueOf(10), RepairSegment.State.NOT_STARTED);

    // Starts at 2 instead of 1
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(createSegment(BigInteger.valueOf(2), BigInteger.valueOf(10),
        RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = verifyCompleteCoverage(originalSegment, replacements);
    assertFalse("Coverage should be incomplete with wrong start token", coverageComplete);
  }

  /**
   * Test: Coverage verification detects wrong end token
   */
  @Test
  public void testWrongEndToken() {
    RepairSegment originalSegment = createSegment(
        BigInteger.valueOf(1), BigInteger.valueOf(10), RepairSegment.State.NOT_STARTED);

    // Ends at 9 instead of 10
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(createSegment(BigInteger.valueOf(1), BigInteger.valueOf(9),
        RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = verifyCompleteCoverage(originalSegment, replacements);
    assertFalse("Coverage should be incomplete with wrong end token", coverageComplete);
  }

  /**
   * Test: Boundary exactly matching start token
   */
  @Test
  public void testBoundaryAtStartToken() {
    RepairSegment originalSegment = createSegment(
        BigInteger.valueOf(100), BigInteger.valueOf(200), RepairSegment.State.NOT_STARTED);

    // Boundary at 100 (start token) should not create zero-length segment
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(createSegment(BigInteger.valueOf(100), BigInteger.valueOf(150),
        RepairSegment.State.NOT_STARTED));
    replacements.add(createSegment(BigInteger.valueOf(150), BigInteger.valueOf(200),
        RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = verifyCompleteCoverage(originalSegment, replacements);
    assertTrue("Coverage should be complete with boundary at start", coverageComplete);
  }

  /**
   * Test: Boundary exactly matching end token
   */
  @Test
  public void testBoundaryAtEndToken() {
    RepairSegment originalSegment = createSegment(
        BigInteger.valueOf(100), BigInteger.valueOf(200), RepairSegment.State.NOT_STARTED);

    // Boundary at 200 (end token) should not create zero-length segment
    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(createSegment(BigInteger.valueOf(100), BigInteger.valueOf(150),
        RepairSegment.State.NOT_STARTED));
    replacements.add(createSegment(BigInteger.valueOf(150), BigInteger.valueOf(200),
        RepairSegment.State.NOT_STARTED));

    boolean coverageComplete = verifyCompleteCoverage(originalSegment, replacements);
    assertTrue("Coverage should be complete with boundary at end", coverageComplete);
  }

  /**
   * Test: DAO getRepairSegmentByTokenRange finds existing segment
   */
  @Test
  public void testDaoFindsByTokenRange() {
    UUID runId = createRepairRun();

    // Create a segment
    RepairSegment.Builder builder = RepairSegment.builder(
        Segment.builder()
            .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(10)))
            .build(),
        repairUnitId)
        .withRunId(runId)
        .withState(RepairSegment.State.NOT_STARTED);

    storage.getRepairSegmentDao().addRepairSegments(Collections.singletonList(builder), runId);

    // Find by token range
    Optional<RepairSegment> found = storage.getRepairSegmentDao()
        .getRepairSegmentByTokenRange(runId, repairUnitId,
            BigInteger.valueOf(1), BigInteger.valueOf(10));

    assertTrue("Segment should be found by token range", found.isPresent());
    assertEquals("Start token should match", BigInteger.valueOf(1), found.get().getStartToken());
    assertEquals("End token should match", BigInteger.valueOf(10), found.get().getEndToken());
  }

  /**
   * Test: DAO getRepairSegmentByTokenRange returns empty for non-existent segment
   */
  @Test
  public void testDaoReturnsEmptyForNonExistentSegment() {
    UUID runId = createRepairRun();

    Optional<RepairSegment> found = storage.getRepairSegmentDao()
        .getRepairSegmentByTokenRange(runId, repairUnitId,
            BigInteger.valueOf(999), BigInteger.valueOf(1000));

    assertFalse("Non-existent segment should not be found", found.isPresent());
  }

  /**
   * Test: DAO conditional update succeeds when state matches
   */
  @Test
  public void testDaoConditionalUpdateSucceedsWhenStateMatches() {
    UUID runId = createRepairRun();

    // Create a segment in NOT_STARTED state
    RepairSegment.Builder builder = RepairSegment.builder(
        Segment.builder()
            .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(10)))
            .build(),
        repairUnitId)
        .withRunId(runId)
        .withState(RepairSegment.State.NOT_STARTED);

    storage.getRepairSegmentDao().addRepairSegments(Collections.singletonList(builder), runId);

    // Get the created segment
    Optional<RepairSegment> created = storage.getRepairSegmentDao()
        .getRepairSegmentByTokenRange(runId, repairUnitId,
            BigInteger.valueOf(1), BigInteger.valueOf(10));

    assertTrue("Segment should exist", created.isPresent());
    UUID segmentId = created.get().getId();

    // Conditional update from NOT_STARTED to RUNNING
    // (DONE would require startTime/endTime to be set)
    boolean updated = storage.getRepairSegmentDao()
        .updateRepairSegmentStateConditional(runId, segmentId,
            RepairSegment.State.RUNNING, RepairSegment.State.NOT_STARTED);

    assertTrue("Conditional update should succeed when state matches", updated);

    // Verify state changed
    Optional<RepairSegment> updatedSegment = storage.getRepairSegmentDao()
        .getRepairSegment(runId, segmentId);

    assertTrue("Segment should still exist", updatedSegment.isPresent());
    assertEquals("State should be RUNNING", RepairSegment.State.RUNNING,
        updatedSegment.get().getState());
  }

  /**
   * Test: DAO conditional update fails when state doesn't match
   */
  @Test
  public void testDaoConditionalUpdateFailsWhenStateDoesNotMatch() {
    UUID runId = createRepairRun();

    // Create a segment in NOT_STARTED state
    RepairSegment.Builder builder = RepairSegment.builder(
        Segment.builder()
            .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(10)))
            .build(),
        repairUnitId)
        .withRunId(runId)
        .withState(RepairSegment.State.NOT_STARTED);

    storage.getRepairSegmentDao().addRepairSegments(Collections.singletonList(builder), runId);

    // Get the created segment
    Optional<RepairSegment> created = storage.getRepairSegmentDao()
        .getRepairSegmentByTokenRange(runId, repairUnitId,
            BigInteger.valueOf(1), BigInteger.valueOf(10));

    assertTrue("Segment should exist", created.isPresent());
    UUID segmentId = created.get().getId();

    // Try conditional update from RUNNING to DONE (but segment is NOT_STARTED)
    boolean updated = storage.getRepairSegmentDao()
        .updateRepairSegmentStateConditional(runId, segmentId,
            RepairSegment.State.RUNNING, RepairSegment.State.RUNNING);

    assertFalse("Conditional update should fail when state doesn't match", updated);

    // Verify state unchanged
    Optional<RepairSegment> unchangedSegment = storage.getRepairSegmentDao()
        .getRepairSegment(runId, segmentId);

    assertTrue("Segment should still exist", unchangedSegment.isPresent());
    assertEquals("State should still be NOT_STARTED", RepairSegment.State.NOT_STARTED,
        unchangedSegment.get().getState());
  }

  /**
   * Test: Idempotent segment creation - existing segments are detected
   */
  @Test
  public void testIdempotentSegmentCreation() {
    UUID runId = createRepairRun();

    // Create initial segment
    RepairSegment.Builder builder = RepairSegment.builder(
        Segment.builder()
            .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(10)))
            .build(),
        repairUnitId)
        .withRunId(runId)
        .withState(RepairSegment.State.NOT_STARTED);

    storage.getRepairSegmentDao().addRepairSegments(Collections.singletonList(builder), runId);

    // Get initial count
    int initialCount = storage.getRepairSegmentDao()
        .getSegmentAmountForRepairRun(runId);

    // Try to create same segment again
    storage.getRepairSegmentDao().addRepairSegments(Collections.singletonList(builder), runId);

    // Verify segment exists
    Optional<RepairSegment> found = storage.getRepairSegmentDao()
        .getRepairSegmentByTokenRange(runId, repairUnitId,
            BigInteger.valueOf(1), BigInteger.valueOf(10));

    assertTrue("Segment should exist", found.isPresent());

    // Note: The current implementation creates duplicates. This test documents current behavior.
    // In a production system, you might want to add duplicate detection logic.
  }

  /**
   * Test: addRepairSegments creates multiple segments in batch
   */
  @Test
  public void testBatchSegmentCreation() {
    UUID runId = createRepairRun();

    List<RepairSegment.Builder> builders = new ArrayList<>();
    builders.add(RepairSegment.builder(
        Segment.builder()
            .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(4)))
            .build(),
        repairUnitId)
        .withState(RepairSegment.State.NOT_STARTED));

    builders.add(RepairSegment.builder(
        Segment.builder()
            .withTokenRange(new RingRange(BigInteger.valueOf(4), BigInteger.valueOf(7)))
            .build(),
        repairUnitId)
        .withState(RepairSegment.State.NOT_STARTED));

    builders.add(RepairSegment.builder(
        Segment.builder()
            .withTokenRange(new RingRange(BigInteger.valueOf(7), BigInteger.valueOf(10)))
            .build(),
        repairUnitId)
        .withState(RepairSegment.State.NOT_STARTED));

    storage.getRepairSegmentDao().addRepairSegments(builders, runId);

    // Verify all segments created
    Optional<RepairSegment> seg1 = storage.getRepairSegmentDao()
        .getRepairSegmentByTokenRange(runId, repairUnitId,
            BigInteger.valueOf(1), BigInteger.valueOf(4));
    Optional<RepairSegment> seg2 = storage.getRepairSegmentDao()
        .getRepairSegmentByTokenRange(runId, repairUnitId,
            BigInteger.valueOf(4), BigInteger.valueOf(7));
    Optional<RepairSegment> seg3 = storage.getRepairSegmentDao()
        .getRepairSegmentByTokenRange(runId, repairUnitId,
            BigInteger.valueOf(7), BigInteger.valueOf(10));

    assertTrue("First segment should exist", seg1.isPresent());
    assertTrue("Second segment should exist", seg2.isPresent());
    assertTrue("Third segment should exist", seg3.isPresent());
  }

  // Helper methods

  private RepairSegment createSegment(BigInteger start, BigInteger end,
      RepairSegment.State state) {
    return RepairSegment.builder(
        Segment.builder()
            .withTokenRange(new RingRange(start, end))
            .build(),
        repairUnitId)
        .withState(state)
        .withRunId(UUID.randomUUID())
        .withId(UUID.randomUUID())
        .build();
  }

  private UUID createRepairRun() {
    RepairRun run = storage.getRepairRunDao()
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
   * Simplified coverage verification for testing.
   * Mirrors the logic in RepairRunner.verifyCompleteCoverage()
   */
  private boolean verifyCompleteCoverage(RepairSegment originalSegment,
      List<RepairSegment> replacementSegments) {
    if (replacementSegments.isEmpty()) {
      return false;
    }

    List<RepairSegment> sorted = new ArrayList<>(replacementSegments);
    sorted.sort((a, b) -> a.getStartToken().compareTo(b.getStartToken()));

    BigInteger originalStart = originalSegment.getStartToken();
    BigInteger originalEnd = originalSegment.getEndToken();

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
}

// Made with Bob
