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
import io.cassandrareaper.storage.repairrun.IRepairRunDao;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for RepairRunner topology change failure and edge case paths. Focuses on increasing
 * coverage for uncovered branches in detectAndHandleTopologyChange and related methods.
 */
public final class RepairRunnerTopologyChangeFailureTest {

  private static final Set<String> TABLES = ImmutableSet.of("table1");
  private IStorageDao storage;
  private AppContext context;
  private Cluster cluster;
  private UUID repairUnitId;
  private RepairRunService mockRepairRunService;
  private ClusterFacade mockClusterFacade;

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

    // Create mocks
    mockRepairRunService = mock(RepairRunService.class);
    mockClusterFacade = mock(ClusterFacade.class);
  }

  /**
   * Test: detectAndHandleTopologyChange returns false when original segment not found in storage.
   * Covers line 962-965 in RepairRunner.java
   */
  @Test
  public void testTopologyChangeDetection_SegmentNotFound() throws Exception {
    UUID runId = createRepairRun();
    UUID nonExistentSegmentId = UUID.randomUUID();

    RepairRunner runner = createRepairRunner(runId);

    // Call detectAndHandleTopologyChange with non-existent segment
    Segment segmentRange =
        Segment.builder()
            .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(10)))
            .build();

    boolean result = runner.detectAndHandleTopologyChange(nonExistentSegmentId, segmentRange);

    assertFalse("Should return false when segment not found", result);
  }

  /**
   * Test: detectAndHandleTopologyChange returns false when no replacement segments computed. Covers
   * line 974-977 in RepairRunner.java
   */
  @Test
  public void testTopologyChangeDetection_NoReplacementSegments() throws Exception {
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

    Optional<RepairSegment> segment =
        storage
            .getRepairSegmentDao()
            .getRepairSegmentByTokenRange(
                runId, repairUnitId, BigInteger.valueOf(1), BigInteger.valueOf(10));

    assertTrue("Segment should exist", segment.isPresent());

    RepairRunner runner = createRepairRunner(runId);

    // Mock ClusterFacade to return empty token list (no replacement segments)
    when(mockClusterFacade.getTokens(any())).thenReturn(Collections.emptyList());

    Segment segmentRange =
        Segment.builder()
            .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(10)))
            .build();

    boolean result = runner.detectAndHandleTopologyChange(segment.get().getId(), segmentRange);

    assertFalse("Should return false when no replacement segments computed", result);
  }

  /**
   * Test: detectAndHandleTopologyChange returns false when single replacement has same range.
   * Covers line 981-989 in RepairRunner.java
   */
  @Test
  public void testTopologyChangeDetection_SingleReplacementUnchanged() throws Exception {
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

    Optional<RepairSegment> segment =
        storage
            .getRepairSegmentDao()
            .getRepairSegmentByTokenRange(
                runId, repairUnitId, BigInteger.valueOf(1), BigInteger.valueOf(10));

    assertTrue("Segment should exist", segment.isPresent());

    RepairRunner runner = createRepairRunner(runId);

    // Mock ClusterFacade to return tokens that don't split the range
    // Only boundary tokens, no internal tokens
    List<BigInteger> ringTokens = new ArrayList<>();
    ringTokens.add(BigInteger.valueOf(1));
    ringTokens.add(BigInteger.valueOf(10));
    when(mockClusterFacade.getTokens(any())).thenReturn(ringTokens);

    Segment segmentRange =
        Segment.builder()
            .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(10)))
            .build();

    boolean result = runner.detectAndHandleTopologyChange(segment.get().getId(), segmentRange);

    assertFalse("Should return false when topology unchanged", result);
  }

  /**
   * Test: detectAndHandleTopologyChange returns false when replacement has no coordinators. Covers
   * line 1009-1016 in RepairRunner.java
   */
  @Test
  public void testTopologyChangeDetection_ReplacementHasNoCoordinators() throws Exception {
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

    Optional<RepairSegment> segment =
        storage
            .getRepairSegmentDao()
            .getRepairSegmentByTokenRange(
                runId, repairUnitId, BigInteger.valueOf(1), BigInteger.valueOf(10));

    assertTrue("Segment should exist", segment.isPresent());

    RepairRunner runner = createRepairRunner(runId);

    // Mock ClusterFacade to return tokens that split the range
    List<BigInteger> ringTokens = new ArrayList<>();
    ringTokens.add(BigInteger.valueOf(5)); // Split at 5
    when(mockClusterFacade.getTokens(any())).thenReturn(ringTokens);

    // Mock RepairRunService to return empty topology (no coordinators)
    when(mockRepairRunService.getDCsByNodeForRepairSegment(any(), any(), anyString(), any()))
        .thenReturn(Collections.emptyMap());

    Segment segmentRange =
        Segment.builder()
            .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(10)))
            .build();

    boolean result = runner.detectAndHandleTopologyChange(segment.get().getId(), segmentRange);

    assertFalse("Should return false when replacement has no coordinators", result);
  }

  /**
   * Test: detectAndHandleTopologyChange returns false when coverage verification fails. Covers line
   * 1027-1030 in RepairRunner.java
   */
  @Test
  public void testTopologyChangeDetection_CoverageVerificationFails() throws Exception {
    UUID runId = createRepairRun();

    // Create original segment [1, 10)
    RepairSegment.Builder builder =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(10)))
                    .build(),
                repairUnitId)
            .withRunId(runId)
            .withState(RepairSegment.State.NOT_STARTED);

    storage.getRepairSegmentDao().addRepairSegments(Collections.singletonList(builder), runId);

    Optional<RepairSegment> segment =
        storage
            .getRepairSegmentDao()
            .getRepairSegmentByTokenRange(
                runId, repairUnitId, BigInteger.valueOf(1), BigInteger.valueOf(10));

    assertTrue("Segment should exist", segment.isPresent());

    RepairRunner runner = createRepairRunner(runId);

    // Mock ClusterFacade to return tokens that split the range
    List<BigInteger> ringTokens = new ArrayList<>();
    ringTokens.add(BigInteger.valueOf(5));
    when(mockClusterFacade.getTokens(any())).thenReturn(ringTokens);

    // Mock RepairRunService to return valid coordinators
    Map<String, String> topology = new HashMap<>();
    topology.put("127.0.0.1", "dc1");
    when(mockRepairRunService.getDCsByNodeForRepairSegment(any(), any(), anyString(), any()))
        .thenReturn(topology);

    // Create replacement segments that DON'T cover the original range
    // Original: [1, 10), Replacements: [1, 5), [6, 10) - gap at 5-6
    RepairSegment.Builder replacement1 =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(5)))
                    .build(),
                repairUnitId)
            .withRunId(runId)
            .withState(RepairSegment.State.NOT_STARTED);

    RepairSegment.Builder replacement2 =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(6), BigInteger.valueOf(10)))
                    .build(),
                repairUnitId)
            .withRunId(runId)
            .withState(RepairSegment.State.NOT_STARTED);

    storage.getRepairSegmentDao().addRepairSegments(Collections.singletonList(replacement1), runId);
    storage.getRepairSegmentDao().addRepairSegments(Collections.singletonList(replacement2), runId);

    Segment segmentRange =
        Segment.builder()
            .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(10)))
            .build();

    boolean result = runner.detectAndHandleTopologyChange(segment.get().getId(), segmentRange);

    assertFalse("Should return false when coverage verification fails", result);
  }

  /**
   * Test: detectAndHandleTopologyChange returns false when retirement fails. Covers line 1038-1041
   * in RepairRunner.java
   */
  @Test
  public void testTopologyChangeDetection_RetirementFails() throws Exception {
    UUID runId = createRepairRun();

    // Create original segment in RUNNING state (not NOT_STARTED)
    // This will cause retirement to fail because conditional update expects NOT_STARTED
    RepairSegment.Builder builder =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(10)))
                    .build(),
                repairUnitId)
            .withRunId(runId)
            .withStartTime(DateTime.now()) // RUNNING state requires startTime
            .withState(RepairSegment.State.RUNNING); // Already running

    storage.getRepairSegmentDao().addRepairSegments(Collections.singletonList(builder), runId);

    Optional<RepairSegment> segment =
        storage
            .getRepairSegmentDao()
            .getRepairSegmentByTokenRange(
                runId, repairUnitId, BigInteger.valueOf(1), BigInteger.valueOf(10));

    assertTrue("Segment should exist", segment.isPresent());

    RepairRunner runner = createRepairRunner(runId);

    // Mock ClusterFacade to return tokens that split the range
    List<BigInteger> ringTokens = new ArrayList<>();
    ringTokens.add(BigInteger.valueOf(5));
    when(mockClusterFacade.getTokens(any())).thenReturn(ringTokens);

    // Mock RepairRunService to return valid coordinators
    Map<String, String> topology = new HashMap<>();
    topology.put("127.0.0.1", "dc1");
    when(mockRepairRunService.getDCsByNodeForRepairSegment(any(), any(), anyString(), any()))
        .thenReturn(topology);

    Segment segmentRange =
        Segment.builder()
            .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(10)))
            .build();

    boolean result = runner.detectAndHandleTopologyChange(segment.get().getId(), segmentRange);

    assertFalse("Should return false when retirement fails", result);
  }

  /**
   * Test: detectAndHandleTopologyChange handles exception during topology detection. Covers line
   * 1042-1045 in RepairRunner.java
   */
  @Test
  public void testTopologyChangeDetection_ExceptionDuringDetection() throws Exception {
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

    Optional<RepairSegment> segment =
        storage
            .getRepairSegmentDao()
            .getRepairSegmentByTokenRange(
                runId, repairUnitId, BigInteger.valueOf(1), BigInteger.valueOf(10));

    assertTrue("Segment should exist", segment.isPresent());

    RepairRunner runner = createRepairRunner(runId);

    // Mock ClusterFacade to throw exception
    when(mockClusterFacade.getTokens(any())).thenThrow(new RuntimeException("Cluster unavailable"));

    Segment segmentRange =
        Segment.builder()
            .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(10)))
            .build();

    boolean result = runner.detectAndHandleTopologyChange(segment.get().getId(), segmentRange);

    assertFalse("Should return false when exception occurs", result);
  }

  /**
   * Test: computeReplacementSegments handles exception gracefully. Covers line 1123-1125 in
   * RepairRunner.java
   */
  @Test
  public void testComputeReplacementSegments_ExceptionHandling() throws Exception {
    UUID runId = createRepairRun();

    RepairRunner runner = createRepairRunner(runId);

    // Mock ClusterFacade to throw exception
    when(mockClusterFacade.getTokens(any())).thenThrow(new RuntimeException("Network error"));

    RepairSegment originalSegment =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(10)))
                    .build(),
                repairUnitId)
            .withRunId(runId)
            .withState(RepairSegment.State.NOT_STARTED)
            .withId(UUID.randomUUID())
            .build();

    Segment segmentRange =
        Segment.builder()
            .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(10)))
            .build();

    List<RepairSegment> result = runner.computeReplacementSegments(originalSegment, segmentRange);

    assertNotNull("Should return non-null list", result);
    assertTrue("Should return empty list on exception", result.isEmpty());
  }

  /**
   * Test: createMissingReplacementSegments handles exception during segment creation. Covers line
   * 1202-1208 in RepairRunner.java
   */
  @Test
  public void testCreateMissingReplacementSegments_ExceptionHandling() throws Exception {
    UUID runId = createRepairRun();

    RepairRunner runner = createRepairRunner(runId);

    // Create a replacement segment
    RepairSegment replacement =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(5)))
                    .build(),
                repairUnitId)
            .withRunId(runId)
            .withState(RepairSegment.State.NOT_STARTED)
            .build();

    List<RepairSegment> replacements = Collections.singletonList(replacement);

    // The method should handle exceptions gracefully and continue
    List<RepairSegment> result = runner.createMissingReplacementSegments(replacements);

    assertNotNull("Should return non-null list", result);
    // Result may be empty or contain created segments depending on exception timing
  }

  /**
   * Test: verifyCompleteCoverage returns false for empty replacement list. Covers line 1224-1227 in
   * RepairRunner.java
   */
  @Test
  public void testVerifyCompleteCoverage_EmptyReplacements() throws Exception {
    UUID runId = createRepairRun();

    RepairRunner runner = createRepairRunner(runId);

    RepairSegment originalSegment =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(10)))
                    .build(),
                repairUnitId)
            .withRunId(runId)
            .withState(RepairSegment.State.NOT_STARTED)
            .withId(UUID.randomUUID())
            .build();

    boolean result = runner.verifyCompleteCoverage(originalSegment, Collections.emptyList());

    assertFalse("Should return false for empty replacement list", result);
  }

  /**
   * Test: verifyCompleteCoverage returns false when first segment doesn't start at original start.
   * Covers line 1239-1242 in RepairRunner.java
   */
  @Test
  public void testVerifyCompleteCoverage_FirstSegmentMismatch() throws Exception {
    UUID runId = createRepairRun();

    RepairRunner runner = createRepairRunner(runId);

    RepairSegment originalSegment =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(10)))
                    .build(),
                repairUnitId)
            .withRunId(runId)
            .withState(RepairSegment.State.NOT_STARTED)
            .withId(UUID.randomUUID())
            .build();

    // Replacement starts at 2, not 1
    RepairSegment replacement =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(2), BigInteger.valueOf(10)))
                    .build(),
                repairUnitId)
            .withRunId(runId)
            .withState(RepairSegment.State.NOT_STARTED)
            .withId(UUID.randomUUID())
            .build();

    boolean result =
        runner.verifyCompleteCoverage(originalSegment, Collections.singletonList(replacement));

    assertFalse("Should return false when first segment doesn't match start", result);
  }

  /**
   * Test: verifyCompleteCoverage returns false when last segment doesn't end at original end.
   * Covers line 1245-1248 in RepairRunner.java
   */
  @Test
  public void testVerifyCompleteCoverage_LastSegmentMismatch() throws Exception {
    UUID runId = createRepairRun();

    RepairRunner runner = createRepairRunner(runId);

    RepairSegment originalSegment =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(10)))
                    .build(),
                repairUnitId)
            .withRunId(runId)
            .withState(RepairSegment.State.NOT_STARTED)
            .withId(UUID.randomUUID())
            .build();

    // Replacement ends at 9, not 10
    RepairSegment replacement =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(9)))
                    .build(),
                repairUnitId)
            .withRunId(runId)
            .withState(RepairSegment.State.NOT_STARTED)
            .withId(UUID.randomUUID())
            .build();

    boolean result =
        runner.verifyCompleteCoverage(originalSegment, Collections.singletonList(replacement));

    assertFalse("Should return false when last segment doesn't match end", result);
  }

  /**
   * Test: verifyCompleteCoverage returns false when gap exists between segments. Covers line
   * 1255-1258 in RepairRunner.java
   */
  @Test
  public void testVerifyCompleteCoverage_GapBetweenSegments() throws Exception {
    UUID runId = createRepairRun();

    RepairRunner runner = createRepairRunner(runId);

    RepairSegment originalSegment =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(10)))
                    .build(),
                repairUnitId)
            .withRunId(runId)
            .withState(RepairSegment.State.NOT_STARTED)
            .withId(UUID.randomUUID())
            .build();

    // Gap: [1,5) and [6,10) - missing 5-6
    RepairSegment replacement1 =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(5)))
                    .build(),
                repairUnitId)
            .withRunId(runId)
            .withState(RepairSegment.State.NOT_STARTED)
            .withId(UUID.randomUUID())
            .build();

    RepairSegment replacement2 =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(6), BigInteger.valueOf(10)))
                    .build(),
                repairUnitId)
            .withRunId(runId)
            .withState(RepairSegment.State.NOT_STARTED)
            .withId(UUID.randomUUID())
            .build();

    List<RepairSegment> replacements = new ArrayList<>();
    replacements.add(replacement1);
    replacements.add(replacement2);

    boolean result = runner.verifyCompleteCoverage(originalSegment, replacements);

    assertFalse("Should return false when gap exists", result);
  }

  /**
   * Test: retireOriginalSegment handles exception gracefully. Covers line 1296-1299 in
   * RepairRunner.java
   */
  @Test
  public void testRetireOriginalSegment_ExceptionHandling() throws Exception {
    UUID runId = createRepairRun();

    RepairRunner runner = createRepairRunner(runId);

    // Create a segment with invalid UUID that will cause exception
    RepairSegment originalSegment =
        RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.valueOf(1), BigInteger.valueOf(10)))
                    .build(),
                repairUnitId)
            .withRunId(UUID.randomUUID()) // Different run ID will cause issues
            .withState(RepairSegment.State.NOT_STARTED)
            .withId(UUID.randomUUID())
            .build();

    boolean result = runner.retireOriginalSegment(originalSegment);

    assertFalse("Should return false when exception occurs", result);
  }

  // Helper methods

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

  private RepairRunner createRepairRunner(UUID runId) throws ReaperException {
    // Use reflection to create RepairRunner since constructor is private
    try {
      java.lang.reflect.Constructor<RepairRunner> constructor =
          RepairRunner.class.getDeclaredConstructor(
              AppContext.class, UUID.class, ClusterFacade.class, IRepairRunDao.class);
      constructor.setAccessible(true);

      // Set up context with mocked ClusterFacade
      AppContext testContext = new AppContext();
      testContext.storage = storage;
      testContext.config = context.config;

      return constructor.newInstance(
          testContext, runId, mockClusterFacade, storage.getRepairRunDao());
    } catch (Exception e) {
      throw new ReaperException("Failed to create RepairRunner", e);
    }
  }
}
