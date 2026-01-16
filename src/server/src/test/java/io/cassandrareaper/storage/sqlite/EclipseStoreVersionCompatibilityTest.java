/*
 * Copyright 2025-2025 DataStax, Inc.
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

package io.cassandrareaper.storage.sqlite;

import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.storage.memory.MemoryStorageRoot;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.eclipse.serializer.persistence.types.PersistenceFieldEvaluator;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test to verify EclipseStore version compatibility and Guava ImmutableSet behavior.
 *
 * <p>This test investigates: 1. Can EclipseStore 2.1.3 read data it writes? 2. Does Guava
 * ImmutableSet survive serialization/deserialization? 3. What happens when we try to iterate the
 * deserialized ImmutableSet?
 */
public class EclipseStoreVersionCompatibilityTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(EclipseStoreVersionCompatibilityTest.class);

  /**
   * Field evaluator that production uses to handle Guava collections. This tells EclipseStore to
   * persist transient fields (except those starting with "_").
   */
  private static final PersistenceFieldEvaluator TRANSIENT_FIELD_EVALUATOR =
      (clazz, field) -> !field.getName().startsWith("_");

  private Path tempDir;

  @BeforeEach
  void setUp() throws Exception {
    tempDir = Files.createTempDirectory("eclipsestore-compat-test");
    LOG.info("Created temp directory: {}", tempDir);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (tempDir != null && Files.exists(tempDir)) {
      Files.walk(tempDir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }
  }

  /** Test 1: Verify basic EclipseStore write/read with current version (2.1.3) */
  @Test
  void testBasicWriteRead() {
    LOG.info("=== Test 1: Basic EclipseStore Write/Read ===");

    // Write data using production configuration
    EmbeddedStorageManager writeStorage =
        EmbeddedStorage.Foundation(tempDir)
            .onConnectionFoundation(c -> c.setFieldEvaluatorPersistable(TRANSIENT_FIELD_EVALUATOR))
            .createEmbeddedStorageManager();
    writeStorage.start();
    try {
      MemoryStorageRoot root = new MemoryStorageRoot();

      // Create a cluster with HashSet (mutable) for seedHosts
      Set<String> seedHosts = Sets.newHashSet("host1.example.com", "host2.example.com");
      Cluster cluster =
          Cluster.builder()
              .withName("test-cluster")
              .withSeedHosts(seedHosts)
              .withState(Cluster.State.ACTIVE)
              .build();

      root.getClusters().put(cluster.getName(), cluster);
      writeStorage.setRoot(root);
      writeStorage.storeRoot();

      LOG.info("Wrote cluster with seedHosts: {}", seedHosts);
    } finally {
      writeStorage.shutdown();
    }

    // Read data back using production configuration
    EmbeddedStorageManager readStorage =
        EmbeddedStorage.Foundation(tempDir)
            .onConnectionFoundation(c -> c.setFieldEvaluatorPersistable(TRANSIENT_FIELD_EVALUATOR))
            .createEmbeddedStorageManager();
    readStorage.start();
    try {
      Object root = readStorage.root();
      assertNotNull(root, "Root should not be null");
      assertTrue(root instanceof MemoryStorageRoot, "Root should be MemoryStorageRoot");

      MemoryStorageRoot memRoot = (MemoryStorageRoot) root;
      Cluster readCluster = memRoot.getClusters().get("test-cluster");
      assertNotNull(readCluster, "Cluster should be readable");

      Set<String> readSeedHosts = readCluster.getSeedHosts();
      LOG.info("Read seedHosts type: {}", readSeedHosts.getClass().getName());
      LOG.info("Read seedHosts: {}", readSeedHosts);

      // Try to iterate - this is where corruption would show
      int count = 0;
      try {
        for (String host : readSeedHosts) {
          LOG.info("  Host: {}", host);
          count++;
        }
        LOG.info("Successfully iterated {} hosts", count);
      } catch (NullPointerException e) {
        LOG.error("NPE when iterating seedHosts - this indicates Guava corruption!", e);
        fail("NullPointerException when iterating seedHosts");
      }

      assertEquals(2, count, "Should have 2 hosts");
      assertTrue(readSeedHosts.contains("host1.example.com"));
      assertTrue(readSeedHosts.contains("host2.example.com"));

    } finally {
      readStorage.shutdown();
    }

    LOG.info("=== Test 1 PASSED ===\n");
  }

  /** Test 2: Test with ImmutableSet specifically (this is what Cluster.builder creates) */
  @Test
  void testImmutableSetSerialization() {
    LOG.info("=== Test 2: ImmutableSet Serialization ===");

    // Write data with ImmutableSet using production configuration
    EmbeddedStorageManager writeStorage =
        EmbeddedStorage.Foundation(tempDir)
            .onConnectionFoundation(c -> c.setFieldEvaluatorPersistable(TRANSIENT_FIELD_EVALUATOR))
            .createEmbeddedStorageManager();
    writeStorage.start();
    try {
      MemoryStorageRoot root = new MemoryStorageRoot();

      // Create cluster - the builder converts to ImmutableSet
      Cluster cluster =
          Cluster.builder()
              .withName("immutable-test")
              .withSeedHosts(
                  ImmutableSet.of("seed1.example.com", "seed2.example.com", "seed3.example.com"))
              .withState(Cluster.State.ACTIVE)
              .build();

      LOG.info("Original seedHosts type: {}", cluster.getSeedHosts().getClass().getName());
      LOG.info("Original seedHosts: {}", cluster.getSeedHosts());

      root.getClusters().put(cluster.getName(), cluster);
      writeStorage.setRoot(root);
      writeStorage.storeRoot();

    } finally {
      writeStorage.shutdown();
    }

    // Read back and verify using production configuration
    EmbeddedStorageManager readStorage =
        EmbeddedStorage.Foundation(tempDir)
            .onConnectionFoundation(c -> c.setFieldEvaluatorPersistable(TRANSIENT_FIELD_EVALUATOR))
            .createEmbeddedStorageManager();
    readStorage.start();
    try {
      MemoryStorageRoot memRoot = (MemoryStorageRoot) readStorage.root();
      Cluster readCluster = memRoot.getClusters().get("immutable-test");

      Set<String> readSeedHosts = readCluster.getSeedHosts();
      LOG.info("Deserialized seedHosts type: {}", readSeedHosts.getClass().getName());

      // Critical test: can we call size()?
      try {
        int size = readSeedHosts.size();
        LOG.info("size() returned: {}", size);
      } catch (NullPointerException e) {
        LOG.error("NPE on size() - Guava ImmutableSet is CORRUPTED!", e);
        fail("size() threw NPE - ImmutableSet corrupted");
      }

      // Critical test: can we iterate?
      try {
        for (String host : readSeedHosts) {
          LOG.info("  Iterated host: {}", host);
        }
      } catch (NullPointerException e) {
        LOG.error("NPE on iteration - Guava ImmutableSet is CORRUPTED!", e);
        fail("Iteration threw NPE - ImmutableSet corrupted");
      }

      // Critical test: can we call contains()?
      try {
        boolean contains = readSeedHosts.contains("seed1.example.com");
        LOG.info("contains() returned: {}", contains);
        assertTrue(contains);
      } catch (NullPointerException e) {
        LOG.error("NPE on contains() - Guava ImmutableSet is CORRUPTED!", e);
        fail("contains() threw NPE - ImmutableSet corrupted");
      }

      assertEquals(3, readSeedHosts.size());

    } finally {
      readStorage.shutdown();
    }

    LOG.info("=== Test 2 PASSED ===\n");
  }

  /** Test 3: Simulate the migration scenario - write, close, reopen with new instance */
  @Test
  void testMigrationScenario() {
    LOG.info("=== Test 3: Migration Scenario (simulate upgrade) ===");

    // Step 1: Create data (simulating old Reaper version)
    LOG.info("Step 1: Creating data...");
    EmbeddedStorageManager storage1 =
        EmbeddedStorage.Foundation(tempDir)
            .onConnectionFoundation(c -> c.setFieldEvaluatorPersistable(TRANSIENT_FIELD_EVALUATOR))
            .createEmbeddedStorageManager();
    storage1.start();
    try {
      MemoryStorageRoot root = new MemoryStorageRoot();

      // Create multiple clusters with various data
      for (int i = 1; i <= 3; i++) {
        Cluster cluster =
            Cluster.builder()
                .withName("cluster-" + i)
                .withSeedHosts(
                    ImmutableSet.of("node" + i + "a.example.com", "node" + i + "b.example.com"))
                .withState(Cluster.State.ACTIVE)
                .build();
        root.getClusters().put(cluster.getName(), cluster);
      }

      storage1.setRoot(root);
      storage1.storeRoot();
      LOG.info("Created {} clusters", root.getClusters().size());

    } finally {
      storage1.shutdown();
      LOG.info("Storage closed");
    }

    // Step 2: Reopen (simulating new Reaper version after upgrade)
    LOG.info("Step 2: Reopening storage (simulating upgrade)...");
    EmbeddedStorageManager storage2 =
        EmbeddedStorage.Foundation(tempDir)
            .onConnectionFoundation(c -> c.setFieldEvaluatorPersistable(TRANSIENT_FIELD_EVALUATOR))
            .createEmbeddedStorageManager();
    storage2.start();
    try {
      Object root = storage2.root();

      if (root == null) {
        LOG.error("ROOT IS NULL! EclipseStore could not read the data.");
        LOG.error("This means EclipseStore 2.x CANNOT read its own data after restart!");
        fail("Root is null - EclipseStore data not readable");
        return;
      }

      LOG.info("Root loaded successfully, type: {}", root.getClass().getName());

      MemoryStorageRoot memRoot = (MemoryStorageRoot) root;
      LOG.info("Found {} clusters", memRoot.getClusters().size());

      // Step 3: Try to access data (this is where migration would fail)
      LOG.info("Step 3: Accessing cluster data...");
      for (Cluster cluster : memRoot.getClusters().values()) {
        LOG.info("Cluster: {}", cluster.getName());

        Set<String> seedHosts = cluster.getSeedHosts();
        LOG.info("  SeedHosts type: {}", seedHosts.getClass().getName());

        try {
          // These operations would fail if Guava ImmutableSet is corrupted
          LOG.info("  SeedHosts size: {}", seedHosts.size());
          LOG.info("  SeedHosts: {}", seedHosts);

          // Simulate what migration does: convert to JSON
          StringBuilder json = new StringBuilder("[");
          boolean first = true;
          for (String host : seedHosts) {
            if (!first) json.append(",");
            json.append("\"").append(host).append("\"");
            first = false;
          }
          json.append("]");
          LOG.info("  JSON: {}", json);

        } catch (NullPointerException e) {
          LOG.error("NPE when accessing seedHosts for cluster {}!", cluster.getName(), e);
          fail("NullPointerException - Guava ImmutableSet is corrupted");
        }
      }

    } finally {
      storage2.shutdown();
    }

    LOG.info("=== Test 3 PASSED ===\n");
  }

  /** Test 4: Check what Guava version we're using */
  @Test
  void testGuavaVersion() {
    LOG.info("=== Test 4: Guava Version Info ===");

    // Check ImmutableSet class
    Class<?> immutableSetClass = ImmutableSet.class;
    LOG.info(
        "ImmutableSet class location: {}",
        immutableSetClass.getProtectionDomain().getCodeSource().getLocation());

    // Create an ImmutableSet and check its implementation
    ImmutableSet<String> set = ImmutableSet.of("a", "b", "c");
    LOG.info("ImmutableSet implementation: {}", set.getClass().getName());

    // Check internal structure via reflection
    try {
      java.lang.reflect.Field[] fields = set.getClass().getDeclaredFields();
      LOG.info("Internal fields:");
      for (java.lang.reflect.Field field : fields) {
        field.setAccessible(true);
        Object value = field.get(set);
        LOG.info(
            "  {} ({}) = {}",
            field.getName(),
            field.getType().getSimpleName(),
            value == null
                ? "null"
                : (value.getClass().isArray()
                    ? "array[" + java.lang.reflect.Array.getLength(value) + "]"
                    : value));
      }
    } catch (Exception e) {
      LOG.warn("Could not inspect internal fields: {}", e.getMessage());
    }

    LOG.info("=== Test 4 PASSED ===\n");
  }
}
