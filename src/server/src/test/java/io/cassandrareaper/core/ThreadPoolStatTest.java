package io.cassandrareaper.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for ThreadPoolStat class. Tests the builder pattern and data access methods for thread
 * pool statistics.
 */
public class ThreadPoolStatTest {

  @Test
  public void testBuilderWithAllFields() {
    // Given: A ThreadPoolStat built with all fields
    ThreadPoolStat threadPoolStat =
        ThreadPoolStat.builder()
            .withName("ReadStage")
            .withActiveTasks(10)
            .withPendingTasks(5)
            .withCompletedTasks(1000)
            .withCurrentlyBlockedTasks(2)
            .withTotalBlockedTasks(15)
            .withMaxPoolSize(32)
            .build();

    // When/Then: All getters should return the correct values
    assertThat(threadPoolStat.getName()).isEqualTo("ReadStage");
    assertThat(threadPoolStat.getActiveTasks()).isEqualTo(10);
    assertThat(threadPoolStat.getPendingTasks()).isEqualTo(5);
    assertThat(threadPoolStat.getCompletedTasks()).isEqualTo(1000);
    assertThat(threadPoolStat.getCurrentlyBlockedTasks()).isEqualTo(2);
    assertThat(threadPoolStat.getTotalBlockedTasks()).isEqualTo(15);
    assertThat(threadPoolStat.getMaxPoolSize()).isEqualTo(32);
  }

  @Test
  public void testBuilderWithMinimalFields() {
    // Given: A ThreadPoolStat built with only name
    ThreadPoolStat threadPoolStat = ThreadPoolStat.builder().withName("WriteStage").build();

    // When/Then: Name should be set, others should be null
    assertThat(threadPoolStat.getName()).isEqualTo("WriteStage");
    assertThat(threadPoolStat.getActiveTasks()).isNull();
    assertThat(threadPoolStat.getPendingTasks()).isNull();
    assertThat(threadPoolStat.getCompletedTasks()).isNull();
    assertThat(threadPoolStat.getCurrentlyBlockedTasks()).isNull();
    assertThat(threadPoolStat.getTotalBlockedTasks()).isNull();
    assertThat(threadPoolStat.getMaxPoolSize()).isNull();
  }

  @Test
  public void testBuilderWithNullValues() {
    // Given: A ThreadPoolStat built with explicit null values
    ThreadPoolStat threadPoolStat =
        ThreadPoolStat.builder()
            .withName(null)
            .withActiveTasks(null)
            .withPendingTasks(null)
            .withCompletedTasks(null)
            .withCurrentlyBlockedTasks(null)
            .withTotalBlockedTasks(null)
            .withMaxPoolSize(null)
            .build();

    // When/Then: All values should be null
    assertThat(threadPoolStat.getName()).isNull();
    assertThat(threadPoolStat.getActiveTasks()).isNull();
    assertThat(threadPoolStat.getPendingTasks()).isNull();
    assertThat(threadPoolStat.getCompletedTasks()).isNull();
    assertThat(threadPoolStat.getCurrentlyBlockedTasks()).isNull();
    assertThat(threadPoolStat.getTotalBlockedTasks()).isNull();
    assertThat(threadPoolStat.getMaxPoolSize()).isNull();
  }

  @Test
  public void testBuilderWithZeroValues() {
    // Given: A ThreadPoolStat built with zero values
    ThreadPoolStat threadPoolStat =
        ThreadPoolStat.builder()
            .withName("CompactionExecutor")
            .withActiveTasks(0)
            .withPendingTasks(0)
            .withCompletedTasks(0)
            .withCurrentlyBlockedTasks(0)
            .withTotalBlockedTasks(0)
            .withMaxPoolSize(0)
            .build();

    // When/Then: All values should be zero
    assertThat(threadPoolStat.getName()).isEqualTo("CompactionExecutor");
    assertThat(threadPoolStat.getActiveTasks()).isEqualTo(0);
    assertThat(threadPoolStat.getPendingTasks()).isEqualTo(0);
    assertThat(threadPoolStat.getCompletedTasks()).isEqualTo(0);
    assertThat(threadPoolStat.getCurrentlyBlockedTasks()).isEqualTo(0);
    assertThat(threadPoolStat.getTotalBlockedTasks()).isEqualTo(0);
    assertThat(threadPoolStat.getMaxPoolSize()).isEqualTo(0);
  }

  @Test
  public void testBuilderWithNegativeValues() {
    // Given: A ThreadPoolStat built with negative values (edge case)
    ThreadPoolStat threadPoolStat =
        ThreadPoolStat.builder()
            .withName("MutationStage")
            .withActiveTasks(-1)
            .withPendingTasks(-2)
            .withCompletedTasks(-3)
            .withCurrentlyBlockedTasks(-4)
            .withTotalBlockedTasks(-5)
            .withMaxPoolSize(-6)
            .build();

    // When/Then: Negative values should be preserved (no validation in builder)
    assertThat(threadPoolStat.getName()).isEqualTo("MutationStage");
    assertThat(threadPoolStat.getActiveTasks()).isEqualTo(-1);
    assertThat(threadPoolStat.getPendingTasks()).isEqualTo(-2);
    assertThat(threadPoolStat.getCompletedTasks()).isEqualTo(-3);
    assertThat(threadPoolStat.getCurrentlyBlockedTasks()).isEqualTo(-4);
    assertThat(threadPoolStat.getTotalBlockedTasks()).isEqualTo(-5);
    assertThat(threadPoolStat.getMaxPoolSize()).isEqualTo(-6);
  }

  @Test
  public void testBuilderWithLargeValues() {
    // Given: A ThreadPoolStat built with large values
    ThreadPoolStat threadPoolStat =
        ThreadPoolStat.builder()
            .withName("ValidationExecutor")
            .withActiveTasks(Integer.MAX_VALUE)
            .withPendingTasks(1000000)
            .withCompletedTasks(999999999)
            .withCurrentlyBlockedTasks(500000)
            .withTotalBlockedTasks(750000)
            .withMaxPoolSize(128)
            .build();

    // When/Then: Large values should be preserved
    assertThat(threadPoolStat.getName()).isEqualTo("ValidationExecutor");
    assertThat(threadPoolStat.getActiveTasks()).isEqualTo(Integer.MAX_VALUE);
    assertThat(threadPoolStat.getPendingTasks()).isEqualTo(1000000);
    assertThat(threadPoolStat.getCompletedTasks()).isEqualTo(999999999);
    assertThat(threadPoolStat.getCurrentlyBlockedTasks()).isEqualTo(500000);
    assertThat(threadPoolStat.getTotalBlockedTasks()).isEqualTo(750000);
    assertThat(threadPoolStat.getMaxPoolSize()).isEqualTo(128);
  }

  @Test
  public void testBuilderWithEmptyStringName() {
    // Given: A ThreadPoolStat built with empty string name
    ThreadPoolStat threadPoolStat =
        ThreadPoolStat.builder()
            .withName("")
            .withActiveTasks(1)
            .withPendingTasks(2)
            .withCompletedTasks(3)
            .withCurrentlyBlockedTasks(4)
            .withTotalBlockedTasks(5)
            .withMaxPoolSize(6)
            .build();

    // When/Then: Empty string should be preserved
    assertThat(threadPoolStat.getName()).isEqualTo("");
    assertThat(threadPoolStat.getActiveTasks()).isEqualTo(1);
    assertThat(threadPoolStat.getPendingTasks()).isEqualTo(2);
    assertThat(threadPoolStat.getCompletedTasks()).isEqualTo(3);
    assertThat(threadPoolStat.getCurrentlyBlockedTasks()).isEqualTo(4);
    assertThat(threadPoolStat.getTotalBlockedTasks()).isEqualTo(5);
    assertThat(threadPoolStat.getMaxPoolSize()).isEqualTo(6);
  }

  @Test
  public void testBuilderPartialSetters() {
    // Given: A ThreadPoolStat built with only some fields set
    ThreadPoolStat threadPoolStat =
        ThreadPoolStat.builder()
            .withName("RepairStage")
            .withActiveTasks(3)
            .withCompletedTasks(500)
            .withMaxPoolSize(16)
            .build();

    // When/Then: Set fields should have values, others should be null
    assertThat(threadPoolStat.getName()).isEqualTo("RepairStage");
    assertThat(threadPoolStat.getActiveTasks()).isEqualTo(3);
    assertThat(threadPoolStat.getPendingTasks()).isNull();
    assertThat(threadPoolStat.getCompletedTasks()).isEqualTo(500);
    assertThat(threadPoolStat.getCurrentlyBlockedTasks()).isNull();
    assertThat(threadPoolStat.getTotalBlockedTasks()).isNull();
    assertThat(threadPoolStat.getMaxPoolSize()).isEqualTo(16);
  }

  @Test
  public void testBuilderChaining() {
    // Given: A builder that chains multiple setter calls
    ThreadPoolStat.Builder builder = ThreadPoolStat.builder();
    ThreadPoolStat threadPoolStat =
        builder
            .withName("AntiEntropyStage")
            .withActiveTasks(1)
            .withPendingTasks(2)
            .withCompletedTasks(3)
            .withCurrentlyBlockedTasks(4)
            .withTotalBlockedTasks(5)
            .withMaxPoolSize(6)
            .build();

    // When/Then: Builder chaining should work correctly
    assertThat(threadPoolStat.getName()).isEqualTo("AntiEntropyStage");
    assertThat(threadPoolStat.getActiveTasks()).isEqualTo(1);
    assertThat(threadPoolStat.getPendingTasks()).isEqualTo(2);
    assertThat(threadPoolStat.getCompletedTasks()).isEqualTo(3);
    assertThat(threadPoolStat.getCurrentlyBlockedTasks()).isEqualTo(4);
    assertThat(threadPoolStat.getTotalBlockedTasks()).isEqualTo(5);
    assertThat(threadPoolStat.getMaxPoolSize()).isEqualTo(6);
  }

  @Test
  public void testBuilderReuse() {
    // Given: A builder used to create multiple instances
    ThreadPoolStat.Builder builder = ThreadPoolStat.builder();

    // When: Building first instance
    ThreadPoolStat first = builder.withName("FIRST").withActiveTasks(1).build();

    // Then: First instance should have correct values
    assertThat(first.getName()).isEqualTo("FIRST");
    assertThat(first.getActiveTasks()).isEqualTo(1);

    // When: Building second instance with different values
    ThreadPoolStat second = builder.withName("SECOND").withActiveTasks(2).build();

    // Then: Second instance should have updated values
    assertThat(second.getName()).isEqualTo("SECOND");
    assertThat(second.getActiveTasks()).isEqualTo(2);
  }

  @Test
  public void testBuilderOverrideValues() {
    // Given: A builder where values are overridden
    ThreadPoolStat threadPoolStat =
        ThreadPoolStat.builder()
            .withName("INITIAL")
            .withActiveTasks(100)
            .withName("FINAL") // Override name
            .withActiveTasks(200) // Override activeTasks
            .build();

    // When/Then: Final values should be used
    assertThat(threadPoolStat.getName()).isEqualTo("FINAL");
    assertThat(threadPoolStat.getActiveTasks()).isEqualTo(200);
  }

  @Test
  public void testTypicalCassandraThreadPools() {
    // Given: ThreadPoolStats for typical Cassandra thread pools
    ThreadPoolStat readStage =
        ThreadPoolStat.builder()
            .withName("ReadStage")
            .withActiveTasks(8)
            .withPendingTasks(0)
            .withCompletedTasks(1000000)
            .withCurrentlyBlockedTasks(0)
            .withTotalBlockedTasks(0)
            .withMaxPoolSize(32)
            .build();

    ThreadPoolStat writeStage =
        ThreadPoolStat.builder()
            .withName("MutationStage")
            .withActiveTasks(4)
            .withPendingTasks(2)
            .withCompletedTasks(500000)
            .withCurrentlyBlockedTasks(0)
            .withTotalBlockedTasks(5)
            .withMaxPoolSize(32)
            .build();

    // When/Then: Both should have expected values
    assertThat(readStage.getName()).isEqualTo("ReadStage");
    assertThat(readStage.getActiveTasks()).isEqualTo(8);
    assertThat(readStage.getPendingTasks()).isEqualTo(0);

    assertThat(writeStage.getName()).isEqualTo("MutationStage");
    assertThat(writeStage.getActiveTasks()).isEqualTo(4);
    assertThat(writeStage.getPendingTasks()).isEqualTo(2);
  }

  @Test
  public void testBuilderWithAllTypicalThreadPoolNames() {
    // Given: ThreadPoolStats for various Cassandra thread pool names
    String[] threadPoolNames = {
      "ReadStage",
      "MutationStage",
      "CompactionExecutor",
      "ValidationExecutor",
      "AntiEntropyStage",
      "RepairStage",
      "GossipStage",
      "RequestResponseStage",
      "InternalResponseStage",
      "MemtableFlushWriter",
      "MemtablePostFlush",
      "MemtableReclaimMemory",
      "PendingRangeCalculator",
      "Sampler",
      "SecondaryIndexManagement",
      "CacheCleanupExecutor"
    };

    for (String poolName : threadPoolNames) {
      // When: Creating a ThreadPoolStat for each pool
      ThreadPoolStat stat =
          ThreadPoolStat.builder()
              .withName(poolName)
              .withActiveTasks(1)
              .withPendingTasks(0)
              .withCompletedTasks(100)
              .withCurrentlyBlockedTasks(0)
              .withTotalBlockedTasks(0)
              .withMaxPoolSize(8)
              .build();

      // Then: Name should be correctly set
      assertThat(stat.getName()).isEqualTo(poolName);
      assertThat(stat.getActiveTasks()).isEqualTo(1);
      assertThat(stat.getMaxPoolSize()).isEqualTo(8);
    }
  }
}
