package io.cassandrareaper.core;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for PercentRepairedMetric class. Tests the builder pattern and data access methods for
 * percent repaired metrics.
 */
public class PercentRepairedMetricTest {

  @Test
  public void testBuilderWithAllFields() {
    // Given: A PercentRepairedMetric built with all fields
    UUID repairScheduleId = UUID.randomUUID();
    PercentRepairedMetric metric =
        PercentRepairedMetric.builder()
            .withCluster("test_cluster")
            .withNode("127.0.0.1")
            .withRepairScheduleId(repairScheduleId)
            .withKeyspaceName("test_keyspace")
            .withTableName("test_table")
            .withPercentRepaired(85)
            .build();

    // When/Then: All getters should return the correct values
    assertThat(metric.getCluster()).isEqualTo("test_cluster");
    assertThat(metric.getNode()).isEqualTo("127.0.0.1");
    assertThat(metric.getRepairScheduleId()).isEqualTo(repairScheduleId);
    assertThat(metric.getKeyspaceName()).isEqualTo("test_keyspace");
    assertThat(metric.getTableName()).isEqualTo("test_table");
    assertThat(metric.getPercentRepaired()).isEqualTo(85);
  }

  @Test
  public void testBuilderWithNullValues() {
    // Given: A PercentRepairedMetric built with null values
    PercentRepairedMetric metric =
        PercentRepairedMetric.builder()
            .withCluster(null)
            .withNode(null)
            .withRepairScheduleId(null)
            .withKeyspaceName(null)
            .withTableName(null)
            .withPercentRepaired(0)
            .build();

    // When/Then: All null values should be preserved
    assertThat(metric.getCluster()).isNull();
    assertThat(metric.getNode()).isNull();
    assertThat(metric.getRepairScheduleId()).isNull();
    assertThat(metric.getKeyspaceName()).isNull();
    assertThat(metric.getTableName()).isNull();
    assertThat(metric.getPercentRepaired()).isEqualTo(0);
  }

  @Test
  public void testBuilderWithEmptyStrings() {
    // Given: A PercentRepairedMetric built with empty strings
    UUID repairScheduleId = UUID.randomUUID();
    PercentRepairedMetric metric =
        PercentRepairedMetric.builder()
            .withCluster("")
            .withNode("")
            .withRepairScheduleId(repairScheduleId)
            .withKeyspaceName("")
            .withTableName("")
            .withPercentRepaired(100)
            .build();

    // When/Then: Empty strings should be preserved
    assertThat(metric.getCluster()).isEqualTo("");
    assertThat(metric.getNode()).isEqualTo("");
    assertThat(metric.getRepairScheduleId()).isEqualTo(repairScheduleId);
    assertThat(metric.getKeyspaceName()).isEqualTo("");
    assertThat(metric.getTableName()).isEqualTo("");
    assertThat(metric.getPercentRepaired()).isEqualTo(100);
  }

  @Test
  public void testBuilderWithZeroPercentRepaired() {
    // Given: A PercentRepairedMetric with 0% repaired
    PercentRepairedMetric metric =
        PercentRepairedMetric.builder()
            .withCluster("test_cluster")
            .withNode("node1")
            .withRepairScheduleId(UUID.randomUUID())
            .withKeyspaceName("system")
            .withTableName("peers")
            .withPercentRepaired(0)
            .build();

    // When/Then: Zero percent should be preserved
    assertThat(metric.getPercentRepaired()).isEqualTo(0);
  }

  @Test
  public void testBuilderWithFullyRepairedTable() {
    // Given: A PercentRepairedMetric with 100% repaired
    PercentRepairedMetric metric =
        PercentRepairedMetric.builder()
            .withCluster("prod_cluster")
            .withNode("192.168.1.100")
            .withRepairScheduleId(UUID.randomUUID())
            .withKeyspaceName("user_data")
            .withTableName("users")
            .withPercentRepaired(100)
            .build();

    // When/Then: Full repair percentage should be preserved
    assertThat(metric.getPercentRepaired()).isEqualTo(100);
  }

  @Test
  public void testBuilderWithNegativePercentRepaired() {
    // Given: A PercentRepairedMetric with negative percent (edge case)
    PercentRepairedMetric metric =
        PercentRepairedMetric.builder()
            .withCluster("test_cluster")
            .withNode("node1")
            .withRepairScheduleId(UUID.randomUUID())
            .withKeyspaceName("test_ks")
            .withTableName("test_table")
            .withPercentRepaired(-1)
            .build();

    // When/Then: Negative value should be preserved (for error cases)
    assertThat(metric.getPercentRepaired()).isEqualTo(-1);
  }

  @Test
  public void testBuilderWithLargePercentRepaired() {
    // Given: A PercentRepairedMetric with percent > 100 (edge case)
    PercentRepairedMetric metric =
        PercentRepairedMetric.builder()
            .withCluster("test_cluster")
            .withNode("node1")
            .withRepairScheduleId(UUID.randomUUID())
            .withKeyspaceName("test_ks")
            .withTableName("test_table")
            .withPercentRepaired(150)
            .build();

    // When/Then: Value > 100 should be preserved (for error cases)
    assertThat(metric.getPercentRepaired()).isEqualTo(150);
  }

  @Test
  public void testBuilderChaining() {
    // Given: Builder method chaining
    UUID scheduleId = UUID.randomUUID();
    PercentRepairedMetric metric =
        PercentRepairedMetric.builder()
            .withCluster("chain_cluster")
            .withNode("chain_node")
            .withRepairScheduleId(scheduleId)
            .withKeyspaceName("chain_ks")
            .withTableName("chain_table")
            .withPercentRepaired(75)
            .build();

    // When/Then: All methods should chain properly
    assertThat(metric.getCluster()).isEqualTo("chain_cluster");
    assertThat(metric.getNode()).isEqualTo("chain_node");
    assertThat(metric.getRepairScheduleId()).isEqualTo(scheduleId);
    assertThat(metric.getKeyspaceName()).isEqualTo("chain_ks");
    assertThat(metric.getTableName()).isEqualTo("chain_table");
    assertThat(metric.getPercentRepaired()).isEqualTo(75);
  }

  @Test
  public void testBuilderWithPartialFields() {
    // Given: A PercentRepairedMetric built with only some fields set
    UUID scheduleId = UUID.randomUUID();
    PercentRepairedMetric metric =
        PercentRepairedMetric.builder()
            .withCluster("partial_cluster")
            .withRepairScheduleId(scheduleId)
            .withPercentRepaired(42)
            .build();

    // When/Then: Set fields should have values, unset fields should be null/default
    assertThat(metric.getCluster()).isEqualTo("partial_cluster");
    assertThat(metric.getNode()).isNull();
    assertThat(metric.getRepairScheduleId()).isEqualTo(scheduleId);
    assertThat(metric.getKeyspaceName()).isNull();
    assertThat(metric.getTableName()).isNull();
    assertThat(metric.getPercentRepaired()).isEqualTo(42);
  }

  @Test
  public void testBuilderWithRealWorldData() {
    // Given: A PercentRepairedMetric with realistic data
    UUID scheduleId = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
    PercentRepairedMetric metric =
        PercentRepairedMetric.builder()
            .withCluster("production-cassandra")
            .withNode("cassandra-node-01.example.com")
            .withRepairScheduleId(scheduleId)
            .withKeyspaceName("user_analytics")
            .withTableName("user_sessions")
            .withPercentRepaired(67)
            .build();

    // When/Then: All realistic values should be preserved
    assertThat(metric.getCluster()).isEqualTo("production-cassandra");
    assertThat(metric.getNode()).isEqualTo("cassandra-node-01.example.com");
    assertThat(metric.getRepairScheduleId()).isEqualTo(scheduleId);
    assertThat(metric.getKeyspaceName()).isEqualTo("user_analytics");
    assertThat(metric.getTableName()).isEqualTo("user_sessions");
    assertThat(metric.getPercentRepaired()).isEqualTo(67);
  }

  @Test
  public void testBuilderWithSpecialCharacters() {
    // Given: A PercentRepairedMetric with special characters in strings
    UUID scheduleId = UUID.randomUUID();
    PercentRepairedMetric metric =
        PercentRepairedMetric.builder()
            .withCluster("test-cluster_with.special#chars")
            .withNode("node@192.168.1.1:9042")
            .withRepairScheduleId(scheduleId)
            .withKeyspaceName("test_ks-v2")
            .withTableName("table_with_underscores")
            .withPercentRepaired(88)
            .build();

    // When/Then: Special characters should be preserved
    assertThat(metric.getCluster()).isEqualTo("test-cluster_with.special#chars");
    assertThat(metric.getNode()).isEqualTo("node@192.168.1.1:9042");
    assertThat(metric.getKeyspaceName()).isEqualTo("test_ks-v2");
    assertThat(metric.getTableName()).isEqualTo("table_with_underscores");
    assertThat(metric.getPercentRepaired()).isEqualTo(88);
  }
}
