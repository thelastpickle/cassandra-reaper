package io.cassandrareaper.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for Table class. Tests the builder pattern, validation, and data access methods for
 * table representation.
 */
public class TableTest {

  @Test
  public void testBuilderWithNameOnly() {
    // Given: A Table built with only name
    Table table = Table.builder().withName("test_table").build();

    // When/Then: Name should be set and compaction strategy should be default
    assertThat(table.getName()).isEqualTo("test_table");
    assertThat(table.getCompactionStrategy()).isEqualTo("UNKNOWN");
  }

  @Test
  public void testBuilderWithNameAndCompactionStrategy() {
    // Given: A Table built with name and compaction strategy
    Table table =
        Table.builder()
            .withName("test_table")
            .withCompactionStrategy("SizeTieredCompactionStrategy")
            .build();

    // When/Then: Both values should be set correctly
    assertThat(table.getName()).isEqualTo("test_table");
    assertThat(table.getCompactionStrategy()).isEqualTo("SizeTieredCompactionStrategy");
  }

  @Test
  public void testBuilderWithNullCompactionStrategy() {
    // Given: A Table built with null compaction strategy
    Table table = Table.builder().withName("test_table").withCompactionStrategy(null).build();

    // When/Then: Compaction strategy should default to UNKNOWN
    assertThat(table.getName()).isEqualTo("test_table");
    assertThat(table.getCompactionStrategy()).isEqualTo("UNKNOWN");
  }

  @Test
  public void testBuilderWithVariousCompactionStrategies() {
    // Given: Different compaction strategies
    String[] strategies = {
      "TimeWindowCompactionStrategy",
      "DateTieredCompactionStrategy",
      "LeveledCompactionStrategy",
      "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy"
    };

    for (String strategy : strategies) {
      // When: Building table with different strategies
      Table table = Table.builder().withName("test_table").withCompactionStrategy(strategy).build();

      // Then: Strategy should be preserved
      assertThat(table.getCompactionStrategy()).isEqualTo(strategy);
    }
  }

  @Test
  public void testBuilderFailsWithoutName() {
    // Given: A builder without name
    // When/Then: Building should fail with IllegalStateException
    assertThatThrownBy(() -> Table.builder().build())
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("`.withName(..)` must be called before `.build()`");
  }

  @Test
  public void testBuilderFailsWithDuplicateName() {
    // Given: A builder with duplicate name calls
    // When/Then: Second withName call should fail
    assertThatThrownBy(
            () ->
                Table.builder()
                    .withName("first_name")
                    .withName("second_name") // This should fail
                    .build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("`.withName(..)` can only be called once");
  }

  @Test
  public void testBuilderFailsWithDuplicateCompactionStrategy() {
    // Given: A builder with duplicate compaction strategy calls
    // When/Then: Second withCompactionStrategy call should fail
    assertThatThrownBy(
            () ->
                Table.builder()
                    .withName("test_table")
                    .withCompactionStrategy("Strategy1")
                    .withCompactionStrategy("Strategy2") // This should fail
                    .build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("`.withCompactionStrategy(..)` can only be called once");
  }

  @Test
  public void testToString() {
    // Given: A Table with name and compaction strategy
    Table table =
        Table.builder()
            .withName("users")
            .withCompactionStrategy("LeveledCompactionStrategy")
            .build();

    // When: Converting to string
    String result = table.toString();

    // Then: Should contain both name and compaction strategy
    assertThat(result).isEqualTo("{name=users, compactionStrategy=LeveledCompactionStrategy}");
  }

  @Test
  public void testToStringWithDefaultCompactionStrategy() {
    // Given: A Table with only name (default compaction strategy)
    Table table = Table.builder().withName("system_auth").build();

    // When: Converting to string
    String result = table.toString();

    // Then: Should show default compaction strategy
    assertThat(result).isEqualTo("{name=system_auth, compactionStrategy=UNKNOWN}");
  }

  @Test
  public void testBuilderChaining() {
    // Given: Builder method chaining
    Table table =
        Table.builder().withName("chained_table").withCompactionStrategy("ChainedStrategy").build();

    // When/Then: All methods should chain properly
    assertThat(table.getName()).isEqualTo("chained_table");
    assertThat(table.getCompactionStrategy()).isEqualTo("ChainedStrategy");
  }

  @Test
  public void testBuilderWithEmptyName() {
    // Given: A Table built with empty name
    Table table = Table.builder().withName("").build();

    // When/Then: Empty name should be preserved
    assertThat(table.getName()).isEqualTo("");
    assertThat(table.getCompactionStrategy()).isEqualTo("UNKNOWN");
  }

  @Test
  public void testBuilderWithEmptyCompactionStrategy() {
    // Given: A Table built with empty compaction strategy
    Table table = Table.builder().withName("test_table").withCompactionStrategy("").build();

    // When/Then: Empty strategy should be preserved (not defaulted)
    assertThat(table.getName()).isEqualTo("test_table");
    assertThat(table.getCompactionStrategy()).isEqualTo("");
  }
}
