package io.cassandrareaper.storage;

import io.cassandrareaper.service.RingRange;

import java.lang.reflect.Constructor;
import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for JsonParseUtils utility class. Tests cover JSON parsing and writing operations for
 * ring ranges and replicas. Follows Clean Code principles with descriptive test names and
 * comprehensive coverage.
 */
public class JsonParseUtilsTest {

  @Test
  public void testConstructorThrowsIllegalStateException() throws Exception {
    // When/Then: Constructor should throw IllegalStateException for utility class
    assertThatThrownBy(
            () -> {
              try {
                Constructor<JsonParseUtils> constructor =
                    JsonParseUtils.class.getDeclaredConstructor();
                constructor.setAccessible(true);
                constructor.newInstance();
              } catch (Exception e) {
                if (e.getCause() instanceof IllegalStateException) {
                  throw (IllegalStateException) e.getCause();
                }
                throw new RuntimeException(e);
              }
            })
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Utility class");
  }

  @Test
  public void testParseRingRangeListWithEmptyOptional() {
    // Given: Empty optional
    Optional<String> emptyJson = Optional.empty();

    // When: Parsing ring range list
    List<RingRange> result = JsonParseUtils.parseRingRangeList(emptyJson);

    // Then: Should return empty list
    assertThat(result).isNotNull().isEmpty();
  }

  @Test
  public void testParseRingRangeListWithValidJson() {
    // Given: Valid JSON for ring ranges
    String json = "[{\"start\":\"0\",\"end\":\"100\"}]";
    Optional<String> jsonOptional = Optional.of(json);

    // When: Parsing ring range list
    List<RingRange> result = JsonParseUtils.parseRingRangeList(jsonOptional);

    // Then: Should return parsed ring ranges
    assertThat(result).isNotNull().hasSize(1);
    RingRange range = result.get(0);
    assertThat(range.getStart()).isEqualTo(new BigInteger("0"));
    assertThat(range.getEnd()).isEqualTo(new BigInteger("100"));
  }

  @Test
  public void testParseRingRangeListWithMultipleRanges() {
    // Given: JSON with multiple ring ranges
    String json = "[{\"start\":\"0\",\"end\":\"100\"},{\"start\":\"100\",\"end\":\"200\"}]";
    Optional<String> jsonOptional = Optional.of(json);

    // When: Parsing ring range list
    List<RingRange> result = JsonParseUtils.parseRingRangeList(jsonOptional);

    // Then: Should return all parsed ring ranges
    assertThat(result).isNotNull().hasSize(2);
    assertThat(result.get(0).getStart()).isEqualTo(new BigInteger("0"));
    assertThat(result.get(0).getEnd()).isEqualTo(new BigInteger("100"));
    assertThat(result.get(1).getStart()).isEqualTo(new BigInteger("100"));
    assertThat(result.get(1).getEnd()).isEqualTo(new BigInteger("200"));
  }

  @Test
  public void testParseRingRangeListWithInvalidJson() {
    // Given: Invalid JSON
    String invalidJson = "{invalid json}";
    Optional<String> jsonOptional = Optional.of(invalidJson);

    // When/Then: Should throw IllegalArgumentException
    assertThatThrownBy(() -> JsonParseUtils.parseRingRangeList(jsonOptional))
        .isInstanceOf(IllegalArgumentException.class)
        .hasCauseInstanceOf(Exception.class);
  }

  @Test
  public void testWriteTokenRangesTxtWithEmptyList() {
    // Given: Empty list of token ranges
    List<RingRange> emptyList = Lists.newArrayList();

    // When: Writing token ranges to text
    String result = JsonParseUtils.writeTokenRangesTxt(emptyList);

    // Then: Should return JSON array representation
    assertThat(result).isEqualTo("[]");
  }

  @Test
  public void testWriteTokenRangesTxtWithSingleRange() {
    // Given: Single ring range
    RingRange range = new RingRange(new BigInteger("0"), new BigInteger("100"));
    List<RingRange> ranges = Lists.newArrayList(range);

    // When: Writing token ranges to text
    String result = JsonParseUtils.writeTokenRangesTxt(ranges);

    // Then: Should return JSON representation
    assertThat(result).contains("\"start\":0").contains("\"end\":100");
  }

  @Test
  public void testWriteTokenRangesTxtWithMultipleRanges() {
    // Given: Multiple ring ranges
    RingRange range1 = new RingRange(new BigInteger("0"), new BigInteger("100"));
    RingRange range2 = new RingRange(new BigInteger("100"), new BigInteger("200"));
    List<RingRange> ranges = Lists.newArrayList(range1, range2);

    // When: Writing token ranges to text
    String result = JsonParseUtils.writeTokenRangesTxt(ranges);

    // Then: Should return JSON array with both ranges
    assertThat(result)
        .contains("\"start\":0")
        .contains("\"end\":100")
        .contains("\"start\":100")
        .contains("\"end\":200");
  }

  @Test
  public void testParseReplicasWithEmptyOptional() {
    // Given: Empty optional
    Optional<String> emptyJson = Optional.empty();

    // When: Parsing replicas
    Map<String, String> result = JsonParseUtils.parseReplicas(emptyJson);

    // Then: Should return empty map
    assertThat(result).isNotNull().isEmpty();
  }

  @Test
  public void testParseReplicasWithValidJson() {
    // Given: Valid JSON for replicas
    String json = "{\"node1\":\"dc1\",\"node2\":\"dc2\"}";
    Optional<String> jsonOptional = Optional.of(json);

    // When: Parsing replicas
    Map<String, String> result = JsonParseUtils.parseReplicas(jsonOptional);

    // Then: Should return parsed replicas map
    assertThat(result).isNotNull().hasSize(2);
    assertThat(result.get("node1")).isEqualTo("dc1");
    assertThat(result.get("node2")).isEqualTo("dc2");
  }

  @Test
  public void testParseReplicasWithEmptyJson() {
    // Given: Empty JSON object
    String json = "{}";
    Optional<String> jsonOptional = Optional.of(json);

    // When: Parsing replicas
    Map<String, String> result = JsonParseUtils.parseReplicas(jsonOptional);

    // Then: Should return empty map
    assertThat(result).isNotNull().isEmpty();
  }

  @Test
  public void testParseReplicasWithInvalidJson() {
    // Given: Invalid JSON
    String invalidJson = "{invalid: json}";
    Optional<String> jsonOptional = Optional.of(invalidJson);

    // When/Then: Should throw IllegalArgumentException
    assertThatThrownBy(() -> JsonParseUtils.parseReplicas(jsonOptional))
        .isInstanceOf(IllegalArgumentException.class)
        .hasCauseInstanceOf(Exception.class);
  }

  @Test
  public void testWriteReplicasWithEmptyMap() {
    // Given: Empty replicas map
    Map<String, String> emptyMap = Collections.emptyMap();

    // When: Writing replicas
    String result = JsonParseUtils.writeReplicas(emptyMap);

    // Then: Should return empty JSON object
    assertThat(result).isEqualTo("{}");
  }

  @Test
  public void testWriteReplicasWithSingleEntry() {
    // Given: Map with single entry
    Map<String, String> replicas = new HashMap<>();
    replicas.put("node1", "dc1");

    // When: Writing replicas
    String result = JsonParseUtils.writeReplicas(replicas);

    // Then: Should return JSON representation
    assertThat(result).contains("\"node1\":\"dc1\"");
  }

  @Test
  public void testWriteReplicasWithMultipleEntries() {
    // Given: Map with multiple entries
    Map<String, String> replicas = new HashMap<>();
    replicas.put("node1", "dc1");
    replicas.put("node2", "dc2");
    replicas.put("node3", "dc1");

    // When: Writing replicas
    String result = JsonParseUtils.writeReplicas(replicas);

    // Then: Should return JSON with all entries
    assertThat(result)
        .contains("\"node1\":\"dc1\"")
        .contains("\"node2\":\"dc2\"")
        .contains("\"node3\":\"dc1\"");
  }

  @Test
  public void testRoundTripRingRanges() {
    // Given: Ring ranges
    RingRange range1 = new RingRange(new BigInteger("0"), new BigInteger("100"));
    RingRange range2 = new RingRange(new BigInteger("100"), new BigInteger("200"));
    List<RingRange> originalRanges = Lists.newArrayList(range1, range2);

    // When: Writing and then parsing back
    String json = JsonParseUtils.writeTokenRangesTxt(originalRanges);
    List<RingRange> parsedRanges = JsonParseUtils.parseRingRangeList(Optional.of(json));

    // Then: Should get equivalent ranges back
    assertThat(parsedRanges).hasSize(2);
    assertThat(parsedRanges.get(0).getStart()).isEqualTo(new BigInteger("0"));
    assertThat(parsedRanges.get(0).getEnd()).isEqualTo(new BigInteger("100"));
    assertThat(parsedRanges.get(1).getStart()).isEqualTo(new BigInteger("100"));
    assertThat(parsedRanges.get(1).getEnd()).isEqualTo(new BigInteger("200"));
  }

  @Test
  public void testRoundTripReplicas() {
    // Given: Replicas map
    Map<String, String> originalReplicas = new HashMap<>();
    originalReplicas.put("node1", "dc1");
    originalReplicas.put("node2", "dc2");

    // When: Writing and then parsing back
    String json = JsonParseUtils.writeReplicas(originalReplicas);
    Map<String, String> parsedReplicas = JsonParseUtils.parseReplicas(Optional.of(json));

    // Then: Should get equivalent map back
    assertThat(parsedReplicas).hasSize(2);
    assertThat(parsedReplicas.get("node1")).isEqualTo("dc1");
    assertThat(parsedReplicas.get("node2")).isEqualTo("dc2");
  }

  @Test
  public void testLargeNumberRanges() {
    // Given: Ring ranges with very large numbers
    BigInteger largeStart = new BigInteger("123456789012345678901234567890");
    BigInteger largeEnd = new BigInteger("987654321098765432109876543210");
    RingRange largeRange = new RingRange(largeStart, largeEnd);
    List<RingRange> ranges = Lists.newArrayList(largeRange);

    // When: Writing and parsing back
    String json = JsonParseUtils.writeTokenRangesTxt(ranges);
    List<RingRange> parsedRanges = JsonParseUtils.parseRingRangeList(Optional.of(json));

    // Then: Should handle large numbers correctly
    assertThat(parsedRanges).hasSize(1);
    assertThat(parsedRanges.get(0).getStart()).isEqualTo(largeStart);
    assertThat(parsedRanges.get(0).getEnd()).isEqualTo(largeEnd);
  }
}
