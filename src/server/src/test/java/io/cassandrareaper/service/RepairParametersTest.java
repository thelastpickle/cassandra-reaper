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

package io.cassandrareaper.service;

import io.cassandrareaper.core.Segment;

import java.math.BigInteger;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.cassandra.repair.RepairParallelism;
import org.junit.jupiter.api.Test;

/** Unit tests for RepairParameters class. */
public class RepairParametersTest {

  @Test
  void testConstructor_WithAllParameters() {
    // Given: All parameters for RepairParameters
    Segment tokenRange =
        Segment.builder()
            .withTokenRange(new RingRange(BigInteger.ZERO, BigInteger.valueOf(1000)))
            .build();
    String keyspaceName = "test_keyspace";
    Set<String> columnFamilies = new HashSet<>();
    columnFamilies.add("table1");
    columnFamilies.add("table2");
    RepairParallelism repairParallelism = RepairParallelism.PARALLEL;

    // When: Creating RepairParameters
    RepairParameters params =
        new RepairParameters(tokenRange, keyspaceName, columnFamilies, repairParallelism);

    // Then: All fields should be set correctly
    assertThat(params.tokenRange).isEqualTo(tokenRange);
    assertThat(params.keyspaceName).isEqualTo(keyspaceName);
    assertThat(params.columnFamilies).isEqualTo(columnFamilies);
    assertThat(params.repairParallelism).isEqualTo(repairParallelism);
  }

  @Test
  void testConstructor_WithEmptyColumnFamilies() {
    // Given: Empty column families set
    Segment tokenRange =
        Segment.builder()
            .withTokenRange(new RingRange(BigInteger.ZERO, BigInteger.valueOf(1000)))
            .build();
    String keyspaceName = "test_keyspace";
    Set<String> columnFamilies = Collections.emptySet();
    RepairParallelism repairParallelism = RepairParallelism.SEQUENTIAL;

    // When: Creating RepairParameters
    RepairParameters params =
        new RepairParameters(tokenRange, keyspaceName, columnFamilies, repairParallelism);

    // Then: Column families should be empty
    assertThat(params.columnFamilies).isEmpty();
    assertThat(params.keyspaceName).isEqualTo(keyspaceName);
    assertThat(params.repairParallelism).isEqualTo(RepairParallelism.SEQUENTIAL);
  }

  @Test
  void testConstructor_WithNullValues() {
    // Given: Null values for some parameters
    Segment tokenRange = null;
    String keyspaceName = null;
    Set<String> columnFamilies = null;
    RepairParallelism repairParallelism = null;

    // When: Creating RepairParameters with nulls
    RepairParameters params =
        new RepairParameters(tokenRange, keyspaceName, columnFamilies, repairParallelism);

    // Then: Fields should be null (no null protection in constructor)
    assertThat(params.tokenRange).isNull();
    assertThat(params.keyspaceName).isNull();
    assertThat(params.columnFamilies).isNull();
    assertThat(params.repairParallelism).isNull();
  }

  @Test
  void testConstructor_WithDatacenterAwareParallelism() {
    // Given: Datacenter aware parallelism
    Segment tokenRange =
        Segment.builder()
            .withTokenRange(new RingRange(BigInteger.valueOf(1000), BigInteger.valueOf(2000)))
            .build();
    String keyspaceName = "system_auth";
    Set<String> columnFamilies = Collections.singleton("roles");
    RepairParallelism repairParallelism = RepairParallelism.DATACENTER_AWARE;

    // When: Creating RepairParameters
    RepairParameters params =
        new RepairParameters(tokenRange, keyspaceName, columnFamilies, repairParallelism);

    // Then: Should use datacenter aware parallelism
    assertThat(params.repairParallelism).isEqualTo(RepairParallelism.DATACENTER_AWARE);
    assertThat(params.keyspaceName).isEqualTo("system_auth");
    assertThat(params.columnFamilies).containsExactly("roles");
  }

  @Test
  void testFieldsArePublicFinal() {
    // Given: RepairParameters with test data
    Segment tokenRange =
        Segment.builder()
            .withTokenRange(new RingRange(BigInteger.ZERO, BigInteger.valueOf(1000)))
            .build();
    String keyspaceName = "test_keyspace";
    Set<String> columnFamilies = Collections.singleton("test_table");
    RepairParallelism repairParallelism = RepairParallelism.PARALLEL;

    // When: Creating RepairParameters
    RepairParameters params =
        new RepairParameters(tokenRange, keyspaceName, columnFamilies, repairParallelism);

    // Then: Fields should be accessible directly (public final)
    Segment accessedTokenRange = params.tokenRange;
    String accessedKeyspace = params.keyspaceName;
    Set<String> accessedColumnFamilies = params.columnFamilies;
    RepairParallelism accessedParallelism = params.repairParallelism;

    assertThat(accessedTokenRange).isEqualTo(tokenRange);
    assertThat(accessedKeyspace).isEqualTo(keyspaceName);
    assertThat(accessedColumnFamilies).isEqualTo(columnFamilies);
    assertThat(accessedParallelism).isEqualTo(repairParallelism);
  }

  @Test
  void testConstructor_WithLargeTokenRange() {
    // Given: Large token range
    BigInteger startToken = new BigInteger("0");
    BigInteger endToken = new BigInteger("170141183460469231731687303715884105727");
    Segment tokenRange =
        Segment.builder().withTokenRange(new RingRange(startToken, endToken)).build();
    String keyspaceName = "large_keyspace";
    Set<String> columnFamilies = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      columnFamilies.add("table_" + i);
    }
    RepairParallelism repairParallelism = RepairParallelism.PARALLEL;

    // When: Creating RepairParameters
    RepairParameters params =
        new RepairParameters(tokenRange, keyspaceName, columnFamilies, repairParallelism);

    // Then: Should handle large values correctly
    assertThat(params.tokenRange.getBaseRange().getStart()).isEqualTo(startToken);
    assertThat(params.tokenRange.getBaseRange().getEnd()).isEqualTo(endToken);
    assertThat(params.columnFamilies).hasSize(10);
  }
}
