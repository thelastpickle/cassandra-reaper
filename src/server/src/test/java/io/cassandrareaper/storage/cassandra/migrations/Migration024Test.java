/*
 * Copyright 2020-2020 The Last Pickle Ltd
 *
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

package io.cassandrareaper.storage.cassandra.migrations;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public final class Migration024Test {

  @Mock private CqlSession mockSession;
  @Mock private Metadata mockMetadata;
  @Mock private Node mockNode;
  @Mock private KeyspaceMetadata mockKeyspace;
  @Mock private TableMetadata mockTable;

  private final String keyspaceName = "reaper";

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testMigrateWithCassandra3_0_8_ShouldAlterTable() {
    // Setup - Cassandra 3.0.8 should trigger migration
    Version cassandraVersion = Version.parse("3.0.8");
    setupMockSessionWithVersion(cassandraVersion);
    setupMockTableNotUsingTwcs();

    // Execute
    Migration024.migrate(mockSession, keyspaceName);

    // Verify - Table should be altered to use TWCS
    verify(mockSession, times(1)).execute(anyString());
  }

  @Test
  public void testMigrateWithCassandra3_8_ShouldAlterTable() {
    // Setup - Cassandra 3.8+ should trigger migration
    Version cassandraVersion = Version.parse("3.8.0");
    setupMockSessionWithVersion(cassandraVersion);
    setupMockTableNotUsingTwcs();

    // Execute
    Migration024.migrate(mockSession, keyspaceName);

    // Verify - Table should be altered to use TWCS
    verify(mockSession, times(1)).execute(anyString());
  }

  @Test
  public void testMigrateWithCassandra4_0_ShouldAlterTable() {
    // Setup - Cassandra 4.0+ should trigger migration
    Version cassandraVersion = Version.parse("4.0.0");
    setupMockSessionWithVersion(cassandraVersion);
    setupMockTableNotUsingTwcs();

    // Execute
    Migration024.migrate(mockSession, keyspaceName);

    // Verify - Table should be altered to use TWCS
    verify(mockSession, times(1)).execute(anyString());
  }

  @Test
  public void testMigrateWithCassandra2_2_ShouldNotAlterTable() {
    // Setup - Cassandra 2.2 should not trigger migration
    Version cassandraVersion = Version.parse("2.2.0");
    setupMockSessionWithVersion(cassandraVersion);

    // Execute
    Migration024.migrate(mockSession, keyspaceName);

    // Verify - No table should be altered
    verify(mockSession, never()).execute(anyString());
  }

  @Test
  public void testMigrateWithCassandra3_0_7_ShouldNotAlterTable() {
    // Setup - Cassandra 3.0.7 (below 3.0.8) should not trigger migration
    Version cassandraVersion = Version.parse("3.0.7");
    setupMockSessionWithVersion(cassandraVersion);

    // Execute
    Migration024.migrate(mockSession, keyspaceName);

    // Verify - No table should be altered
    verify(mockSession, never()).execute(anyString());
  }

  @Test
  public void testMigrateWithCassandra3_1_ShouldNotAlterTable() {
    // Setup - Cassandra 3.1-3.7 should not trigger migration
    Version cassandraVersion = Version.parse("3.1.0");
    setupMockSessionWithVersion(cassandraVersion);

    // Execute
    Migration024.migrate(mockSession, keyspaceName);

    // Verify - No table should be altered
    verify(mockSession, never()).execute(anyString());
  }

  @Test
  public void testMigrateWhenAlreadyUsingTwcs_ShouldNotAlterTable() {
    // Setup - Table already using TWCS
    Version cassandraVersion = Version.parse("3.8.0");
    setupMockSessionWithVersion(cassandraVersion);
    setupMockTableAlreadyUsingTwcs();

    // Execute
    Migration024.migrate(mockSession, keyspaceName);

    // Verify - No table should be altered since already using TWCS
    verify(mockSession, never()).execute(anyString());
  }

  @Test
  public void testMigrateWithRuntimeException_ShouldCatchAndContinue() {
    // Setup - RuntimeException during migration
    Version cassandraVersion = Version.parse("3.8.0");
    setupMockSessionWithVersion(cassandraVersion);

    // Mock table metadata to throw exception when checking compaction
    when(mockSession.getMetadata()).thenReturn(mockMetadata);
    when(mockMetadata.getKeyspace(keyspaceName)).thenReturn(Optional.of(mockKeyspace));
    when(mockKeyspace.getTable("node_metrics_v3")).thenReturn(Optional.of(mockTable));
    when(mockTable.getOptions()).thenThrow(new RuntimeException("Test exception"));

    // Execute - should not throw exception
    Migration024.migrate(mockSession, keyspaceName);

    // Verify - Exception should be caught and logged, no executions should happen
    verify(mockSession, never()).execute(anyString());
  }

  private void setupMockSessionWithVersion(Version version) {
    Map<UUID, Node> nodes = new HashMap<>();
    nodes.put(UUID.randomUUID(), mockNode);

    when(mockSession.getMetadata()).thenReturn(mockMetadata);
    when(mockMetadata.getNodes()).thenReturn(nodes);
    when(mockNode.getCassandraVersion()).thenReturn(version);
  }

  private void setupMockTableNotUsingTwcs() {
    when(mockSession.getMetadata()).thenReturn(mockMetadata);
    when(mockMetadata.getKeyspace(keyspaceName)).thenReturn(Optional.of(mockKeyspace));
    when(mockKeyspace.getTable("node_metrics_v3")).thenReturn(Optional.of(mockTable));

    Map<String, String> compactionOptions = new HashMap<>();
    compactionOptions.put("class", "SizeTieredCompactionStrategy");

    Map<CqlIdentifier, Object> tableOptions = new HashMap<>();
    tableOptions.put(CqlIdentifier.fromCql("compaction"), compactionOptions);

    when(mockTable.getOptions()).thenReturn(tableOptions);
  }

  private void setupMockTableAlreadyUsingTwcs() {
    when(mockSession.getMetadata()).thenReturn(mockMetadata);
    when(mockMetadata.getKeyspace(keyspaceName)).thenReturn(Optional.of(mockKeyspace));
    when(mockKeyspace.getTable("node_metrics_v3")).thenReturn(Optional.of(mockTable));

    Map<String, String> compactionOptions = new HashMap<>();
    compactionOptions.put("class", "TimeWindowCompactionStrategy");

    Map<CqlIdentifier, Object> tableOptions = new HashMap<>();
    tableOptions.put(CqlIdentifier.fromCql("compaction"), compactionOptions);

    when(mockTable.getOptions()).thenReturn(tableOptions);
  }
}
