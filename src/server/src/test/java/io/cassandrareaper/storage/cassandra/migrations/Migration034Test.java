/*
 * Copyright 2020-2020 The Last Pickle Ltd
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cassandrareaper.storage.cassandra.migrations;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public final class Migration034Test {

  @Mock private CqlSession mockSession;
  @Mock private Metadata mockMetadata;
  @Mock private KeyspaceMetadata mockKeyspace;
  @Mock private TableMetadata mockTable;
  @Mock private ColumnMetadata mockColumn;

  private final String keyspaceName = "reaper";
  private final String tableName = "repair_unit_v1";

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    when(mockSession.getMetadata()).thenReturn(mockMetadata);
    when(mockMetadata.getKeyspace(keyspaceName)).thenReturn(Optional.of(mockKeyspace));
    when(mockKeyspace.getTable(tableName)).thenReturn(Optional.of(mockTable));
  }

  @Test
  public void testMigrateWhenFieldDoesNotExist_ShouldAddField() {
    // Setup - Table doesn't have subrange_incremental field
    setupTableWithoutSubrangeIncrementalField();

    // Execute
    Migration034.migrate(mockSession, keyspaceName);

    // Verify - Should execute ALTER TABLE statement
    verify(mockSession, times(1))
        .execute("ALTER TABLE " + tableName + " ADD subrange_incremental boolean");
  }

  @Test
  public void testMigrateWhenFieldAlreadyExists_ShouldNotAddField() {
    // Setup - Table already has subrange_incremental field
    setupTableWithSubrangeIncrementalField();

    // Execute
    Migration034.migrate(mockSession, keyspaceName);

    // Verify - Should not execute ALTER TABLE statement
    verify(mockSession, never()).execute(anyString());
  }

  @Test
  public void testMigrateWithRuntimeException_ShouldCatchAndLog() {
    // Setup - RuntimeException when checking metadata
    when(mockSession.getMetadata()).thenThrow(new RuntimeException("Metadata error"));

    // Execute - should not throw exception
    Migration034.migrate(mockSession, keyspaceName);

    // Verify - Exception should be caught and logged, no ALTER should happen
    verify(mockSession, never()).execute(anyString());
  }

  @Test
  public void testMigrateWithExceptionDuringAlter_ShouldCatchAndLog() {
    // Setup - Exception during ALTER TABLE execution
    setupTableWithoutSubrangeIncrementalField();
    when(mockSession.execute(anyString())).thenThrow(new RuntimeException("ALTER failed"));

    // Execute - should not throw exception
    Migration034.migrate(mockSession, keyspaceName);

    // Verify - Exception should be caught and logged
    verify(mockSession, times(1))
        .execute("ALTER TABLE " + tableName + " ADD subrange_incremental boolean");
  }

  @Test
  public void testMigrateWithMissingKeyspace_ShouldCatchAndLog() {
    // Setup - Keyspace doesn't exist
    when(mockMetadata.getKeyspace(keyspaceName)).thenReturn(Optional.empty());

    // Execute - should not throw exception
    Migration034.migrate(mockSession, keyspaceName);

    // Verify - Exception should be caught and logged, no ALTER should happen
    verify(mockSession, never()).execute(anyString());
  }

  @Test
  public void testMigrateWithMissingTable_ShouldCatchAndLog() {
    // Setup - Table doesn't exist
    when(mockKeyspace.getTable(tableName)).thenReturn(Optional.empty());

    // Execute - should not throw exception
    Migration034.migrate(mockSession, keyspaceName);

    // Verify - Exception should be caught and logged, no ALTER should happen
    verify(mockSession, never()).execute(anyString());
  }

  @Test
  public void testMigrateWithNullKeyspace_ShouldCatchAndLog() {
    // Setup - Null keyspace
    when(mockMetadata.getKeyspace(keyspaceName)).thenReturn(null);

    // Execute - should not throw exception
    Migration034.migrate(mockSession, keyspaceName);

    // Verify - Exception should be caught and logged, no ALTER should happen
    verify(mockSession, never()).execute(anyString());
  }

  @Test
  public void testMigrateWithNullTable_ShouldCatchAndLog() {
    // Setup - Null table
    when(mockKeyspace.getTable(tableName)).thenReturn(null);

    // Execute - should not throw exception
    Migration034.migrate(mockSession, keyspaceName);

    // Verify - Exception should be caught and logged, no ALTER should happen
    verify(mockSession, never()).execute(anyString());
  }

  @Test
  public void testMigrateWithEmptyColumns_ShouldAddField() {
    // Setup - Table exists but has no columns (edge case)
    Map<CqlIdentifier, ColumnMetadata> emptyColumns = new HashMap<>();
    when(mockTable.getColumns()).thenReturn(emptyColumns);

    // Execute
    Migration034.migrate(mockSession, keyspaceName);

    // Verify - Should execute ALTER TABLE statement since field doesn't exist
    verify(mockSession, times(1))
        .execute("ALTER TABLE " + tableName + " ADD subrange_incremental boolean");
  }

  @Test
  public void testMigrateWithOtherColumns_ShouldAddField() {
    // Setup - Table has other columns but not subrange_incremental
    Map<CqlIdentifier, ColumnMetadata> columns = new HashMap<>();
    CqlIdentifier otherId = CqlIdentifier.fromCql("other_field");
    columns.put(otherId, mockColumn);
    when(mockTable.getColumns()).thenReturn(columns);

    // Execute
    Migration034.migrate(mockSession, keyspaceName);

    // Verify - Should execute ALTER TABLE statement
    verify(mockSession, times(1))
        .execute("ALTER TABLE " + tableName + " ADD subrange_incremental boolean");
  }

  @Test
  public void testMigrateWithSimilarlyNamedColumn_ShouldAddField() {
    // Setup - Table has similarly named column but not exact match
    Map<CqlIdentifier, ColumnMetadata> columns = new HashMap<>();
    CqlIdentifier similarId = CqlIdentifier.fromCql("subrange_incremental_other");
    columns.put(similarId, mockColumn);
    when(mockTable.getColumns()).thenReturn(columns);

    // Execute
    Migration034.migrate(mockSession, keyspaceName);

    // Verify - Should execute ALTER TABLE statement since exact field doesn't exist
    verify(mockSession, times(1))
        .execute("ALTER TABLE " + tableName + " ADD subrange_incremental boolean");
  }

  private void setupTableWithoutSubrangeIncrementalField() {
    Map<CqlIdentifier, ColumnMetadata> columns = new HashMap<>();
    CqlIdentifier otherId = CqlIdentifier.fromCql("other_field");
    columns.put(otherId, mockColumn);
    when(mockTable.getColumns()).thenReturn(columns);
  }

  private void setupTableWithSubrangeIncrementalField() {
    Map<CqlIdentifier, ColumnMetadata> columns = new HashMap<>();
    CqlIdentifier subrangeId = CqlIdentifier.fromCql("subrange_incremental");
    columns.put(subrangeId, mockColumn);
    when(mockTable.getColumns()).thenReturn(columns);
  }
}
