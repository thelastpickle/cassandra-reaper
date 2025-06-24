/*
 * Copyright 2019-2019 The Last Pickle Ltd
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

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public final class Migration025Test {

  @Mock private CqlSession mockSession;
  @Mock private Metadata mockMetadata;
  @Mock private KeyspaceMetadata mockKeyspace;
  @Mock private TableMetadata mockTable;
  @Mock private ResultSet mockResultSet;
  @Mock private ResultSet mockRunResultSet;
  @Mock private Row mockRow;
  @Mock private Row mockRunRow;
  @Mock private PreparedStatement mockPreparedStatement;
  @Mock private BoundStatement mockBoundStatement;

  private final String keyspaceName = "reaper";
  private final String v1Table = "repair_run_by_cluster";
  private final String v2Table = "repair_run_by_cluster_v2";

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    when(mockSession.getMetadata()).thenReturn(mockMetadata);
    when(mockMetadata.getKeyspace(keyspaceName)).thenReturn(Optional.of(mockKeyspace));
    when(mockSession.prepare(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.bind(any(), any(), any())).thenReturn(mockBoundStatement);
  }

  @Test
  public void testMigrateWithNoV1Table_ShouldAttemptMigrationAndFail() {
    // Setup - V1 table doesn't exist, but migration has bug where it checks Optional != null
    when(mockKeyspace.getTable(v1Table)).thenReturn(Optional.empty());

    // The migration will attempt to SELECT from non-existent table, which should fail
    when(mockSession.execute("SELECT * FROM " + v1Table))
        .thenThrow(new RuntimeException("Table doesn't exist"));

    // Execute
    Migration025.migrate(mockSession, keyspaceName);

    // Verify - Due to bug in migration code, it will attempt to prepare and execute
    // The migration checks Optional != null instead of Optional.isPresent()
    verify(mockSession, times(1)).prepare(anyString());
    verify(mockSession, times(1)).execute("SELECT * FROM " + v1Table);
    // The SELECT fails and is caught by try-catch, so no DROP happens
    verify(mockSession, never()).execute("DROP TABLE " + v1Table);
  }

  @Test
  public void testMigrateWithEmptyV1Table_ShouldDropTable() {
    // Setup - V1 table exists but is empty
    when(mockKeyspace.getTable(v1Table)).thenReturn(Optional.of(mockTable));
    when(mockSession.execute("SELECT * FROM " + v1Table)).thenReturn(mockResultSet);
    when(mockResultSet.iterator()).thenReturn(Collections.emptyIterator());

    // Execute
    Migration025.migrate(mockSession, keyspaceName);

    // Verify - Should prepare statement and drop table
    verify(mockSession, times(1)).prepare(anyString());
    verify(mockSession, times(1)).execute("DROP TABLE " + v1Table);
  }

  @Test
  public void testMigrateWithSingleRow_ShouldMigrateAndDrop() {
    // Setup - V1 table exists with one row
    UUID repairId = UUID.randomUUID();
    String clusterName = "test-cluster";
    String state = "RUNNING";

    setupV1TableWithData(repairId, clusterName);
    setupRepairRunData(repairId, state);

    // Execute
    Migration025.migrate(mockSession, keyspaceName);

    // Verify - Should migrate data and drop table
    verify(mockSession, times(1)).prepare(anyString());
    verify(mockSession, times(1)).execute(mockBoundStatement); // Insert into V2
    verify(mockSession, times(1)).execute("DROP TABLE " + v1Table);
  }

  @Test
  public void testMigrateWithMultipleRows_ShouldMigrateAll() {
    // Setup - V1 table exists with multiple rows
    UUID repairId1 = UUID.randomUUID();
    UUID repairId2 = UUID.randomUUID();
    String clusterName = "test-cluster";
    String state1 = "RUNNING";
    String state2 = "DONE";

    Row row1 = mock(Row.class);
    Row row2 = mock(Row.class);

    when(mockKeyspace.getTable(v1Table)).thenReturn(Optional.of(mockTable));
    when(mockSession.execute("SELECT * FROM " + v1Table)).thenReturn(mockResultSet);
    when(mockResultSet.iterator()).thenReturn(Arrays.asList(row1, row2).iterator());

    // Setup row1
    when(row1.getUuid("id")).thenReturn(repairId1);
    when(row1.getString("cluster_name")).thenReturn(clusterName);

    // Setup row2
    when(row2.getUuid("id")).thenReturn(repairId2);
    when(row2.getString("cluster_name")).thenReturn(clusterName);

    // Setup repair run queries
    ResultSet runResultSet1 = mock(ResultSet.class);
    ResultSet runResultSet2 = mock(ResultSet.class);
    Row runRow1 = mock(Row.class);
    Row runRow2 = mock(Row.class);

    when(mockSession.execute("SELECT distinct state from repair_run where id = " + repairId1))
        .thenReturn(runResultSet1);
    when(mockSession.execute("SELECT distinct state from repair_run where id = " + repairId2))
        .thenReturn(runResultSet2);

    when(runResultSet1.iterator()).thenReturn(Arrays.asList(runRow1).iterator());
    when(runResultSet2.iterator()).thenReturn(Arrays.asList(runRow2).iterator());

    when(runRow1.getString("state")).thenReturn(state1);
    when(runRow2.getString("state")).thenReturn(state2);

    // Execute
    Migration025.migrate(mockSession, keyspaceName);

    // Verify - Should migrate both rows and drop table
    verify(mockSession, times(1)).prepare(anyString());
    verify(mockSession, times(2)).execute(mockBoundStatement); // Two inserts into V2
    verify(mockSession, times(1)).execute("DROP TABLE " + v1Table);
  }

  @Test
  public void testMigrateWithMultipleStatesPerRepair_ShouldMigrateAll() {
    // Setup - One repair run with multiple states (edge case)
    UUID repairId = UUID.randomUUID();
    String clusterName = "test-cluster";
    String state1 = "RUNNING";
    String state2 = "PAUSED";

    setupV1TableWithData(repairId, clusterName);

    // Setup repair run with multiple states
    Row runRow1 = mock(Row.class);
    Row runRow2 = mock(Row.class);

    when(mockSession.execute("SELECT distinct state from repair_run where id = " + repairId))
        .thenReturn(mockRunResultSet);
    when(mockRunResultSet.iterator()).thenReturn(Arrays.asList(runRow1, runRow2).iterator());

    when(runRow1.getString("state")).thenReturn(state1);
    when(runRow2.getString("state")).thenReturn(state2);

    // Execute
    Migration025.migrate(mockSession, keyspaceName);

    // Verify - Should migrate both states and drop table
    verify(mockSession, times(1)).prepare(anyString());
    verify(mockSession, times(2)).execute(mockBoundStatement); // Two inserts for different states
    verify(mockSession, times(1)).execute("DROP TABLE " + v1Table);
  }

  @Test
  public void testMigrateWithRuntimeException_ShouldCatchAndLog() {
    // Setup - RuntimeException during migration
    when(mockKeyspace.getTable(v1Table)).thenReturn(Optional.of(mockTable));
    when(mockSession.execute("SELECT * FROM " + v1Table))
        .thenThrow(new RuntimeException("Test exception"));

    // Execute - should not throw exception
    Migration025.migrate(mockSession, keyspaceName);

    // Verify - Exception should be caught and logged, no drop should happen
    verify(mockSession, never()).execute("DROP TABLE " + v1Table);
  }

  @Test
  public void testMigrateWithExceptionDuringPrepare_ShouldCatchAndLog() {
    // Setup - Exception during prepare statement
    when(mockKeyspace.getTable(v1Table)).thenReturn(Optional.of(mockTable));
    when(mockSession.prepare(anyString())).thenThrow(new RuntimeException("Prepare failed"));

    // Execute - should not throw exception
    Migration025.migrate(mockSession, keyspaceName);

    // Verify - Exception should be caught and logged
    verify(mockSession, never()).execute("DROP TABLE " + v1Table);
  }

  @Test
  public void testMigrateWithExceptionDuringInsert_ShouldCatchAndLog() {
    // Setup - Exception during insert
    UUID repairId = UUID.randomUUID();
    String clusterName = "test-cluster";
    String state = "RUNNING";

    setupV1TableWithData(repairId, clusterName);
    setupRepairRunData(repairId, state);

    when(mockSession.execute(mockBoundStatement)).thenThrow(new RuntimeException("Insert failed"));

    // Execute - should not throw exception
    Migration025.migrate(mockSession, keyspaceName);

    // Verify - Exception should be caught and logged, no drop should happen
    verify(mockSession, never()).execute("DROP TABLE " + v1Table);
  }

  private void setupV1TableWithData(UUID repairId, String clusterName) {
    when(mockKeyspace.getTable(v1Table)).thenReturn(Optional.of(mockTable));
    when(mockSession.execute("SELECT * FROM " + v1Table)).thenReturn(mockResultSet);
    when(mockResultSet.iterator()).thenReturn(Arrays.asList(mockRow).iterator());

    when(mockRow.getUuid("id")).thenReturn(repairId);
    when(mockRow.getString("cluster_name")).thenReturn(clusterName);
  }

  private void setupRepairRunData(UUID repairId, String state) {
    when(mockSession.execute("SELECT distinct state from repair_run where id = " + repairId))
        .thenReturn(mockRunResultSet);
    when(mockRunResultSet.iterator()).thenReturn(Arrays.asList(mockRunRow).iterator());
    when(mockRunRow.getString("state")).thenReturn(state);
  }
}
