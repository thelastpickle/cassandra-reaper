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

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public final class Migration019Test {

  @Mock private CqlSession mockSession;
  @Mock private ResultSet mockResultSet;
  @Mock private Row mockRow;
  @Mock private PreparedStatement mockPreparedStatement;
  @Mock private BoundStatement mockBoundStatement;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    when(mockSession.execute(any(SimpleStatement.class))).thenReturn(mockResultSet);
    when(mockSession.prepare(any(SimpleStatement.class))).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.bind(any(), any(), any(), any())).thenReturn(mockBoundStatement);
  }

  @Test
  public void testMigrateCallsFixRepairRunTimestamps() {
    // Setup - Empty result set so no actual updates happen
    when(mockResultSet.iterator()).thenReturn(Collections.emptyIterator());

    // Execute
    Migration019.migrate(mockSession);

    // Verify - Should call the underlying FixRepairRunTimestamps logic
    // This verifies the migration delegates to FixRepairRunTimestamps.migrate()
    verify(mockSession, times(1)).execute(any(SimpleStatement.class));
    verify(mockSession, times(1)).prepare(any(SimpleStatement.class));
  }

  @Test
  public void testMigrateWithDataCallsFixRepairRunTimestamps() {
    // Setup - Row with invalid timestamps that needs fixing
    UUID repairId = UUID.randomUUID();
    Instant invalidStartTime = Instant.now();

    when(mockRow.getUuid("id")).thenReturn(repairId);
    when(mockRow.getString("state")).thenReturn("RUNNING");
    when(mockRow.getInstant("start_time")).thenReturn(null); // Invalid for RUNNING
    when(mockRow.getInstant("pause_time")).thenReturn(null);
    when(mockRow.getInstant("end_time")).thenReturn(null);

    when(mockResultSet.iterator()).thenReturn(Arrays.asList(mockRow).iterator());

    // Execute
    Migration019.migrate(mockSession);

    // Verify - Should process the data through FixRepairRunTimestamps
    verify(mockSession, times(1)).execute(any(SimpleStatement.class));
    verify(mockSession, times(1)).prepare(any(SimpleStatement.class));
    verify(mockSession, times(1)).executeAsync(mockBoundStatement);
  }
}
