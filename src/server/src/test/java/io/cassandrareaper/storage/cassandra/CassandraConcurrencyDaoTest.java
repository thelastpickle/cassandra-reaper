/*
 * Copyright 2024-2024 The Last Pickle Ltd
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

package io.cassandrareaper.storage.cassandra;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.junit.Before;
import org.junit.Test;

/**
 * Regression tests for #1632.
 *
 * <p>When an LWT lead cannot be taken/renewed/released, {@code logFailedLead()} iterates the {@link
 * ResultSet}. The DataStax driver forbids calling {@link ResultSet#wasApplied()} once the rows have
 * been consumed, so calling it a second time (after {@code logFailedLead()}) throws {@code
 * IllegalStateException: This method must be called before consuming all the rows}. These tests
 * assert that {@code wasApplied()} is read exactly once and the not-applied path returns {@code
 * false} without throwing.
 */
public final class CassandraConcurrencyDaoTest {

  private static final UUID REPAIR_ID = UUID.randomUUID();
  private static final UUID SEGMENT_ID = UUID.randomUUID();
  private static final Set<String> REPLICAS = Collections.singleton("127.0.0.1");

  private ResultSet results;
  private CassandraConcurrencyDao dao;

  @Before
  public void setUp() {
    CqlSession session = mock(CqlSession.class);
    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    BoundStatement boundStatement = mock(BoundStatement.class);
    results = mock(ResultSet.class);

    when(session.prepare(any(SimpleStatement.class))).thenReturn(preparedStatement);
    when(session.prepare(anyString())).thenReturn(preparedStatement);
    when(preparedStatement.bind(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(boundStatement);
    when(session.execute(any(BatchStatement.class))).thenReturn(results);

    // First read returns false (not applied); any second read reproduces the driver contract
    // violation reported in #1632 by throwing.
    when(results.wasApplied())
        .thenReturn(false)
        .thenThrow(
            new IllegalStateException("This method must be called before consuming all the rows"));
    when(results.iterator()).thenReturn(Collections.emptyIterator());

    dao = new CassandraConcurrencyDao(Version.parse("4.0.0"), UUID.randomUUID(), session);
  }

  @Test
  public void lockRunningRepairsForNodesReadsWasAppliedOnce() {
    assertFalse(dao.lockRunningRepairsForNodes(REPAIR_ID, SEGMENT_ID, REPLICAS));
    verify(results, times(1)).wasApplied();
  }

  @Test
  public void renewRunningRepairsForNodesReadsWasAppliedOnce() {
    assertFalse(dao.renewRunningRepairsForNodes(REPAIR_ID, SEGMENT_ID, REPLICAS));
    verify(results, times(1)).wasApplied();
  }

  @Test
  public void releaseRunningRepairsForNodesReadsWasAppliedOnce() {
    assertFalse(dao.releaseRunningRepairsForNodes(REPAIR_ID, SEGMENT_ID, REPLICAS));
    verify(results, times(1)).wasApplied();
  }
}
