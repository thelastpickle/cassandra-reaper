/*
 * Copyright 2016-2017 Spotify AB Copyright 2016-2019 The Last Pickle Ltd Copyright 2020-2020
 * DataStax, Inc.
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

package io.cassandrareaper.storage.operations;

import io.cassandrareaper.storage.OpType;

import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public final class CassandraOperationsDaoTest {

  private static final DateTimeFormatter TIME_BUCKET_FORMATTER =
      DateTimeFormat.forPattern("yyyyMMddHHmm");

  @Mock private CqlSession mockSession;
  @Mock private PreparedStatement insertOperationsPrepStmt;
  @Mock private PreparedStatement listOperationsForNodePrepStmt;
  @Mock private BoundStatement boundStatement;
  @Mock private AsyncResultSet asyncResultSet;
  @Mock private Row row;

  private CassandraOperationsDao operationsDao;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    // Mock the prepared statement creation
    when(mockSession.prepare(anyString()))
        .thenReturn(insertOperationsPrepStmt)
        .thenReturn(listOperationsForNodePrepStmt);

    operationsDao = new CassandraOperationsDao(mockSession);
  }

  @Test
  public void testConstructorPreparesStatements() {
    // Verify that the constructor prepares the required statements
    verify(mockSession, times(2)).prepare(anyString());
  }

  @Test
  public void testStoreOperationsWithStreamingType() {
    // Setup
    String clusterName = "testCluster";
    OpType operationType = OpType.OP_STREAMING;
    String host = "127.0.0.1";
    String operationsJson = "{'streaming': 'data'}";

    when(insertOperationsPrepStmt.bind(any(), any(), any(), any(), any(), any()))
        .thenReturn(boundStatement);

    // Execute
    operationsDao.storeOperations(clusterName, operationType, host, operationsJson);

    // Verify
    verify(mockSession).executeAsync(boundStatement);

    // Capture and verify the arguments passed to bind
    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(insertOperationsPrepStmt)
        .bind(
            captor.capture(),
            captor.capture(),
            captor.capture(),
            captor.capture(),
            captor.capture(),
            captor.capture());

    assertEquals(clusterName, captor.getAllValues().get(0));
    assertEquals(operationType.getName(), captor.getAllValues().get(1));
    // Time bucket should be current time formatted
    assertTrue(captor.getAllValues().get(2) instanceof String);
    assertEquals(host, captor.getAllValues().get(3));
    assertTrue(captor.getAllValues().get(4) instanceof Instant);
    assertEquals(operationsJson, captor.getAllValues().get(5));
  }

  @Test
  public void testStoreOperationsWithCompactionType() {
    // Setup
    String clusterName = "testCluster";
    OpType operationType = OpType.OP_COMPACTION;
    String host = "192.168.1.1";
    String operationsJson = "{'compaction': 'info'}";

    when(insertOperationsPrepStmt.bind(any(), any(), any(), any(), any(), any()))
        .thenReturn(boundStatement);

    // Execute
    operationsDao.storeOperations(clusterName, operationType, host, operationsJson);

    // Verify
    verify(mockSession).executeAsync(boundStatement);

    // Capture and verify the operation type
    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(insertOperationsPrepStmt)
        .bind(
            captor.capture(),
            captor.capture(),
            captor.capture(),
            captor.capture(),
            captor.capture(),
            captor.capture());

    assertEquals("compaction", captor.getAllValues().get(1));
  }

  @Test
  public void testListOperationsReturnsDataFromCurrentTimeBucket() {
    // Setup
    String clusterName = "testCluster";
    OpType operationType = OpType.OP_STREAMING;
    String host = "127.0.0.1";
    String expectedData = "{'operations': 'current'}";

    when(listOperationsForNodePrepStmt.bind(any(), any(), any(), any())).thenReturn(boundStatement);

    CompletionStage<AsyncResultSet> currentStage =
        CompletableFuture.completedFuture(asyncResultSet);
    CompletionStage<AsyncResultSet> previousStage =
        CompletableFuture.completedFuture(mock(AsyncResultSet.class));

    when(mockSession.executeAsync(boundStatement))
        .thenReturn(currentStage)
        .thenReturn(previousStage);

    when(asyncResultSet.currentPage()).thenReturn(Arrays.asList(row));
    when(row.getString("data")).thenReturn(expectedData);

    // Execute
    String result = operationsDao.listOperations(clusterName, operationType, host);

    // Verify
    assertEquals(expectedData, result);
    verify(mockSession, times(2)).executeAsync(boundStatement);
  }

  @Test
  public void testListOperationsReturnsDataFromPreviousTimeBucket() {
    // Setup
    String clusterName = "testCluster";
    OpType operationType = OpType.OP_COMPACTION;
    String host = "127.0.0.1";
    String expectedData = "{'operations': 'previous'}";

    when(listOperationsForNodePrepStmt.bind(any(), any(), any(), any())).thenReturn(boundStatement);

    // Current time bucket returns empty
    AsyncResultSet emptyAsyncResultSet = mock(AsyncResultSet.class);
    when(emptyAsyncResultSet.currentPage()).thenReturn(Arrays.asList());
    CompletionStage<AsyncResultSet> currentStage =
        CompletableFuture.completedFuture(emptyAsyncResultSet);

    // Previous time bucket returns data
    when(asyncResultSet.currentPage()).thenReturn(Arrays.asList(row));
    when(row.getString("data")).thenReturn(expectedData);
    CompletionStage<AsyncResultSet> previousStage =
        CompletableFuture.completedFuture(asyncResultSet);

    when(mockSession.executeAsync(boundStatement))
        .thenReturn(currentStage)
        .thenReturn(previousStage);

    // Execute
    String result = operationsDao.listOperations(clusterName, operationType, host);

    // Verify
    assertEquals(expectedData, result);
    verify(mockSession, times(2)).executeAsync(boundStatement);
  }

  @Test
  public void testListOperationsReturnsEmptyStringWhenNoData() {
    // Setup
    String clusterName = "testCluster";
    OpType operationType = OpType.OP_STREAMING;
    String host = "127.0.0.1";

    when(listOperationsForNodePrepStmt.bind(any(), any(), any(), any())).thenReturn(boundStatement);

    // Both current and previous time buckets return empty
    AsyncResultSet emptyAsyncResultSet = mock(AsyncResultSet.class);
    when(emptyAsyncResultSet.currentPage()).thenReturn(Arrays.asList());
    CompletionStage<AsyncResultSet> emptyStage =
        CompletableFuture.completedFuture(emptyAsyncResultSet);

    when(mockSession.executeAsync(boundStatement)).thenReturn(emptyStage).thenReturn(emptyStage);

    // Execute
    String result = operationsDao.listOperations(clusterName, operationType, host);

    // Verify
    assertEquals("", result);
    verify(mockSession, times(2)).executeAsync(boundStatement);
  }

  @Test
  public void testListOperationsQueriesCorrectTimeBuckets() {
    // Setup
    String clusterName = "testCluster";
    OpType operationType = OpType.OP_STREAMING;
    String host = "127.0.0.1";

    when(listOperationsForNodePrepStmt.bind(any(), any(), any(), any())).thenReturn(boundStatement);

    AsyncResultSet emptyAsyncResultSet = mock(AsyncResultSet.class);
    when(emptyAsyncResultSet.currentPage()).thenReturn(Arrays.asList());
    CompletionStage<AsyncResultSet> emptyStage =
        CompletableFuture.completedFuture(emptyAsyncResultSet);

    when(mockSession.executeAsync(boundStatement)).thenReturn(emptyStage);

    // Execute
    operationsDao.listOperations(clusterName, operationType, host);

    // Verify the correct parameters are passed
    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(listOperationsForNodePrepStmt, times(2))
        .bind(captor.capture(), captor.capture(), captor.capture(), captor.capture());

    // First call should be for current time bucket
    assertEquals(clusterName, captor.getAllValues().get(0));
    assertEquals(operationType.getName(), captor.getAllValues().get(1));
    String currentTimeBucket = DateTime.now().toString(TIME_BUCKET_FORMATTER);
    assertEquals(currentTimeBucket, captor.getAllValues().get(2));
    assertEquals(host, captor.getAllValues().get(3));

    // Second call should be for previous minute time bucket
    assertEquals(clusterName, captor.getAllValues().get(4));
    assertEquals(operationType.getName(), captor.getAllValues().get(5));
    String previousTimeBucket = DateTime.now().minusMinutes(1).toString(TIME_BUCKET_FORMATTER);
    assertEquals(previousTimeBucket, captor.getAllValues().get(6));
    assertEquals(host, captor.getAllValues().get(7));
  }

  @Test
  public void testPurgeNodeOperationsDoesNothing() {
    // This method is currently a no-op, just verify it can be called without issues
    operationsDao.purgeNodeOperations();
    // No verification needed as it's a no-op
  }

  @Test
  public void testStoreOperationsWithNullValues() {
    // Test edge cases with null values - should not break
    when(insertOperationsPrepStmt.bind(any(), any(), any(), any(), any(), any()))
        .thenReturn(boundStatement);

    operationsDao.storeOperations(null, OpType.OP_STREAMING, null, null);

    verify(mockSession).executeAsync(boundStatement);
    verify(insertOperationsPrepStmt).bind(any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testListOperationsWithNullValues() {
    // Test edge cases with null values
    when(listOperationsForNodePrepStmt.bind(any(), any(), any(), any())).thenReturn(boundStatement);

    AsyncResultSet emptyAsyncResultSet = mock(AsyncResultSet.class);
    when(emptyAsyncResultSet.currentPage()).thenReturn(Arrays.asList());
    CompletionStage<AsyncResultSet> emptyStage =
        CompletableFuture.completedFuture(emptyAsyncResultSet);

    when(mockSession.executeAsync(boundStatement)).thenReturn(emptyStage);

    String result = operationsDao.listOperations(null, OpType.OP_COMPACTION, null);

    assertEquals("", result);
    verify(mockSession, times(2)).executeAsync(boundStatement);
  }
}
