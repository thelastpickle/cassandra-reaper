/*
 * Copyright 2016-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
 * Copyright 2020-2020 DataStax, Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public final class RetryPolicyImplTest {

  @Mock private DriverContext mockDriverContext;
  @Mock private Request mockRequest;
  @Mock private CoordinatorException mockException;

  private RetryPolicyImpl retryPolicy;
  private final String profileName = "test-profile";
  private final String sessionName = "test-session";

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(mockDriverContext.getSessionName()).thenReturn(sessionName);
    retryPolicy = new RetryPolicyImpl(mockDriverContext, profileName);
  }

  @Test
  public void testConstructorWithDriverContext() {
    RetryPolicyImpl policy = new RetryPolicyImpl(mockDriverContext, profileName);
    // Constructor should complete without error
  }

  @Test
  public void testConstructorWithLogPrefix() {
    String logPrefix = "custom-prefix";
    RetryPolicyImpl policy = new RetryPolicyImpl(logPrefix);
    // Constructor should complete without error
  }

  @Test
  public void testOnReadTimeoutWithIdempotentRequest_ShouldRetryNext() {
    // Setup
    when(mockRequest.isIdempotent()).thenReturn(true);
    int retryCount = 5;

    // Execute
    RetryDecision decision =
        retryPolicy.onReadTimeout(mockRequest, ConsistencyLevel.QUORUM, 3, 2, false, retryCount);

    // Verify
    assertEquals(RetryDecision.RETRY_NEXT, decision);
  }

  @Test
  public void testOnReadTimeoutWithNonIdempotentRequest_ShouldRethrow() {
    // Setup
    when(mockRequest.isIdempotent()).thenReturn(false);
    int retryCount = 3;

    // Execute
    RetryDecision decision =
        retryPolicy.onReadTimeout(mockRequest, ConsistencyLevel.QUORUM, 3, 2, false, retryCount);

    // Verify
    assertEquals(RetryDecision.RETHROW, decision);
  }

  @Test
  public void testOnReadTimeoutWithMaxRetriesExceeded_ShouldRethrow() {
    // Setup
    when(mockRequest.isIdempotent()).thenReturn(true);
    int retryCount = 10; // Max retries

    // Execute
    RetryDecision decision =
        retryPolicy.onReadTimeout(mockRequest, ConsistencyLevel.QUORUM, 3, 2, false, retryCount);

    // Verify
    assertEquals(RetryDecision.RETHROW, decision);
  }

  @Test
  public void testOnWriteTimeoutWithIdempotentRequest_ShouldRetryNext() {
    // Setup
    when(mockRequest.isIdempotent()).thenReturn(true);
    int retryCount = 3;

    // Execute
    RetryDecision decision =
        retryPolicy.onWriteTimeout(
            mockRequest, ConsistencyLevel.QUORUM, WriteType.SIMPLE, 3, 2, retryCount);

    // Verify
    assertEquals(RetryDecision.RETRY_NEXT, decision);
  }

  @Test
  public void testOnWriteTimeoutWithNonIdempotentRequest_ShouldRethrow() {
    // Setup
    when(mockRequest.isIdempotent()).thenReturn(false);
    int retryCount = 3;

    // Execute
    RetryDecision decision =
        retryPolicy.onWriteTimeout(
            mockRequest, ConsistencyLevel.QUORUM, WriteType.SIMPLE, 3, 2, retryCount);

    // Verify
    assertEquals(RetryDecision.RETHROW, decision);
  }

  @Test
  public void testOnWriteTimeoutWithMaxRetriesExceeded_ShouldRethrow() {
    // Setup
    when(mockRequest.isIdempotent()).thenReturn(true);
    int retryCount = 10; // Max retries

    // Execute
    RetryDecision decision =
        retryPolicy.onWriteTimeout(
            mockRequest, ConsistencyLevel.QUORUM, WriteType.BATCH, 3, 2, retryCount);

    // Verify
    assertEquals(RetryDecision.RETHROW, decision);
  }

  @Test
  public void testOnUnavailableWithIdempotentRequest_ShouldRetryNext() {
    // Setup
    when(mockRequest.isIdempotent()).thenReturn(true);
    int retryCount = 2;

    // Execute
    RetryDecision decision =
        retryPolicy.onUnavailable(mockRequest, ConsistencyLevel.QUORUM, 3, 1, retryCount);

    // Verify
    assertEquals(RetryDecision.RETRY_NEXT, decision);
  }

  @Test
  public void testOnUnavailableWithNonIdempotentRequest_ShouldRethrow() {
    // Setup
    when(mockRequest.isIdempotent()).thenReturn(false);
    int retryCount = 2;

    // Execute
    RetryDecision decision =
        retryPolicy.onUnavailable(mockRequest, ConsistencyLevel.QUORUM, 3, 1, retryCount);

    // Verify
    assertEquals(RetryDecision.RETHROW, decision);
  }

  @Test
  public void testOnUnavailableWithMaxRetriesExceeded_ShouldRethrow() {
    // Setup
    when(mockRequest.isIdempotent()).thenReturn(true);
    int retryCount = 10; // Max retries

    // Execute
    RetryDecision decision =
        retryPolicy.onUnavailable(mockRequest, ConsistencyLevel.QUORUM, 3, 1, retryCount);

    // Verify
    assertEquals(RetryDecision.RETHROW, decision);
  }

  @Test
  public void testOnRequestAbortedWithIdempotentRequest_ShouldRetryNext() {
    // Setup
    when(mockRequest.isIdempotent()).thenReturn(true);
    int retryCount = 1;
    Throwable throwable = new RuntimeException("Request aborted");

    // Execute
    RetryDecision decision = retryPolicy.onRequestAborted(mockRequest, throwable, retryCount);

    // Verify
    assertEquals(RetryDecision.RETRY_NEXT, decision);
  }

  @Test
  public void testOnRequestAbortedWithNonIdempotentRequest_ShouldRethrow() {
    // Setup
    when(mockRequest.isIdempotent()).thenReturn(false);
    int retryCount = 1;
    Throwable throwable = new RuntimeException("Request aborted");

    // Execute
    RetryDecision decision = retryPolicy.onRequestAborted(mockRequest, throwable, retryCount);

    // Verify
    assertEquals(RetryDecision.RETHROW, decision);
  }

  @Test
  public void testOnRequestAbortedWithMaxRetriesExceeded_ShouldRethrow() {
    // Setup
    when(mockRequest.isIdempotent()).thenReturn(true);
    int retryCount = 10; // Max retries
    Throwable throwable = new RuntimeException("Request aborted");

    // Execute
    RetryDecision decision = retryPolicy.onRequestAborted(mockRequest, throwable, retryCount);

    // Verify
    assertEquals(RetryDecision.RETHROW, decision);
  }

  @Test
  public void testOnErrorResponseWithIdempotentRequest_ShouldRetryNext() {
    // Setup
    when(mockRequest.isIdempotent()).thenReturn(true);
    int retryCount = 1;

    // Execute
    RetryDecision decision = retryPolicy.onErrorResponse(mockRequest, mockException, retryCount);

    // Verify
    assertEquals(RetryDecision.RETRY_NEXT, decision);
  }

  @Test
  public void testOnErrorResponseWithNonIdempotentRequest_ShouldRethrow() {
    // Setup
    when(mockRequest.isIdempotent()).thenReturn(false);
    int retryCount = 1;

    // Execute
    RetryDecision decision = retryPolicy.onErrorResponse(mockRequest, mockException, retryCount);

    // Verify
    assertEquals(RetryDecision.RETHROW, decision);
  }

  @Test
  public void testOnErrorResponseWithMaxRetriesExceeded_ShouldRethrow() {
    // Setup
    when(mockRequest.isIdempotent()).thenReturn(true);
    int retryCount = 10; // Max retries

    // Execute
    RetryDecision decision = retryPolicy.onErrorResponse(mockRequest, mockException, retryCount);

    // Verify
    assertEquals(RetryDecision.RETHROW, decision);
  }

  @Test
  public void testClose() {
    // Should complete without error
    retryPolicy.close();
  }

  @Test
  public void testBackoffDelayWithDifferentRetryCounts() {
    // Test that higher retry counts introduce longer delays
    when(mockRequest.isIdempotent()).thenReturn(true);

    // Test retry count 1 (should have minimal delay)
    long startTime1 = System.currentTimeMillis();
    RetryDecision decision1 =
        retryPolicy.onReadTimeout(mockRequest, ConsistencyLevel.QUORUM, 3, 2, false, 1);
    long duration1 = System.currentTimeMillis() - startTime1;

    // Test retry count 3 (should have longer delay)
    long startTime2 = System.currentTimeMillis();
    RetryDecision decision2 =
        retryPolicy.onReadTimeout(mockRequest, ConsistencyLevel.QUORUM, 3, 2, false, 3);
    long duration2 = System.currentTimeMillis() - startTime2;

    // Verify decisions are correct
    assertEquals(RetryDecision.RETRY_NEXT, decision1);
    assertEquals(RetryDecision.RETRY_NEXT, decision2);

    // Verify that higher retry count takes longer (with some tolerance for timing variations)
    // Base delay is 100ms, retry 1 = 200ms, retry 3 = 800ms
    assertTrue("Higher retry count should take longer", duration2 > duration1);
    assertTrue("Retry count 1 should take at least 150ms", duration1 >= 150);
    assertTrue("Retry count 3 should take at least 600ms", duration2 >= 600);
  }

  @Test
  public void testDifferentWriteTypes() {
    // Test different WriteType values
    when(mockRequest.isIdempotent()).thenReturn(true);

    // Test main WriteType values
    RetryDecision decision1 =
        retryPolicy.onWriteTimeout(mockRequest, ConsistencyLevel.QUORUM, WriteType.SIMPLE, 3, 2, 1);
    assertEquals(RetryDecision.RETRY_NEXT, decision1);

    RetryDecision decision2 =
        retryPolicy.onWriteTimeout(mockRequest, ConsistencyLevel.QUORUM, WriteType.BATCH, 3, 2, 1);
    assertEquals(RetryDecision.RETRY_NEXT, decision2);
  }

  @Test
  public void testDifferentConsistencyLevels() {
    // Test various consistency levels
    when(mockRequest.isIdempotent()).thenReturn(true);

    // Test main ConsistencyLevel values
    RetryDecision decision1 =
        retryPolicy.onReadTimeout(mockRequest, ConsistencyLevel.QUORUM, 3, 2, false, 1);
    assertEquals(RetryDecision.RETRY_NEXT, decision1);

    RetryDecision decision2 =
        retryPolicy.onReadTimeout(mockRequest, ConsistencyLevel.LOCAL_ONE, 3, 2, false, 1);
    assertEquals(RetryDecision.RETRY_NEXT, decision2);

    RetryDecision decision3 =
        retryPolicy.onReadTimeout(mockRequest, ConsistencyLevel.ALL, 3, 2, false, 1);
    assertEquals(RetryDecision.RETRY_NEXT, decision3);
  }
}
