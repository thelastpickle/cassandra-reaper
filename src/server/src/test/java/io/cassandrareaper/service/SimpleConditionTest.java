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

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for SimpleCondition class. */
public class SimpleConditionTest {

  private SimpleCondition condition;

  @BeforeEach
  void setUp() {
    condition = new SimpleCondition();
  }

  @Test
  void testInitialState_ShouldNotBeSignaled() {
    // Then: Initial state should not be signaled
    assertThat(condition.isSignaled()).isFalse();
  }

  @Test
  void testSignalAll_ShouldSetCondition() {
    // Given: Condition is not signaled
    assertThat(condition.isSignaled()).isFalse();

    // When: Signaling all
    condition.signalAll();

    // Then: Condition should be signaled
    assertThat(condition.isSignaled()).isTrue();
  }

  @Test
  void testReset_ShouldClearSignal() {
    // Given: Condition is signaled
    condition.signalAll();
    assertThat(condition.isSignaled()).isTrue();

    // When: Resetting
    condition.reset();

    // Then: Condition should not be signaled
    assertThat(condition.isSignaled()).isFalse();
  }

  @Test
  void testAwait_WithSignaledCondition_ShouldReturnImmediately() throws InterruptedException {
    // Given: Condition is signaled
    condition.signalAll();

    // When: Awaiting on already signaled condition
    long startTime = System.currentTimeMillis();
    condition.await();
    long elapsedTime = System.currentTimeMillis() - startTime;

    // Then: Should return immediately
    assertThat(elapsedTime).isLessThan(100);
    assertThat(condition.isSignaled()).isTrue();
  }

  @Test
  void testAwait_WithTimeout_ShouldReturnFalseOnTimeout() throws InterruptedException {
    // Given: Condition is not signaled
    assertThat(condition.isSignaled()).isFalse();

    // When: Awaiting with timeout
    long startTime = System.currentTimeMillis();
    boolean result = condition.await(200, TimeUnit.MILLISECONDS);
    long elapsedTime = System.currentTimeMillis() - startTime;

    // Then: Should timeout and return false
    assertThat(result).isFalse();
    assertThat(elapsedTime).isGreaterThanOrEqualTo(200);
    assertThat(elapsedTime).isLessThan(300);
  }

  @Test
  void testAwait_WithTimeout_ShouldReturnTrueWhenSignaled() throws InterruptedException {
    // Given: Condition will be signaled after a delay
    new Thread(
            () -> {
              try {
                Thread.sleep(100);
                condition.signalAll();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            })
        .start();

    // When: Awaiting with sufficient timeout
    boolean result = condition.await(500, TimeUnit.MILLISECONDS);

    // Then: Should return true when signaled
    assertThat(result).isTrue();
    assertThat(condition.isSignaled()).isTrue();
  }

  @Test
  void testAwait_MultipleThreads_ShouldAllBeNotified() throws InterruptedException {
    // Given: Multiple threads waiting
    int threadCount = 5;
    CountDownLatch startLatch = new CountDownLatch(threadCount);
    CountDownLatch endLatch = new CountDownLatch(threadCount);
    AtomicBoolean[] threadResults = new AtomicBoolean[threadCount];

    for (int i = 0; i < threadCount; i++) {
      final int index = i;
      threadResults[i] = new AtomicBoolean(false);
      new Thread(
              () -> {
                try {
                  startLatch.countDown();
                  condition.await();
                  threadResults[index].set(true);
                  endLatch.countDown();
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              })
          .start();
    }

    // Wait for all threads to start waiting
    startLatch.await();
    Thread.sleep(100); // Give threads time to enter await

    // When: Signaling all
    condition.signalAll();

    // Then: All threads should be notified
    boolean allNotified = endLatch.await(1, TimeUnit.SECONDS);
    assertThat(allNotified).isTrue();
    for (AtomicBoolean result : threadResults) {
      assertThat(result.get()).isTrue();
    }
  }

  @Test
  void testSignal_ShouldThrowUnsupportedOperationException() {
    // When/Then: Calling signal should throw exception
    assertThatThrownBy(() -> condition.signal()).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void testAwaitUninterruptibly_ShouldThrowUnsupportedOperationException() {
    // When/Then: Calling awaitUninterruptibly should throw exception
    assertThatThrownBy(() -> condition.awaitUninterruptibly())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void testAwaitNanos_ShouldThrowUnsupportedOperationException() {
    // When/Then: Calling awaitNanos should throw exception
    assertThatThrownBy(() -> condition.awaitNanos(1000))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void testAwaitUntil_ShouldThrowUnsupportedOperationException() {
    // When/Then: Calling awaitUntil should throw exception
    assertThatThrownBy(() -> condition.awaitUntil(new Date()))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void testSignalAll_MultipleTimes_ShouldRemainSignaled() {
    // When: Signaling multiple times
    condition.signalAll();
    condition.signalAll();
    condition.signalAll();

    // Then: Should remain signaled
    assertThat(condition.isSignaled()).isTrue();
  }

  @Test
  void testAwait_AfterSignal_ShouldWorkAsDesired() throws InterruptedException {
    // Given: Signal is called before await
    condition.signalAll();

    // When: Calling await after signal
    long startTime = System.currentTimeMillis();
    condition.await();
    long elapsedTime = System.currentTimeMillis() - startTime;

    // Then: Should return immediately (no lost notify problem)
    assertThat(elapsedTime).isLessThan(100);
    assertThat(condition.isSignaled()).isTrue();
  }

  @Test
  void testConcurrentSignalAndAwait() throws InterruptedException {
    // Given: Multiple threads performing different operations
    CountDownLatch latch = new CountDownLatch(2);
    AtomicBoolean awaitReturned = new AtomicBoolean(false);

    // Thread 1: Waits on condition
    Thread waiter =
        new Thread(
            () -> {
              try {
                boolean result = condition.await(2, TimeUnit.SECONDS);
                awaitReturned.set(result);
                latch.countDown();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });

    // Thread 2: Signals after a delay
    Thread signaler =
        new Thread(
            () -> {
              try {
                Thread.sleep(200);
                condition.signalAll();
                latch.countDown();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });

    // When: Starting both threads
    waiter.start();
    signaler.start();

    // Then: Both should complete successfully
    boolean completed = latch.await(3, TimeUnit.SECONDS);
    assertThat(completed).isTrue();
    assertThat(awaitReturned.get()).isTrue();
    assertThat(condition.isSignaled()).isTrue();
  }
}
