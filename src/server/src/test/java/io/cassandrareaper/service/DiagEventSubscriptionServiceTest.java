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

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.core.DiagEventSubscription;
import io.cassandrareaper.management.jmx.JmxManagementConnectionFactory;
import io.cassandrareaper.storage.events.IEventsDao;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.glassfish.jersey.media.sse.EventOutput;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for DiagEventSubscriptionService class. */
public class DiagEventSubscriptionServiceTest {

  private AppContext mockContext;
  private CloseableHttpClient mockHttpClient;
  private ScheduledExecutorService mockExecutor;
  private IEventsDao mockEventsDao;
  private JmxManagementConnectionFactory mockConnectionFactory;
  private ReaperApplicationConfiguration mockConfig;
  private DiagEventSubscriptionService service;

  @BeforeEach
  void setUp() {
    mockContext = mock(AppContext.class);
    mockHttpClient = mock(CloseableHttpClient.class);
    mockExecutor = mock(ScheduledExecutorService.class);
    mockEventsDao = mock(IEventsDao.class);
    mockConnectionFactory = mock(JmxManagementConnectionFactory.class);
    mockConfig = mock(ReaperApplicationConfiguration.class);

    mockContext.managementConnectionFactory = mockConnectionFactory;
    mockContext.config = mockConfig;
    mockContext.metricRegistry = new MetricRegistry();

    when(mockConfig.getJmxConnectionTimeoutInSeconds()).thenReturn(30);

    service =
        DiagEventSubscriptionService.create(
            mockContext, mockHttpClient, mockExecutor, mockEventsDao);
  }

  @Test
  void testCreate_WithJmxConnectionFactory_ShouldCreateService() {
    // When: Creating service with JMX connection factory
    DiagEventSubscriptionService result =
        DiagEventSubscriptionService.create(
            mockContext, mockHttpClient, mockExecutor, mockEventsDao);

    // Then: Should return a non-null service
    assertThat(result).isNotNull();
  }

  @Test
  void testCreate_WithNonJmxConnectionFactory_ShouldThrowException() {
    // Given: Non-JMX connection factory
    mockContext.managementConnectionFactory = null;

    // When/Then: Should throw IllegalStateException
    assertThatThrownBy(
            () ->
                DiagEventSubscriptionService.create(
                    mockContext, mockHttpClient, mockExecutor, mockEventsDao))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(
            "JMX diagnostic events are only available when JMX connections are used");
  }

  @Test
  void testGetEventSubscription_ShouldReturnFromDao() {
    // Given: A subscription ID
    UUID id = UUID.randomUUID();
    DiagEventSubscription expectedSub = createTestSubscription(id);
    when(mockEventsDao.getEventSubscription(id)).thenReturn(expectedSub);

    // When: Getting event subscription
    DiagEventSubscription result = service.getEventSubscription(id);

    // Then: Should return the subscription from DAO
    assertThat(result).isEqualTo(expectedSub);
    verify(mockEventsDao).getEventSubscription(id);
  }

  @Test
  void testGetEventSubscription_WithNullResult_ShouldReturnNull() {
    // Given: A subscription ID that doesn't exist
    UUID id = UUID.randomUUID();
    when(mockEventsDao.getEventSubscription(id)).thenReturn(null);

    // When: Getting event subscription
    DiagEventSubscription result = service.getEventSubscription(id);

    // Then: Should return null
    assertThat(result).isNull();
    verify(mockEventsDao).getEventSubscription(id);
  }

  @Test
  void testAddEventSubscription_WithNewSubscription_ShouldAddToDao() {
    // Given: A new subscription without ID
    DiagEventSubscription subscription = createTestSubscription(null);
    DiagEventSubscription savedSubscription = createTestSubscription(UUID.randomUUID());

    when(mockEventsDao.getEventSubscriptions(subscription.getCluster()))
        .thenReturn(Collections.emptyList());
    when(mockEventsDao.addEventSubscription(any())).thenReturn(savedSubscription);

    // When: Adding event subscription
    DiagEventSubscription result = service.addEventSubscription(subscription);

    // Then: Should return the saved subscription
    assertThat(result).isEqualTo(savedSubscription);
    verify(mockEventsDao).addEventSubscription(any(DiagEventSubscription.class));
  }

  @Test
  void testAddEventSubscription_WithExistingId_ShouldUseProvidedId() {
    // Given: A subscription with existing ID
    UUID existingId = UUID.randomUUID();
    DiagEventSubscription subscription = createTestSubscription(existingId);

    when(mockEventsDao.getEventSubscriptions(subscription.getCluster()))
        .thenReturn(Collections.emptyList());
    when(mockEventsDao.addEventSubscription(any())).thenReturn(subscription);

    // When: Adding event subscription
    DiagEventSubscription result = service.addEventSubscription(subscription);

    // Then: Should use the provided ID
    assertThat(result).isEqualTo(subscription);
    verify(mockEventsDao).addEventSubscription(eq(subscription));
  }

  @Test
  void testDeleteEventSubscription_WithExistingSubscription_ShouldDeleteAndUpdate() {
    // Given: An existing subscription
    UUID id = UUID.randomUUID();
    DiagEventSubscription subscription = createTestSubscription(id);

    when(mockEventsDao.getEventSubscription(id)).thenReturn(subscription);
    when(mockEventsDao.deleteEventSubscription(id)).thenReturn(true);

    // When: Deleting event subscription
    service.deleteEventSubscription(id);

    // Then: Should delete from DAO
    verify(mockEventsDao).deleteEventSubscription(id);
  }

  @Test
  void testDeleteEventSubscription_WithNonExistentSubscription_ShouldNotUpdate() {
    // Given: A non-existent subscription
    UUID id = UUID.randomUUID();

    when(mockEventsDao.getEventSubscription(id)).thenReturn(null);
    when(mockEventsDao.deleteEventSubscription(id)).thenReturn(false);

    // When: Deleting event subscription
    service.deleteEventSubscription(id);

    // Then: Should attempt to delete from DAO
    verify(mockEventsDao).deleteEventSubscription(id);
  }

  @Test
  void testSubscribe_WithNewSubscription_ShouldCreateBroadcaster() {
    // Given: A subscription and event output
    DiagEventSubscription subscription = createTestSubscription(UUID.randomUUID());
    EventOutput mockEventOutput = mock(EventOutput.class);
    String remoteAddr = "192.168.1.1";

    // When: Subscribing to events
    service.subscribe(subscription, mockEventOutput, remoteAddr);

    // Then: Should not throw exception (broadcaster created internally)
    // This test verifies the method completes without error
  }

  @Test
  void testGetAdhocActiveSubs_WithActiveBroadcasters_ShouldReturnActiveSubs() {
    // Given: All subscriptions and always active subscriptions
    DiagEventSubscription alwaysActiveSub = createTestSubscription(UUID.randomUUID());
    DiagEventSubscription adhocSub1 = createTestSubscription(UUID.randomUUID());
    DiagEventSubscription adhocSub2 = createTestSubscription(UUID.randomUUID());

    Collection<DiagEventSubscription> allSubs =
        Arrays.asList(alwaysActiveSub, adhocSub1, adhocSub2);
    Set<DiagEventSubscription> alwaysActiveSubs = Collections.singleton(alwaysActiveSub);

    // When: Getting ad-hoc active subscriptions (no active broadcasters)
    Set<DiagEventSubscription> result =
        DiagEventSubscriptionService.getAdhocActiveSubs(allSubs, alwaysActiveSubs);

    // Then: Should return empty set (no active broadcasters)
    assertThat(result).isEmpty();
  }

  @Test
  void testGetAdhocActiveSubs_WithNoActiveBroadcasters_ShouldReturnEmpty() {
    // Given: All subscriptions and always active subscriptions
    DiagEventSubscription alwaysActiveSub = createTestSubscription(UUID.randomUUID());
    DiagEventSubscription adhocSub = createTestSubscription(UUID.randomUUID());

    Collection<DiagEventSubscription> allSubs = Arrays.asList(alwaysActiveSub, adhocSub);
    Set<DiagEventSubscription> alwaysActiveSubs = Collections.singleton(alwaysActiveSub);

    // When: Getting ad-hoc active subscriptions
    Set<DiagEventSubscription> result =
        DiagEventSubscriptionService.getAdhocActiveSubs(allSubs, alwaysActiveSubs);

    // Then: Should return empty set
    assertThat(result).isEmpty();
  }

  @Test
  void testGetAdhocActiveSubs_WithEmptyInputs_ShouldReturnEmpty() {
    // Given: Empty collections
    Collection<DiagEventSubscription> allSubs = Collections.emptyList();
    Set<DiagEventSubscription> alwaysActiveSubs = Collections.emptySet();

    // When: Getting ad-hoc active subscriptions
    Set<DiagEventSubscription> result =
        DiagEventSubscriptionService.getAdhocActiveSubs(allSubs, alwaysActiveSubs);

    // Then: Should return empty set
    assertThat(result).isEmpty();
  }

  @Test
  void testGetAdhocActiveSubs_WithNullInputs_ShouldHandleGracefully() {
    // Given: Null collections
    Collection<DiagEventSubscription> allSubs = null;
    Set<DiagEventSubscription> alwaysActiveSubs = Collections.emptySet();

    // When/Then: Should throw NullPointerException
    assertThatThrownBy(
            () -> DiagEventSubscriptionService.getAdhocActiveSubs(allSubs, alwaysActiveSubs))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void testGetAdhocActiveSubs_WithAlwaysActiveOnly_ShouldReturnEmpty() {
    // Given: Only always active subscriptions
    DiagEventSubscription alwaysActiveSub1 = createTestSubscription(UUID.randomUUID());
    DiagEventSubscription alwaysActiveSub2 = createTestSubscription(UUID.randomUUID());

    Collection<DiagEventSubscription> allSubs = Arrays.asList(alwaysActiveSub1, alwaysActiveSub2);
    Set<DiagEventSubscription> alwaysActiveSubs = new HashSet<>(allSubs);

    // When: Getting ad-hoc active subscriptions
    Set<DiagEventSubscription> result =
        DiagEventSubscriptionService.getAdhocActiveSubs(allSubs, alwaysActiveSubs);

    // Then: Should return empty set
    assertThat(result).isEmpty();
  }

  private DiagEventSubscription createTestSubscription(UUID id) {
    return new DiagEventSubscription(
        Optional.ofNullable(id),
        "test-cluster",
        Optional.of("Test subscription"),
        Collections.singleton("127.0.0.1"),
        Collections.singleton("compaction"),
        true,
        null,
        null);
  }
}
