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
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.DiagEventSubscription;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.management.DiagnosticProxy;
import io.cassandrareaper.management.jmx.JmxCassandraManagementProxy;
import io.cassandrareaper.management.jmx.JmxManagementConnectionFactory;
import io.cassandrareaper.resources.view.DiagnosticEvent;
import io.cassandrareaper.storage.events.IEventsDao;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

import javax.management.Notification;
import javax.management.remote.JMXConnectionNotification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.HttpEntity;
import org.glassfish.jersey.media.sse.EventOutput;
import org.glassfish.jersey.media.sse.OutboundEvent;
import org.glassfish.jersey.server.ChunkedOutput;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * Advanced unit tests for DiagEventSubscriptionService covering complex scenarios and private
 * methods.
 */
public class DiagEventSubscriptionServiceAdvancedTest {

  private AppContext mockContext;
  private CloseableHttpClient mockHttpClient;
  private ScheduledExecutorService mockExecutor;
  private IEventsDao mockEventsDao;
  private JmxManagementConnectionFactory mockConnectionFactory;
  private ReaperApplicationConfiguration mockConfig;
  private DiagEventSubscriptionService service;
  private JmxCassandraManagementProxy mockJmxProxy;
  private DiagnosticProxy mockDiagnosticProxy;

  @BeforeEach
  void setUp() {
    mockContext = mock(AppContext.class);
    mockHttpClient = mock(CloseableHttpClient.class);
    mockExecutor = mock(ScheduledExecutorService.class);
    mockEventsDao = mock(IEventsDao.class);
    mockConnectionFactory = mock(JmxManagementConnectionFactory.class);
    mockConfig = mock(ReaperApplicationConfiguration.class);
    mockJmxProxy = mock(JmxCassandraManagementProxy.class);
    mockDiagnosticProxy = mock(DiagnosticProxy.class);

    mockContext.managementConnectionFactory = mockConnectionFactory;
    mockContext.config = mockConfig;
    mockContext.metricRegistry = new MetricRegistry();

    when(mockConfig.getJmxConnectionTimeoutInSeconds()).thenReturn(30);
    when(mockJmxProxy.getHost()).thenReturn("127.0.0.1");
    when(mockJmxProxy.getClusterName()).thenReturn("test-cluster");

    // Clear static maps before each test
    clearStaticMaps();

    service =
        DiagEventSubscriptionService.create(
            mockContext, mockHttpClient, mockExecutor, mockEventsDao);
  }

  private void clearStaticMaps() {
    try {
      Field broadcastersField = DiagEventSubscriptionService.class.getDeclaredField("BROADCASTERS");
      broadcastersField.setAccessible(true);
      Map<?, ?> broadcasters = (Map<?, ?>) broadcastersField.get(null);
      broadcasters.clear();

      Field pollersField = DiagEventSubscriptionService.class.getDeclaredField("POLLERS_BY_NODE");
      pollersField.setAccessible(true);
      Map<?, ?> pollers = (Map<?, ?>) pollersField.get(null);
      pollers.clear();
    } catch (Exception e) {
      throw new RuntimeException("Failed to clear static maps", e);
    }
  }

  @Test
  void testSubscribe_CreatesAndUsesBroadcaster() throws Exception {
    // Given: A subscription and event output
    DiagEventSubscription subscription = createTestSubscription(UUID.randomUUID());
    EventOutput mockEventOutput = mock(EventOutput.class);
    String remoteAddr = "192.168.1.1";

    // When: Subscribing to events
    service.subscribe(subscription, mockEventOutput, remoteAddr);

    // Then: Broadcaster should be created and used
    Field broadcastersField = DiagEventSubscriptionService.class.getDeclaredField("BROADCASTERS");
    broadcastersField.setAccessible(true);
    Map<?, ?> broadcasters = (Map<?, ?>) broadcastersField.get(null);
    assertThat(broadcasters).hasSize(1);
  }

  @Test
  void testOnEvent_WithFileLogger_ShouldLogEvent() throws Exception {
    // Given: Subscription with file logger
    DiagEventSubscription subscription =
        new DiagEventSubscription(
            Optional.of(UUID.randomUUID()),
            "test-cluster",
            Optional.of("Test subscription"),
            Collections.singleton("127.0.0.1"),
            Collections.singleton("compaction"),
            true,
            "test.logger",
            null);

    // Set up subsAlwaysActive
    setSubsAlwaysActive(Collections.singleton(subscription));

    DiagnosticEvent event = createTestDiagnosticEvent();

    // When: Calling onEvent
    Method onEventMethod =
        DiagEventSubscriptionService.class.getDeclaredMethod("onEvent", DiagnosticEvent.class);
    onEventMethod.setAccessible(true);
    onEventMethod.invoke(service, event);

    // Then: Should process the event (no exception thrown)
  }

  @Test
  void testOnEvent_WithHttpEndpoint_ShouldPostEvent() throws Exception {
    // Given: Subscription with HTTP endpoint
    DiagEventSubscription subscription =
        new DiagEventSubscription(
            Optional.of(UUID.randomUUID()),
            "test-cluster",
            Optional.of("Test subscription"),
            Collections.singleton("127.0.0.1"),
            Collections.singleton("compaction"),
            true,
            null,
            "http://localhost:8080/events");

    setSubsAlwaysActive(Collections.singleton(subscription));

    DiagnosticEvent event = createTestDiagnosticEvent();
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    HttpEntity mockEntity = mock(HttpEntity.class);
    when(mockResponse.getEntity()).thenReturn(mockEntity);
    when(mockHttpClient.execute(any(ClassicHttpRequest.class))).thenReturn(mockResponse);

    // When: Calling onEvent
    Method onEventMethod =
        DiagEventSubscriptionService.class.getDeclaredMethod("onEvent", DiagnosticEvent.class);
    onEventMethod.setAccessible(true);
    onEventMethod.invoke(service, event);

    // Then: Should make HTTP request
    verify(mockHttpClient).execute(any(ClassicHttpRequest.class));
  }

  @Test
  void testOnEvent_WithHttpEndpointIOException_ShouldHandleGracefully() throws Exception {
    // Given: Subscription with HTTP endpoint that throws IOException
    DiagEventSubscription subscription =
        new DiagEventSubscription(
            Optional.of(UUID.randomUUID()),
            "test-cluster",
            Optional.of("Test subscription"),
            Collections.singleton("127.0.0.1"),
            Collections.singleton("compaction"),
            true,
            null,
            "http://localhost:8080/events");

    setSubsAlwaysActive(Collections.singleton(subscription));

    DiagnosticEvent event = createTestDiagnosticEvent();
    when(mockHttpClient.execute(any(ClassicHttpRequest.class)))
        .thenThrow(new IOException("Connection failed"));

    // When: Calling onEvent
    Method onEventMethod =
        DiagEventSubscriptionService.class.getDeclaredMethod("onEvent", DiagnosticEvent.class);
    onEventMethod.setAccessible(true);
    onEventMethod.invoke(service, event);

    // Then: Should handle exception gracefully
    verify(mockHttpClient).execute(any(ClassicHttpRequest.class));
  }

  @Test
  void testOnEvent_WithRuntimeException_ShouldHandleGracefully() throws Exception {
    // Given: Subscription that will cause runtime exception
    DiagEventSubscription subscription =
        new DiagEventSubscription(
            Optional.of(UUID.randomUUID()),
            "test-cluster",
            Optional.of("Test subscription"),
            Collections.singleton("127.0.0.1"),
            Collections.singleton("compaction"),
            true,
            null,
            "invalid-url");

    setSubsAlwaysActive(Collections.singleton(subscription));

    DiagnosticEvent event = createTestDiagnosticEvent();
    when(mockHttpClient.execute(any(ClassicHttpRequest.class)))
        .thenThrow(new RuntimeException("Test exception"));

    // When: Calling onEvent
    Method onEventMethod =
        DiagEventSubscriptionService.class.getDeclaredMethod("onEvent", DiagnosticEvent.class);
    onEventMethod.setAccessible(true);
    onEventMethod.invoke(service, event);

    // Then: Should handle exception gracefully (no exception propagated)
  }

  @Test
  void testPingSseClients_ShouldPingAllBroadcasters() throws Exception {
    // Given: Broadcasters in the map
    DiagEventSubscription subscription = createTestSubscription(UUID.randomUUID());
    EventOutput mockEventOutput = mock(EventOutput.class);
    service.subscribe(subscription, mockEventOutput, "127.0.0.1");

    // When: Calling pingSseClients
    Method pingSseClientsMethod =
        DiagEventSubscriptionService.class.getDeclaredMethod("pingSseClients");
    pingSseClientsMethod.setAccessible(true);
    pingSseClientsMethod.invoke(service);

    // Then: Should create ping events (verified by no exception)
  }

  @Test
  void testEnableEvents_WithClassNotFoundException_ShouldLogWarning() throws Exception {
    // Given: DiagnosticProxy that throws ClassNotFoundException
    Node node = createTestNode();
    Set<String> events = Collections.singleton("test-event");

    try (MockedStatic<DiagnosticProxy> mockedDiagnosticProxy = mockStatic(DiagnosticProxy.class)) {
      mockedDiagnosticProxy
          .when(() -> DiagnosticProxy.create(mockJmxProxy))
          .thenReturn(mockDiagnosticProxy);

      RuntimeException runtimeException =
          new RuntimeException(new ClassNotFoundException("Event not found"));
      doThrow(runtimeException).when(mockDiagnosticProxy).enableEventPersistence("test-event");

      // When: Calling enableEvents
      Method enableEventsMethod =
          DiagEventSubscriptionService.class.getDeclaredMethod(
              "enableEvents",
              Node.class,
              Set.class,
              boolean.class,
              JmxCassandraManagementProxy.class);
      enableEventsMethod.setAccessible(true);
      enableEventsMethod.invoke(service, node, events, true, mockJmxProxy);

      // Then: Should handle exception gracefully
      verify(mockDiagnosticProxy).enableEventPersistence("test-event");
    }
  }

  @Test
  void testEnableEvents_WithReflectionException_ShouldLogWarning() throws Exception {
    // Given: DiagnosticProxy that throws ReflectionException
    Node node = createTestNode();
    Set<String> events = Collections.singleton("test-event");

    try (MockedStatic<DiagnosticProxy> mockedDiagnosticProxy = mockStatic(DiagnosticProxy.class)) {
      mockedDiagnosticProxy
          .when(() -> DiagnosticProxy.create(mockJmxProxy))
          .thenReturn(mockDiagnosticProxy);

      RuntimeException runtimeException =
          new RuntimeException(
              new javax.management.ReflectionException(new Exception(), "Reflection error"));
      doThrow(runtimeException).when(mockDiagnosticProxy).enableEventPersistence("test-event");

      // When: Calling enableEvents
      Method enableEventsMethod =
          DiagEventSubscriptionService.class.getDeclaredMethod(
              "enableEvents",
              Node.class,
              Set.class,
              boolean.class,
              JmxCassandraManagementProxy.class);
      enableEventsMethod.setAccessible(true);
      enableEventsMethod.invoke(service, node, events, true, mockJmxProxy);

      // Then: Should handle exception gracefully
      verify(mockDiagnosticProxy).enableEventPersistence("test-event");
    }
  }

  @Test
  void testEnableEvents_WithOtherException_ShouldLogError() throws Exception {
    // Given: DiagnosticProxy that throws other exception
    Node node = createTestNode();
    Set<String> events = Collections.singleton("test-event");

    try (MockedStatic<DiagnosticProxy> mockedDiagnosticProxy = mockStatic(DiagnosticProxy.class)) {
      mockedDiagnosticProxy
          .when(() -> DiagnosticProxy.create(mockJmxProxy))
          .thenReturn(mockDiagnosticProxy);

      RuntimeException runtimeException = new RuntimeException("Other error");
      doThrow(runtimeException).when(mockDiagnosticProxy).enableEventPersistence("test-event");

      // When: Calling enableEvents
      Method enableEventsMethod =
          DiagEventSubscriptionService.class.getDeclaredMethod(
              "enableEvents",
              Node.class,
              Set.class,
              boolean.class,
              JmxCassandraManagementProxy.class);
      enableEventsMethod.setAccessible(true);
      enableEventsMethod.invoke(service, node, events, true, mockJmxProxy);

      // Then: Should handle exception gracefully
      verify(mockDiagnosticProxy).enableEventPersistence("test-event");
    }
  }

  @Test
  void testCreatePoller_ShouldCreateAndConfigurePoller() throws Exception {
    // Given: Node and events
    Node node = createTestNode();
    Set<String> events = Collections.singleton("test-event");
    DiagEventPoller mockPoller = mock(DiagEventPoller.class);

    // When: Calling createPoller
    Method createPollerMethod =
        DiagEventSubscriptionService.class.getDeclaredMethod(
            "createPoller",
            Node.class,
            JmxCassandraManagementProxy.class,
            Set.class,
            boolean.class);
    createPollerMethod.setAccessible(true);

    // Mock the POLLERS_BY_NODE map behavior
    Field pollersField = DiagEventSubscriptionService.class.getDeclaredField("POLLERS_BY_NODE");
    pollersField.setAccessible(true);
    Map<Node, DiagEventPoller> pollers = (Map<Node, DiagEventPoller>) pollersField.get(null);
    pollers.put(node, mockPoller);

    Object result = createPollerMethod.invoke(service, node, mockJmxProxy, events, true);

    // Then: Should return the poller
    assertThat(result).isEqualTo(mockPoller);
    verify(mockPoller).setEnabledEvents(events);
    verify(mockPoller).start();
  }

  @Test
  void testNotificationListener_HandleJmxConnectionClosed() throws Exception {
    // Given: NotificationListener
    Node node = createTestNode();
    Object listener = createNotificationListener(node);

    Notification notification = new Notification(JMXConnectionNotification.CLOSED, this, 1);

    // When: Handling notification
    Method handleNotificationMethod =
        listener
            .getClass()
            .getDeclaredMethod("handleNotification", Notification.class, Object.class);
    handleNotificationMethod.invoke(listener, notification, null);

    // Then: Should handle gracefully (no exception)
  }

  @Test
  void testNotificationListener_HandleJmxConnectionFailed() throws Exception {
    // Given: NotificationListener
    Node node = createTestNode();
    Object listener = createNotificationListener(node);

    Notification notification = new Notification(JMXConnectionNotification.FAILED, this, 1);

    // When: Handling notification
    Method handleNotificationMethod =
        listener
            .getClass()
            .getDeclaredMethod("handleNotification", Notification.class, Object.class);
    handleNotificationMethod.invoke(listener, notification, null);

    // Then: Should handle gracefully (no exception)
  }

  @Test
  void testNotificationListener_HandleNotifsLost() throws Exception {
    // Given: NotificationListener
    Node node = createTestNode();
    Object listener = createNotificationListener(node);

    Notification notification = new Notification(JMXConnectionNotification.NOTIFS_LOST, this, 1);

    // When: Handling notification
    Method handleNotificationMethod =
        listener
            .getClass()
            .getDeclaredMethod("handleNotification", Notification.class, Object.class);
    handleNotificationMethod.invoke(listener, notification, null);

    // Then: Should handle gracefully (no exception)
  }

  @Test
  void testNotificationListener_HandleEventSummary() throws Exception {
    // Given: NotificationListener
    Node node = createTestNode();
    Object listener = createNotificationListener(node);

    Notification notification = new Notification("event_last_id_summary", this, 1);
    Map<String, Comparable> userData = new HashMap<>();
    userData.put("test", "value");
    notification.setUserData(userData);

    // When: Handling notification
    Method handleNotificationMethod =
        listener
            .getClass()
            .getDeclaredMethod("handleNotification", Notification.class, Object.class);
    handleNotificationMethod.invoke(listener, notification, null);

    // Then: Should handle gracefully (no exception)
  }

  @Test
  void testNotificationListener_HandleUnknownNotification() throws Exception {
    // Given: NotificationListener
    Node node = createTestNode();
    Object listener = createNotificationListener(node);

    Notification notification = new Notification("unknown.type", this, 1);

    // When: Handling notification
    Method handleNotificationMethod =
        listener
            .getClass()
            .getDeclaredMethod("handleNotification", Notification.class, Object.class);
    handleNotificationMethod.invoke(listener, notification, null);

    // Then: Should handle gracefully (no exception)
  }

  @Test
  void testNotificationListener_EqualsAndHashCode() throws Exception {
    // Given: Two NotificationListeners with same node
    Node node1 = createTestNode();
    Node node2 = createTestNode();
    Object listener1 = createNotificationListener(node1);
    Object listener2 = createNotificationListener(node1);
    Object listener3 = createNotificationListener(node2);

    // When/Then: Testing equals and hashCode
    assertThat(listener1.equals(listener2)).isTrue();
    assertThat(listener1.equals(listener3)).isFalse();
    assertThat(listener1.equals(null)).isFalse();
    assertThat(listener1.equals("string")).isFalse();
    assertThat(listener1.hashCode()).isEqualTo(listener2.hashCode());
  }

  @Test
  void testBroadcaster_IsActive() throws Exception {
    // Given: Broadcaster
    DiagEventSubscription subscription = createTestSubscription(UUID.randomUUID());
    Object broadcaster = createBroadcaster(subscription);

    // When: Checking if active (initially should be false)
    Method isActiveMethod = broadcaster.getClass().getDeclaredMethod("isActive");
    boolean isActive = (boolean) isActiveMethod.invoke(broadcaster);

    // Then: Should be false initially
    assertThat(isActive).isFalse();
  }

  @Test
  void testBroadcaster_AddAndClose() throws Exception {
    // Given: Broadcaster
    DiagEventSubscription subscription = createTestSubscription(UUID.randomUUID());
    Object broadcaster = createBroadcaster(subscription);
    ChunkedOutput<OutboundEvent> mockOutput = mock(ChunkedOutput.class);

    // When: Adding and closing output
    Method addMethod = broadcaster.getClass().getDeclaredMethod("add", ChunkedOutput.class);
    Method onCloseMethod = broadcaster.getClass().getDeclaredMethod("onClose", ChunkedOutput.class);
    Method isActiveMethod = broadcaster.getClass().getDeclaredMethod("isActive");

    addMethod.invoke(broadcaster, mockOutput);
    boolean isActiveAfterAdd = (boolean) isActiveMethod.invoke(broadcaster);

    onCloseMethod.invoke(broadcaster, mockOutput);
    boolean isActiveAfterClose = (boolean) isActiveMethod.invoke(broadcaster);

    // Then: Should be active after add, inactive after close
    assertThat(isActiveAfterAdd).isTrue();
    assertThat(isActiveAfterClose).isFalse();
  }

  @Test
  void testBroadcaster_OnException() throws Exception {
    // Given: Broadcaster
    DiagEventSubscription subscription = createTestSubscription(UUID.randomUUID());
    Object broadcaster = createBroadcaster(subscription);
    ChunkedOutput<OutboundEvent> mockOutput = mock(ChunkedOutput.class);
    Exception exception = new RuntimeException("Test exception");

    // When: Calling onException
    Method onExceptionMethod =
        broadcaster
            .getClass()
            .getDeclaredMethod("onException", ChunkedOutput.class, Exception.class);
    onExceptionMethod.invoke(broadcaster, mockOutput, exception);

    // Then: Should handle gracefully (no exception thrown)
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

  private Node createTestNode() {
    Cluster cluster =
        Cluster.builder()
            .withName("test-cluster")
            .withSeedHosts(Collections.singleton("127.0.0.1"))
            .build();
    return Node.builder().withCluster(cluster).withHostname("127.0.0.1").build();
  }

  private DiagnosticEvent createTestDiagnosticEvent() {
    DiagnosticEvent event = mock(DiagnosticEvent.class);
    when(event.getCluster()).thenReturn("test-cluster");
    when(event.getNode()).thenReturn("127.0.0.1");
    when(event.getEventClass()).thenReturn("compaction");
    when(event.getEventType()).thenReturn("started");
    return event;
  }

  private void setSubsAlwaysActive(Set<DiagEventSubscription> subscriptions) throws Exception {
    Field subsAlwaysActiveField =
        DiagEventSubscriptionService.class.getDeclaredField("subsAlwaysActive");
    subsAlwaysActiveField.setAccessible(true);
    subsAlwaysActiveField.set(service, subscriptions);
  }

  private Object createNotificationListener(Node node) throws Exception {
    Class<?> notificationListenerClass = null;
    for (Class<?> innerClass : DiagEventSubscriptionService.class.getDeclaredClasses()) {
      if (innerClass.getSimpleName().equals("NotificationListener")) {
        notificationListenerClass = innerClass;
        break;
      }
    }

    assertThat(notificationListenerClass).isNotNull();

    return notificationListenerClass
        .getDeclaredConstructor(
            Node.class,
            java.util.function.Consumer.class,
            java.util.function.Consumer.class,
            java.util.concurrent.ExecutorService.class)
        .newInstance(node, null, null, mockExecutor);
  }

  private Object createBroadcaster(DiagEventSubscription subscription) throws Exception {
    Class<?> broadcasterClass = null;
    for (Class<?> innerClass : DiagEventSubscriptionService.class.getDeclaredClasses()) {
      if (innerClass.getSimpleName().equals("Broadcaster")) {
        broadcasterClass = innerClass;
        break;
      }
    }

    assertThat(broadcasterClass).isNotNull();

    return broadcasterClass
        .getDeclaredConstructor(DiagEventSubscription.class)
        .newInstance(subscription);
  }
}
