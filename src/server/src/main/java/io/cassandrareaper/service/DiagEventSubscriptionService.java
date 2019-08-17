/*
 * Copyright 2018-2018 Stefan Podkowinski
 * Copyright 2019-2019 The Last Pickle Ltd
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
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.DiagEventSubscription;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.jmx.DiagnosticProxy;
import io.cassandrareaper.jmx.JmxProxy;
import io.cassandrareaper.resources.view.DiagnosticEvent;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.management.Notification;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnectionNotification;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.InstrumentedExecutorService;
import com.codahale.metrics.InstrumentedScheduledExecutorService;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.glassfish.jersey.media.sse.EventOutput;
import org.glassfish.jersey.media.sse.OutboundEvent;
import org.glassfish.jersey.media.sse.SseBroadcaster;
import org.glassfish.jersey.server.ChunkedOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DiagEventSubscriptionService {

  private static final Logger LOG = LoggerFactory.getLogger(DiagEventSubscriptionService.class);

  private static final Map<DiagEventSubscription, Broadcaster> BROADCASTER_BY_SUBSCRIPTION = new HashMap<>();
  private static final Map<Node, DiagEventPoller> POLLERS_BY_NODE = new HashMap<>();
  private static final int UPDATE_ENABLED_INTERVAL_SECS = 15;//60*15;
  private static final int PING_CLIENTS_INTERVAL_SECS = 5;
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper(new JsonFactory());

  private final AtomicLong idCounter = new AtomicLong(0);
  private final Map<Node, NotificationListener> listenerByNode = new ConcurrentHashMap<>();
  private final AppContext context;
  private final HttpClient httpClient;
  private final ExecutorService taskExecutor;
  private final ScheduledExecutorService scheduledExecutor;

  private Set<DiagEventSubscription> subsAlwaysActive;

  private DiagEventSubscriptionService(AppContext context, HttpClient httpClient) {
    this.context = context;
    this.httpClient = httpClient;

    this.scheduledExecutor
        = new InstrumentedScheduledExecutorService(Executors.newScheduledThreadPool(1), context.metricRegistry);

    this.taskExecutor = new InstrumentedExecutorService(Executors.newWorkStealingPool(), context.metricRegistry);

    scheduledExecutor.scheduleWithFixedDelay(
            this::updateEnabledEvents, 5, UPDATE_ENABLED_INTERVAL_SECS, TimeUnit.SECONDS);

    // ping registered clients to raise and error for already closed connections, which is the only way to notice
    // if a client is still listening
    scheduledExecutor.scheduleWithFixedDelay(
            this::pingSseClients, PING_CLIENTS_INTERVAL_SECS, PING_CLIENTS_INTERVAL_SECS, TimeUnit.SECONDS);
  }

  public static DiagEventSubscriptionService create(AppContext context, HttpClient httpClient) {
    return new DiagEventSubscriptionService(context, httpClient);
  }

  public DiagEventSubscription getEventSubscription(UUID id) {
    return context.storage.getEventSubscription(id);
  }

  public void deleteEventSubscription(UUID id) {
    DiagEventSubscription sub = context.storage.getEventSubscription(id);
    if (null != sub) {
      if (context.storage.deleteEventSubscription(id)) {
        updateEnabledEvents(new HashSet<>(sub.getIncludeNodes()));
      }
    }
  }

  public DiagEventSubscription addEventSubscription(DiagEventSubscription subscription) {
    return context.storage.addEventSubscription(subscription.withId(subscription.getId().orElse(UUID.randomUUID())));
  }

  public void subscribe(DiagEventSubscription sub, EventOutput eventOutput, String remoteAddr) {
    Broadcaster broadcaster = BROADCASTER_BY_SUBSCRIPTION.computeIfAbsent(sub, (key) -> new Broadcaster());
    LOG.debug("Using SSE broadcaster for subscription {} and new client {}", sub.getId(), remoteAddr);
    broadcaster.add(eventOutput);

    // enable events for nodes in subscription (if needed)
    updateEnabledEvents(new HashSet<>(sub.getIncludeNodes()));
  }

  protected synchronized void updateEnabledEvents() {
    updateEnabledEvents(Collections.emptySet());
  }

  protected synchronized void updateEnabledEvents(Set<String> filterByNode) {
    LOG.debug("Checking current event subscriptions");

    // enable all events with active consumers and disable events for non-active consumers
    // we have two different kind of consumers: always active (loggers and http) and ad-hoc (live view)
    Collection<DiagEventSubscription> allSubs = context.storage.getEventSubscriptions();

    Set<DiagEventSubscription> subsAlwaysActive = allSubs.stream()
            .filter((sub) -> sub.getExportFileLogger() != null || sub.getExportHttpEndpoint() != null)
            .collect(Collectors.toSet());

    Set<DiagEventSubscription> subsAdHoc = allSubs.stream()
            .filter((sub) -> !subsAlwaysActive.contains(sub))
            .collect(Collectors.toSet());

    // cache for checking against incoming events
    this.subsAlwaysActive = subsAlwaysActive;

    // determine which of the ad-hoc subscriptions have currently active SSE clients listening
    Set<DiagEventSubscription> subsAdHocActive = subsAdHoc.stream().filter((sub) -> {
      Broadcaster broadcaster = BROADCASTER_BY_SUBSCRIPTION.get(sub);
      return broadcaster != null && broadcaster.isActive();
    }).collect(Collectors.toSet());

    // create mapping for all subscriptions by node
    SetMultimap<Node, DiagEventSubscription> subscriptionsByNodeMulti
        = MultimapBuilder.SetMultimapBuilder.hashKeys().hashSetValues().build();

    // always gather all subscriptions to create the complete list of relevant events for a node
    for (DiagEventSubscription sub : allSubs) {

      Cluster cluster = Cluster.builder()
          .withName(sub.getCluster())
          .withSeedHosts(ImmutableSet.copyOf(sub.getIncludeNodes()))
          .build();

      for (String host : sub.getIncludeNodes()) {
        if (filterByNode.isEmpty() || filterByNode.contains(host)) {
          Node node = Node.builder().withCluster(cluster).withHostname(host).build();
          subscriptionsByNodeMulti.put(node, sub);
        }
      }
    }

    // inspect activation status of subscriptions node by node
    Predicate<DiagEventSubscription> isActiveSubscription
        = (sub) -> subsAlwaysActive.contains(sub) || subsAdHocActive.contains(sub);

    Map<Node, Collection<DiagEventSubscription>> subscriptionsByNode = subscriptionsByNodeMulti.asMap();
    CountDownLatch nodesLatch = new CountDownLatch(subscriptionsByNode.size());
    LOG.debug("Updating event subscriptions for {} nodes", subscriptionsByNode.size());

    for (Node node : subscriptionsByNode.keySet()) {
      LOG.debug("{}: {}", node, subscriptionsByNode.get(node));

      taskExecutor.submit(() -> {
        String threadName = Thread.currentThread().getName();
        try {
          Thread.currentThread().setName(node.getHostname());
          LOG.debug("Starting to update event subscriptions for {}", node);

          JmxProxy jmx;
          try {
            jmx = context.jmxConnectionFactory.connectAny(Collections.singleton(node));
          } catch (ReaperException e) {
            LOG.error("Failed to acquire JMX connection", e);
            return;
          }

          // create set of active and inactive events based on all subscriptions for this node
          // active events are all events included in an active subscription
          Collection<DiagEventSubscription> subscriptions = subscriptionsByNode.get(node);
          Set<String> activeEvents = subscriptions.stream()
                  .filter(isActiveSubscription)
                  .flatMap((sub) -> sub.getEvents().stream())
                  .collect(Collectors.toSet());

          // inactive events are all non-active events
          Set<String> inactiveEvents = subscriptions.stream()
                  .flatMap((sub) -> sub.getEvents().stream())
                  .filter((event) -> !activeEvents.contains(event))
                  .collect(Collectors.toSet());

          // if there are no active events for this node, disable them and stop polling
          if (activeEvents.isEmpty()) {
            LOG.debug("No active events subscriptions");
            Set<String> possiblyEnabledEvents = new HashSet<>();
            possiblyEnabledEvents.addAll(inactiveEvents);
            // kill poller
            DiagEventPoller poller = POLLERS_BY_NODE.remove(node);
            if (poller != null) {
              LOG.debug("Stopping existing event poller");
              possiblyEnabledEvents.addAll(poller.getEnabledEvents());
              poller.stop();
            }
            // unsubscribe from jmx notifications
            unsubscribeNotifications(node, jmx);
            // clear any possible remaining enabled events
            if (!possiblyEnabledEvents.isEmpty()) {
              enableEvents(node, possiblyEnabledEvents, false, jmx);
            }
            return;
          }

          // if there are inactive events, just disable them
          if (!inactiveEvents.isEmpty()) {
            LOG.debug("Disabling events for inactive subscriptions");
            enableEvents(node, inactiveEvents, false, jmx);
          }

          // active events need to be enabled and polled
          if (!activeEvents.isEmpty()) {
            LOG.debug("Enabling events for active subscriptions");
            enableEvents(node, activeEvents, true, jmx);
            DiagEventPoller poller = createPoller(node, jmx, activeEvents, true);
            if (poller != null) {
              subscribeNotifications(node, jmx, poller);
            }
          }

        } finally {
          Thread.currentThread().setName(threadName);
          nodesLatch.countDown();

          LOG.debug(
              "Finished handling event subscriptions for {} ({}/{})",
              node, nodesLatch.getCount(), subscriptionsByNode.size());
        }
      });
    }

    try {
      if (!nodesLatch.await(30, TimeUnit.SECONDS)) {
        LOG.warn("Timeout while handling event subscriptions for some nodes");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn(e.getMessage(), e);
    }
  }

  private void enableEvents(Node node, Set<String> events, boolean enabled, JmxProxy jmxProxy) {
    for (String event : events) {
      LOG.debug("{} {} for {}", enabled ? "Enabling" : "Disabling", event, node);
      try {
        if (enabled) {
          DiagnosticProxy.create(jmxProxy).enableEventPersistence(event);
        } else {
          DiagnosticProxy.create(jmxProxy).disableEventPersistence(event);
        }
      } catch (RuntimeException e) {
        if (e.getCause() instanceof ClassNotFoundException) {
          LOG.warn("Event not supported on server: {}", event);
        } else if (e.getCause() instanceof ReflectionException) {
          LOG.warn(String.format("Failed to manage events via JMX for %s: "
              + "incompatible Cassandra version (>=4.0 required)", node));
        } else {
          LOG.error(String.format("Failed to enable/disable event %s via JMX for %s", event, node), e);
        }
      }
    }
  }

  private DiagEventPoller createPoller(Node node, JmxProxy jmxProxy, Set<String> events, boolean enabled) {
    try {
      DiagEventPoller poller = POLLERS_BY_NODE
          .computeIfAbsent(node, (key) -> new DiagEventPoller(key, jmxProxy, this::onEvent, scheduledExecutor));

      poller.setEnabledEvents(events);
      if (enabled) {
        poller.start();
      } else {
        poller.stop();
      }
      return poller;
    } catch (RuntimeException e) {
      LOG.error("Failed to create poller", e);
      return null;
    }
  }

  private void subscribeNotifications(Node node, JmxProxy jmxProxy, DiagEventPoller poller) {
    LOG.debug("Subscribing to notifications on {} ({})", jmxProxy.getHost(), jmxProxy.getClusterName());
    if (listenerByNode.containsKey(node)) {
      LOG.debug("Notification listener already registered for {}", node);
      return;
    }

    NotificationListener listener = new NotificationListener(
        node,
        // reopen JMX connection by re-enabling all events for this node
        (notification) -> updateEnabledEvents(Collections.singleton(node.getHostname())),
        // forward incoming notifications (event summaries) to poller
        poller::onSummary,
        taskExecutor);

    if (null == listenerByNode.putIfAbsent(node, listener)) {
      DiagnosticProxy.create(jmxProxy).subscribeNotifications(listener);
    }
  }


  private void unsubscribeNotifications(Node node, JmxProxy jmxProxy) {
    LOG.debug("Unsubscribing from notifications on {} ({})", jmxProxy.getHost(), jmxProxy.getClusterName());
    NotificationListener listener = listenerByNode.remove(node);
    if (listener == null) {
      LOG.debug("Notification listener not found for {}", node);
      return;
    }
    DiagnosticProxy.create(jmxProxy).unsubscribeNotifications(listener);
  }

  // as provided by DiagEventPoller
  private void onEvent(DiagnosticEvent event) {
    String json;
    try {
      json = JSON_MAPPER.writeValueAsString(event);
    } catch (JsonProcessingException e) {
      LOG.error("Failed to serialize diagnostic event as JSON", e);
      return;
    }

    //
    // always-active subscriptions
    //
    for (DiagEventSubscription sub : subsAlwaysActive) {
      try {
        if (sub.getCluster().equals(event.getCluster())
                && sub.getIncludeNodes().contains(event.getNode())
                && sub.getEvents().contains(event.getEventClass())) {

          if (sub.getExportFileLogger() != null) {
            Logger logger = LoggerFactory.getLogger(sub.getExportFileLogger());
            if (logger == null) {
              LOG.error("Failed to get logger: {}", sub.getExportFileLogger());
            } else {
              logger.info(json);
            }
          }
          if (sub.getExportHttpEndpoint() != null) {
            HttpPost req = new HttpPost(sub.getExportHttpEndpoint());
            req.setEntity(new StringEntity(json, ContentType.APPLICATION_JSON));
            try {
              HttpResponse resp = httpClient.execute(req);
              // consume response to return connection to pool
              EntityUtils.consumeQuietly(resp.getEntity());
            } catch (IOException e) {
              LOG.error("Failed to post event to endpoint: " + sub.getExportHttpEndpoint(), e);
            }
          }
        }
      } catch (RuntimeException e) {
        LOG.error("Error while checking subscription: " + sub, e);
      }
    }

    //
    // ad-hoc subscriptions
    //
    OutboundEvent out = new OutboundEvent.Builder()
            .id(String.valueOf(idCounter.getAndIncrement()))
            .mediaType(MediaType.APPLICATION_JSON_TYPE)
            .data(json).build();

    // broadcast event to all clients wit matching event subscriptions (node and event type)
    for (Map.Entry<DiagEventSubscription, Broadcaster> entry : BROADCASTER_BY_SUBSCRIPTION.entrySet()) {
      DiagEventSubscription sub = entry.getKey();
      if (sub.getCluster().equals(event.getCluster())
              && sub.getIncludeNodes().contains(event.getNode())
              && sub.getEvents().contains(event.getEventClass())) {

        LOG.debug(
            "[{}] Broadcasting diagnostic event {}/{} from {}",
            entry.getValue(), event.getEventClass(), event.getEventType(), event.getNode());

        entry.getValue().broadcast(out);
      }
    }
  }

  private void pingSseClients() {
    OutboundEvent out = new OutboundEvent.Builder()
            .id(String.valueOf(idCounter.getAndIncrement()))
            .mediaType(MediaType.APPLICATION_JSON_TYPE)
            .data("PING").build();

    for (Broadcaster broadcaster : BROADCASTER_BY_SUBSCRIPTION.values()) {
      broadcaster.broadcast(out);
    }
  }

  private static class NotificationListener implements javax.management.NotificationListener {

    private final Node node;
    private final Consumer<Notification> onConnectionClosed;
    private final Consumer<Map<String, Comparable>> onSummary;
    private final ExecutorService taskExecutor;

    NotificationListener(
        Node node,
        Consumer<Notification> onConnectionClosed,
        Consumer<Map<String, Comparable>> onSummary,
        ExecutorService taskExecutor) {

      this.node = node;
      this.onConnectionClosed = onConnectionClosed;
      this.onSummary = onSummary;
      this.taskExecutor = taskExecutor;
    }

    @Override
    public void handleNotification(Notification notification, Object handback) {
      // pass off the work immediately to a separate thread
      taskExecutor.submit(() -> {
        String threadName = Thread.currentThread().getName();
        try {
          Thread.currentThread().setName(node.getHostname());

          switch (notification.getType()) {
            case JMXConnectionNotification.CLOSED:
            case JMXConnectionNotification.FAILED:
              LOG.debug("JMX connection closed");
              if (onConnectionClosed != null) {
                onConnectionClosed.accept(notification);
              }
              break;

            case JMXConnectionNotification.NOTIFS_LOST:
              LOG.warn("Lost JMX notifications");
              break;

            case "event_last_id_summary":
              LOG.debug("Received event summary: {}", notification);
              if (onSummary != null) {
                onSummary.accept((Map<String, Comparable>) notification.getUserData());
              }
              break;

            default:
              break;
          }
        } catch (RuntimeException e) {
          LOG.error("Error while handling JMX notification", e);
        } finally {
          Thread.currentThread().setName(threadName);
        }
      });
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      NotificationListener that = (NotificationListener) obj;
      return Objects.equals(node, that.node);
    }

    @Override
    public int hashCode() {
      return Objects.hash(node);
    }
  }

  private class Broadcaster extends SseBroadcaster {

    private final AtomicLong outputs = new AtomicLong();

    @Override
    public void onException(ChunkedOutput<OutboundEvent> chunkedOutput, Exception exception) {
      super.onException(chunkedOutput, exception);
      LOG.debug("[{}] SSE exception", this);
      //closeJMX();
    }

    @Override
    public <OUT extends ChunkedOutput<OutboundEvent>> boolean add(OUT chunkedOutput) {
      LOG.debug("[{}] Adding SSE channel", this, chunkedOutput);
      outputs.incrementAndGet();
      return super.add(chunkedOutput);
    }

    @Override
    public void onClose(ChunkedOutput<OutboundEvent> chunkedOutput) {
      super.onClose(chunkedOutput);
      LOG.debug("[{}] SSE channel closed", this);
      outputs.decrementAndGet();
    }

    public boolean isActive() {
      return outputs.get() > 0;
    }
  }
}
