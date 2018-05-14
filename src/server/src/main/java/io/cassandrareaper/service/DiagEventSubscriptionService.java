/*
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
import io.cassandrareaper.jmx.JmxProxy;
import io.cassandrareaper.resources.view.DiagnosticEvent;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import javax.management.JMException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.remote.JMXConnectionNotification;
import javax.ws.rs.core.MediaType;

import com.google.common.base.Optional;
import org.glassfish.jersey.media.sse.EventOutput;
import org.glassfish.jersey.media.sse.OutboundEvent;
import org.glassfish.jersey.media.sse.SseBroadcaster;
import org.glassfish.jersey.server.ChunkedOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DiagEventSubscriptionService {


  private static final Logger LOG = LoggerFactory.getLogger(DiagEventSubscriptionService.class);

  private static final Map<UUID, Broadcaster> BROADCASTER_BY_SUBSCRIPTION = new HashMap<>();

  private static final ExecutorService EXECUTOR = Executors.newWorkStealingPool();
  private static final ScheduledExecutorService SCHEDULED_EXECUTOR = Executors.newScheduledThreadPool(1);
  private static final int UPDATE_ENABLED_INTERVAL_SECS = 60*15;

  private final AppContext context;


  public DiagEventSubscriptionService(AppContext context) {
    this.context = context;

    SCHEDULED_EXECUTOR.scheduleWithFixedDelay(
            this::updateEnabledEvents, 5, UPDATE_ENABLED_INTERVAL_SECS, TimeUnit.SECONDS);
  }

  public static DiagEventSubscriptionService create(AppContext context) {
    return new DiagEventSubscriptionService(context);
  }

  public Optional<DiagEventSubscription> getEventSubscription(UUID id) {
    return context.storage.getEventSubscription(id);
  }

  public boolean deleteEventSubscription(UUID id) {
    // TODO: notify broadcasters
    return context.storage.deleteEventSubscription(id);
  }

  public DiagEventSubscription addEventSubscription(DiagEventSubscription subscription) {
    return context.storage.addEventSubscription(subscription);
  }

  public void subscribe(DiagEventSubscription sub, EventOutput eventOutput, String remoteAddr) {

    String clusterName = sub.getCluster();

    // try acquire cached broadcaster by subscription and early exit
    Broadcaster broadcaster = BROADCASTER_BY_SUBSCRIPTION.get(sub.getId());
    if (broadcaster != null && !broadcaster.isClosed()) {
      LOG.debug("Using existing SSE broadcaster for subscription {} and new client {}",
              sub.getId(), remoteAddr);
      broadcaster.add(eventOutput);
      return;
    }

    Optional<Cluster> cluster = context.storage.getCluster(clusterName);
    if (!cluster.isPresent()) {
      throw new NullPointerException(
              String.format("Cluster '%s' for subscription %s doesn't exists. Update or delete subscription.",
                      clusterName, sub.getId()));
    }

    LOG.debug("Creating SSE broadcaster for subscription {} and client {}",  sub.getId(), remoteAddr);
    Collection<CompletableFuture<JmxProxy>> jmxProxies = sub.getIncludeNodes().stream()
            .map((nodeName) -> Node.builder().withCluster(cluster.get()).withHostname(nodeName).build())
            .map((node) -> CompletableFuture.supplyAsync(() -> {
                      try {
                        return context.jmxConnectionFactory.connect(node, context.config.getJmxConnectionTimeoutInSeconds());
                      } catch (ReaperException | InterruptedException e) {
                        throw new RuntimeException(e);
                      }
                    },
                    EXECUTOR))
            .collect(Collectors.toSet());

    // the broadcaster will close all jmx connection in case none of the clients is connected anymore
    final Broadcaster sseBroadcaster = new Broadcaster(jmxProxies);
    BROADCASTER_BY_SUBSCRIPTION.put(sub.getId(), sseBroadcaster);

    // subscribe to jmx notifications for each jmx proxy that has been initialized
    int totalNodes = sub.getIncludeNodes().size();
    AtomicLong latch = new AtomicLong();

    jmxProxies.forEach((proxy) -> proxy.whenCompleteAsync((jmx, err) -> {
      try {
        if (err != null) {
          LOG.error("Failed to connect to all nodes via JMX. Cannot subscribe to events from all nodes.");
          // XXX: we should probably fail hard by closing the other connections and subscriptions
          return;
        }
        LOG.debug("Subscribing to events on {} ({}): {} ({}/{})", jmx.getHost(), jmx.getClusterName(),
                sub.getEvents(), latch.incrementAndGet(), totalNodes);
        EventListener listener = new EventListener(sseBroadcaster, jmx.getClusterName(), jmx.getHost());
        jmx.addConnectionNotificationListener(listener);
        jmx.addNotificationListener(listener, null); //(notification) -> notification.getType().equals("diag_event"));
      } catch (IOException | JMException e) {
        throw new RuntimeException(e);
      }
    }, EXECUTOR));

    // enable events for newly created subscription
    EXECUTOR.submit(() -> updateEnabledEvents(sub, true));

    sseBroadcaster.add(eventOutput);
  }

  protected void updateEnabledEvents() {

    LOG.debug("Checking active event subscriptions");

    // enable all events with active consumers and disable events for non-active consumers
    // we have two different kind of consumers: always active (loggers and http) and ad-hoc (live view)
    Collection<DiagEventSubscription> allSubs = context.storage.getEventSubscriptions();
    Set<DiagEventSubscription> subsAlwaysActive = allSubs.stream()
            .filter((sub) -> sub.getExportFileLogger() != null || sub.getExportHttpEndpoint() != null)
            .collect(Collectors.toSet());
    Set<DiagEventSubscription> subsAdHoc = allSubs.stream()
            .filter((sub) -> !subsAlwaysActive.contains(sub))
            .collect(Collectors.toSet());

    // determine which of the ad-hoc subscriptions all currently active
    Set<DiagEventSubscription> subsAdHocActive = subsAdHoc.stream().filter((sub) -> {
      Broadcaster broadcaster = BROADCASTER_BY_SUBSCRIPTION.get(sub.getId());
      return broadcaster != null && !broadcaster.isClosed();
    }).collect(Collectors.toSet());

    // create a two sets for each node: active and non-active events
    Map<Node, Set<String>> eventsByNodeActive = new HashMap<>();
    Map<Node, Set<String>> eventsByNodeInactive = new HashMap<>();
    for (DiagEventSubscription sub : allSubs) {
      boolean active = subsAlwaysActive.contains(sub) || subsAdHocActive.contains(sub);
      for (String host : sub.getIncludeNodes()) {
        Node node = Node.builder().withClusterName(sub.getCluster()).withHostname(host).build();
        if (active) {
          eventsByNodeActive.computeIfAbsent(node, (key) -> new HashSet<>()).addAll(sub.getEvents());
          eventsByNodeInactive.computeIfAbsent(node, (key) -> new HashSet<>()).removeAll(sub.getEvents());
        } else {
          eventsByNodeInactive.computeIfAbsent(node, (key) -> new HashSet<>()).addAll(sub.getEvents());
        }
      }
    }

    // updateEnabledEvents(eventsByNodeInactive, false);
    updateEnabledEvents(eventsByNodeActive, true);
  }

  protected void updateEnabledEvents(DiagEventSubscription sub, boolean enabled) {
    Map<Node, Set<String>> eventsByNode = new HashMap<>();
    for (String host : sub.getIncludeNodes()) {
      Node node = Node.builder().withClusterName(sub.getCluster()).withHostname(host).build();
      Set<String> events = eventsByNode.getOrDefault(node, new HashSet<>());
      events.addAll(sub.getEvents());
      eventsByNode.put(node, events);
    }
    updateEnabledEvents(eventsByNode, enabled);
  }

  private void updateEnabledEvents(Map<Node, Set<String>> eventsByNode, boolean enabled) {
    LOG.debug("{} events: {}", enabled ? "Enabling" : "Disabling", eventsByNode.toString());
    for (Node node : eventsByNode.keySet()) {
      try {
        LOG.debug("Creating JMX connection to {}", node);
        JmxProxy jmxProxy = context.jmxConnectionFactory.connect(node, 10000);
        for (String event : eventsByNode.get(node)) {
          LOG.debug("{} {} for {}", enabled ? "Enabling" : "Disabling", event, node);
          try {
            jmxProxy.enableEvents(event);
          } catch (RuntimeException e) {
            if (e.getCause() instanceof ClassNotFoundException) {
              LOG.warn("Event not supported on server: {}", event);
            } else {
              LOG.error(String.format("Failed to enable/disable event %s via JMX for %s", event, node), e);
            }
          }
        }
      } catch (ReaperException | InterruptedException e) {
        LOG.error("Failed to enable/disable diagnostic events via JMX", e);
      }
    }
  }

  private static class EventListener implements NotificationListener {

    private SseBroadcaster broadcaster;
    private String hostName;
    private String clusterName;
    private AtomicLong idCounter = new AtomicLong();

    EventListener(SseBroadcaster broadcaster, String clusterName, String hostName) {
      this.broadcaster = broadcaster;
      this.clusterName = clusterName;
      this.hostName = hostName;
    }

    @Override
    public void handleNotification(Notification notification, Object handback) {
      // pass off the work immediately to a separate thread
      EXECUTOR.submit(() -> {
        String threadName = Thread.currentThread().getName();
        try {
          Thread.currentThread().setName(clusterName);

          switch (notification.getType()) {
            case JMXConnectionNotification.CLOSED:
            case JMXConnectionNotification.FAILED:
              LOG.debug("JMX connection closed");
              // TODO: send sse event
              break;

            case JMXConnectionNotification.NOTIFS_LOST:
              LOG.warn("Lost JMX notifications");
              // TODO: send sse event
              break;

            case "diag_event":
              LOG.debug("Received event: {}", notification);
              Map<String, Object> payload = (Map<String, Object>) notification.getUserData();
              Long timestamp = notification.getTimeStamp();
              String eventClazz = (String) payload.get("class");
              String eventType = (String) payload.get("type");

              DiagnosticEvent event = new DiagnosticEvent(clusterName, hostName,
                      eventClazz, eventType, timestamp, payload);
              OutboundEvent out = new OutboundEvent.Builder()
                      .id(String.valueOf(idCounter.getAndIncrement()))
                      .mediaType(MediaType.APPLICATION_JSON_TYPE)
                      .data(DiagnosticEvent.class, event).build();
              LOG.debug("[{}] Broadcasting diagnostic event {}/{} from {}",
                      broadcaster, eventClazz, eventType, hostName);
              broadcaster.broadcast(out);
              break;

            default:
              break;
          }
        } finally {
          Thread.currentThread().setName(threadName);
        }
      });
    }
  }

  private class Broadcaster extends SseBroadcaster {

    private boolean closed = false;
    private Collection<CompletableFuture<JmxProxy>> jmxProxyPerNode;
    private AtomicLong outputs = new AtomicLong();

    Broadcaster(Collection<CompletableFuture<JmxProxy>> jmxProxyPerNode) {
      super();
      this.jmxProxyPerNode = jmxProxyPerNode;
    }

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
      if (outputs.decrementAndGet() == 0) {
        closeJmx();
      }
    }

    private void closeJmx() {
      closed = true;
      LOG.debug("[{}] Closing {} JMX connection(s)", this, jmxProxyPerNode.size());
      for (Future<JmxProxy> proxy : jmxProxyPerNode) {
        if (proxy.isDone()) {
          // close initialized proxies
          try {
            proxy.get().close();
          } catch (InterruptedException | ExecutionException e) {
            LOG.warn("Failed to close JMX proxy", e);
          }
        } else if (!proxy.isCancelled()) {
          // cancel ongoing initializing
          proxy.cancel(true);
        }
      }
    }

    public boolean isClosed() {
      return closed;
    }
  }
}
