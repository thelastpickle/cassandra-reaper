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

package io.cassandrareaper.resources;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.core.DiagEventSubscription;
import io.cassandrareaper.resources.view.DiagnosticEvent;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DiagnosticEventListener;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.exceptions.DriverException;
import com.google.common.collect.Lists;

import org.glassfish.jersey.media.sse.EventOutput;
import org.glassfish.jersey.media.sse.OutboundEvent;
import org.glassfish.jersey.media.sse.SseBroadcaster;
import org.glassfish.jersey.media.sse.SseFeature;
import org.glassfish.jersey.server.ChunkedOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/diag_event/sse_listen")
public class DiagnosticEventLiveViewer {

  private static final Logger LOG = LoggerFactory.getLogger(DiagnosticEventLiveViewer.class);

  private static final Map<UUID, Broadcaster> BROADCASTER_BY_SUBSCRIPTION = new HashMap<>();

  private final AppContext context;

  public DiagnosticEventLiveViewer(AppContext context) {
    this.context = context;
  }

  @GET
  @Path("/{id}")
  @Produces(SseFeature.SERVER_SENT_EVENTS)
  public EventOutput listen(@Context HttpServletRequest request,
                            @PathParam("id") String id,
                            @HeaderParam(SseFeature.LAST_EVENT_ID_HEADER) @DefaultValue("-1") int lastEventId) {

    LOG.debug("get subscription called with id: {}", id);
    DiagEventSubscription sub = context.storage.getEventSubscription(UUID.fromString(id));
    if (sub == null) {
      LOG.warn("Could not find diag event subscription with id {}", id);
      return null;
    }
    String clusterName = sub.getCluster();

    Broadcaster broadcaster = null;
    Collection<Cluster> clusters = Collections.synchronizedSet(new HashSet<>());
    synchronized (BROADCASTER_BY_SUBSCRIPTION) {
      broadcaster = BROADCASTER_BY_SUBSCRIPTION.get(sub.getId());
      // early exit on existing broadcaster for subscription
      if (broadcaster != null && !broadcaster.isClosed()) {
        EventOutput eventOutput = new EventOutput();
        broadcaster.add(eventOutput);
        return eventOutput;
      }

      if (broadcaster == null || broadcaster.isClosed()) {
        broadcaster = new Broadcaster(clusters);
        BROADCASTER_BY_SUBSCRIPTION.put(sub.getId(), broadcaster);
        LOG.debug("[{}] Created SSE broadcaster for {}", broadcaster, request.getRemoteAddr());
      }
    }


    // create client for cluster we want to subscribe to
    for (io.cassandrareaper.core.Cluster cluster : context.storage.getClusters()) {
      if (!clusterName.equals(cluster.getName())) {
        continue;
      }
      if (cluster.getSeedHosts().isEmpty()) {
        continue;
      }

      try {
        // create driver connection for first contact seed
        String shost = cluster.getSeedHosts().iterator().next();
        InetAddress addr;
        try {
          addr = InetAddress.getByName(shost);
        } catch (UnknownHostException e) {
          LOG.error("Failed to resolve seed address: " + shost, e);
          continue;
        }
        LOG.debug("Subscribing to events on {} ({}): {}", addr, cluster.getName(), sub.getEvents());
        Cluster driverCluster = openControlConnectionAndRegister(
                broadcaster, cluster.getName(), addr, clusters, sub.getEvents());

        // retrieve meta data and connect to remaining nodes in cluster as well
        Metadata metadata = driverCluster.getMetadata();
        Set<Host> allHosts = metadata.getAllHosts();
        for (Host host : allHosts) {
          // filter by hosts specified in subscriptions and init client
          if (!sub.getIncludeNodes().isEmpty() && sub.getIncludeNodes().contains(host.getAddress().getHostAddress())) {
            openControlConnectionAndRegister(
                    broadcaster, cluster.getName(), host.getAddress(), clusters, sub.getEvents());
          }
        }

      } catch (DriverException e) {
        LOG.error("Failed to subscribe to cluster " + cluster.getName(), e);
      }
    }

    EventOutput eventOutput = new EventOutput();
    broadcaster.add(eventOutput);
    return eventOutput;
  }

  private Cluster openControlConnectionAndRegister(SseBroadcaster broadcaster, String clusterName,
                                                   InetAddress host, Collection<Cluster> clusters,
                                                   List<String> events) {
    LOG.debug("Connecting to node: {}", host);
    Cluster driverCluster = Cluster.builder()
            .addContactPoints(host)
            .withoutMetrics()
            .build();
    // register listener and create control connection used for receiving events
    clusters.add(driverCluster);
    driverCluster.register(new EventListener(broadcaster, clusterName, host));
    driverCluster.init();
    driverCluster.debug(Lists.newArrayList(events));

    // TODO: monitor control connect and make sure it's not going to get re-established on a different host
    return driverCluster;
  }

  private static class EventListener implements DiagnosticEventListener {

    private SseBroadcaster broadcaster;
    private InetAddress node;
    private String cluster;
    private AtomicLong idCounter = new AtomicLong();

    EventListener(SseBroadcaster broadcaster, String cluster, InetAddress node) {
      this.broadcaster = broadcaster;
      this.cluster = cluster;
      this.node = node;
    }

    @Override
    public void onEvent(String eventClazz, String eventType, String from, long timestamp, Map<String, String> payload) {
      DiagnosticEvent event = new DiagnosticEvent(cluster, node.toString(), eventClazz, eventType, timestamp, payload);
      OutboundEvent out = new OutboundEvent.Builder()
              .id(String.valueOf(idCounter.getAndIncrement()))
              .mediaType(MediaType.APPLICATION_JSON_TYPE)
              .data(DiagnosticEvent.class, event).build();
      LOG.debug("[{}] Broadcasting diagnostic event {}/{} from {}", broadcaster, eventClazz, eventType, node);
      broadcaster.broadcast(out);
    }

    @Override
    public void onRegister(Cluster cluster) {
      LOG.debug("EventListener registered");
    }

    @Override
    public void onUnregister(Cluster cluster) {
      LOG.debug("EventListener unregistered");
    }
  }

  private class Broadcaster extends SseBroadcaster {

    private boolean closed = false;
    private Collection<Cluster> clusters;
    private AtomicLong outputs = new AtomicLong();

    Broadcaster(Collection<Cluster> clusters) {
      super();
      this.clusters = clusters;
    }

    @Override
    public void onException(ChunkedOutput<OutboundEvent> chunkedOutput, Exception exception) {
      super.onException(chunkedOutput, exception);
      LOG.debug("[{}] SSE exception", this);
      //closeClusters();
    }

    @Override
    public <OUT extends ChunkedOutput<OutboundEvent>> boolean add(OUT chunkedOutput) {
      outputs.incrementAndGet();
      return super.add(chunkedOutput);
    }

    @Override
    public void onClose(ChunkedOutput<OutboundEvent> chunkedOutput) {
      super.onClose(chunkedOutput);
      LOG.debug("[{}] SSE channel closed", this);
      if (outputs.decrementAndGet() == 0) {
        closeClusters();
      }
    }

    private void closeClusters() {
      closed = true;
      LOG.debug("[{}] Closing {} cluster connections", this, clusters.size());
      for (Cluster cluster : clusters) {
        if (!cluster.isClosed()) {
          cluster.closeAsync();
        }
      }
    }

    public boolean isClosed() {
      return closed;
    }
  }
}
