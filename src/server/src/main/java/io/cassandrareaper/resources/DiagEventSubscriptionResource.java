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

package io.cassandrareaper.resources;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.core.DiagEventSubscription;
import io.cassandrareaper.service.DiagEventSubscriptionService;
import io.cassandrareaper.storage.events.IEventsDao;

import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import jakarta.annotation.security.RolesAllowed;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/diag_event/subscription")
@Produces(MediaType.APPLICATION_JSON)
public final class DiagEventSubscriptionResource {

  private static final Logger LOG = LoggerFactory.getLogger(DiagEventSubscriptionResource.class);

  private final IEventsDao eventsDao;
  private final AppContext context;
  private final DiagEventSubscriptionService diagEventService;

  public DiagEventSubscriptionResource(
      AppContext context,
      CloseableHttpClient httpClient,
      ScheduledExecutorService executor,
      IEventsDao eventsDao) {
    this.context = context;
    this.eventsDao = eventsDao;
    this.diagEventService =
        DiagEventSubscriptionService.create(context, httpClient, executor, eventsDao);
  }

  @GET
  @RolesAllowed({"user", "operator"})
  public Response getEventSubscriptionList(
      @QueryParam("clusterName") Optional<String> clusterName) {
    LOG.debug("get event subscriptions called %s", clusterName);

    Collection<DiagEventSubscription> subscriptions =
        clusterName.isPresent()
            ? eventsDao.getEventSubscriptions(clusterName.get())
            : eventsDao.getEventSubscriptions();

    return Response.ok().entity(subscriptions).build();
  }

  @POST
  @RolesAllowed({"operator"})
  public Response addEventSubscription(
      @Context UriInfo uriInfo,
      @QueryParam("clusterName") String cluster,
      @QueryParam("description") Optional<String> desc,
      @QueryParam("nodes") String nodesString,
      @QueryParam("events") String eventsString,
      @QueryParam("exportSse") boolean sse,
      @QueryParam("exportFileLogger") String logger,
      @QueryParam("exportHttpEndpoint") String endpoint) {

    AtomicBoolean created = new AtomicBoolean(false);
    Set<String> nodes =
        ImmutableSet.copyOf(nodesString == null ? new String[] {} : nodesString.split(","));
    Set<String> events =
        ImmutableSet.copyOf(eventsString == null ? new String[] {} : eventsString.split(","));

    DiagEventSubscription subscription =
        eventsDao.getEventSubscriptions(cluster).stream()
            .filter(
                sub ->
                    Objects.equals(sub.getNodes(), nodes)
                        && Objects.equals(sub.getEvents(), events))
            .findFirst()
            .orElseGet(
                () -> {
                  created.set(true);
                  return diagEventService.addEventSubscription(
                      new DiagEventSubscription(
                          Optional.empty(),
                          cluster,
                          desc,
                          nodes,
                          events,
                          sse,
                          logger.isEmpty() ? null : logger,
                          endpoint.isEmpty() ? null : endpoint));
                });

    LOG.debug((created.get() ? "created" : "found") + " subscription {}", subscription);

    URI location =
        uriInfo
            .getBaseUriBuilder()
            .path("diag_event")
            .path("subscription")
            .path(subscription.getId().get().toString())
            .build();

    return created.get()
        ? Response.created(location).build()
        : Response.noContent().location(location).build();
  }

  @GET
  @Path("/{id}")
  @RolesAllowed({"user", "operator"})
  public Response getEventSubscription(@PathParam("id") UUID id) {
    LOG.debug("get subscription {}", id);
    try {
      return Response.ok().entity(diagEventService.getEventSubscription(id)).build();
    } catch (IllegalArgumentException ignore) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
  }

  @VisibleForTesting
  @GET
  @Path("/adhoc")
  @RolesAllowed({"user", "operator"})
  public Response getActiveAdhocSubscriptions() {
    try {
      Collection<DiagEventSubscription> allSubs = eventsDao.getEventSubscriptions();
      Set<DiagEventSubscription> activeSubscriptions =
          DiagEventSubscriptionService.getAdhocActiveSubs(allSubs, Collections.emptySet());
      if (activeSubscriptions.isEmpty()) {
        return Response.status(Response.Status.NOT_FOUND).build();
      }
      return Response.ok().entity(activeSubscriptions).build();
    } catch (RuntimeException e) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e).build();
    }
  }

  @VisibleForTesting
  @GET
  @Path("/pollers")
  @RolesAllowed({"user", "operator"})
  public Response getActivePollers() {
    try {
      if (DiagEventSubscriptionService.POLLERS_BY_NODE.isEmpty()) {
        return Response.status(Response.Status.NOT_FOUND).build();
      }
      return Response.ok().entity(DiagEventSubscriptionService.POLLERS_BY_NODE.size()).build();
    } catch (RuntimeException e) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e).build();
    }
  }

  @DELETE
  @Path("/{id}")
  @RolesAllowed({"operator"})
  public Response deleteEventSubscription(@PathParam("id") UUID id) {
    LOG.debug("delete subscription {}", id);
    try {
      diagEventService.deleteEventSubscription(id);
      return Response.accepted().build();
    } catch (IllegalArgumentException ignore) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
  }
}
