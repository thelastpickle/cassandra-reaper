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

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.http.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/diag_event/subscription")
@Produces(MediaType.APPLICATION_JSON)
public final class DiagEventSubscriptionResource {

  private static final Logger LOG = LoggerFactory.getLogger(DiagEventSubscriptionResource.class);

  private final AppContext context;
  private final DiagEventSubscriptionService diagEventService;

  public DiagEventSubscriptionResource(AppContext context, HttpClient httpClient) {
    this.context = context;
    this.diagEventService = DiagEventSubscriptionService.create(context, httpClient);
  }

  @GET
  public Response getEventSubscriptionList(@QueryParam("clusterName") String clusterName) {
    LOG.debug("get event subscriptions called");
    Collection<DiagEventSubscription> subscriptions = context.storage.getEventSubscriptions(clusterName);
    return Response.ok().entity(subscriptions).build();
  }

  @POST
  public Response addEventSubscription(
          @QueryParam("clusterName") String clusterName,
          @QueryParam("description") Optional<String> description,
          @QueryParam("nodes") String nodes,
          @QueryParam("events") String events,
          @QueryParam("exportSse") boolean exportSse,
          @QueryParam("exportFileLogger") String exportFileLogger,
          @QueryParam("exportHttpEndpoint") String exportHttpEndpoint) {

    DiagEventSubscription subscription = new DiagEventSubscription(
        Optional.empty(),
        clusterName,
        description,
        Arrays.asList(nodes == null ? new String[]{} : nodes.split(",")),
        Arrays.asList(events == null ? new String[]{} : events.split(",")),
        exportSse,
        exportFileLogger,
        exportHttpEndpoint);

    LOG.debug("creating event subscription: {}", subscription);
    subscription = diagEventService.addEventSubscription(subscription);

    return Response.ok().entity(subscription).build();
  }

  @GET
  @Path("/{id}")
  public Response getEventSubscription(@PathParam("id") UUID id) {
    LOG.debug("get subscription called with id: {}", id);
    try {
      return Response.ok().entity(diagEventService.getEventSubscription(id)).build();
    } catch (IllegalArgumentException ignore) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
  }

  @DELETE
  @Path("/{id}")
  public Response deleteEventSubscription(@PathParam("id") UUID id) {
    diagEventService.deleteEventSubscription(id);
    return Response.accepted().build();
  }
}
