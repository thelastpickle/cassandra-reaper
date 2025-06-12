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

import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

import jakarta.annotation.security.RolesAllowed;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.glassfish.jersey.media.sse.EventOutput;
import org.glassfish.jersey.media.sse.SseFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/diag_event/sse_listen")
public final class DiagEventSseResource {

  private static final Logger LOG = LoggerFactory.getLogger(DiagEventSseResource.class);

  private final DiagEventSubscriptionService diagEventService;

  public DiagEventSseResource(
      AppContext context,
      CloseableHttpClient httpClient,
      ScheduledExecutorService executor,
      IEventsDao eventsDao) {
    this.diagEventService =
        DiagEventSubscriptionService.create(context, httpClient, executor, eventsDao);
  }

  @GET
  @Path("/{id}")
  @Produces(SseFeature.SERVER_SENT_EVENTS)
  @RolesAllowed({"user", "operator"})
  public synchronized EventOutput listen(
      @Context HttpServletRequest request,
      @PathParam("id") String id,
      @HeaderParam(SseFeature.LAST_EVENT_ID_HEADER) @DefaultValue("-1") int lastEventId) {

    LOG.debug("get subscription called with id: {}", id);
    DiagEventSubscription sub = diagEventService.getEventSubscription(UUID.fromString(id));
    EventOutput eventOutput = new EventOutput();
    diagEventService.subscribe(sub, eventOutput, request.getRemoteAddr());
    return eventOutput;
  }
}
