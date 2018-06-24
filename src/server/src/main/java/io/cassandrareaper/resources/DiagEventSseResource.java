/*
 * Copyright 2018-2018 Stefan Podkowinski
 *
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

import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

import org.apache.http.client.HttpClient;
import org.glassfish.jersey.media.sse.EventOutput;
import org.glassfish.jersey.media.sse.SseFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/diag_event/sse_listen")
public final class DiagEventSseResource {

  private static final Logger LOG = LoggerFactory.getLogger(DiagEventSseResource.class);

  private final DiagEventSubscriptionService diagEventService;

  public DiagEventSseResource(AppContext context, HttpClient httpClient) {
    this.diagEventService = DiagEventSubscriptionService.create(context, httpClient);
  }

  @GET
  @Path("/{id}")
  @Produces(SseFeature.SERVER_SENT_EVENTS)
  public synchronized EventOutput listen(
      @Context HttpServletRequest request,
      @PathParam("id") String id,
      @HeaderParam(SseFeature.LAST_EVENT_ID_HEADER) @DefaultValue("-1") int lastEventId) {

    LOG.debug("get subscription called with id: {}", id);

    DiagEventSubscription sub = diagEventService.getEventSubscription(UUID.fromString(id));
    if (null == sub) { // @fixme change to catch
      LOG.warn("Could not find diag event subscription with id {}", id);
      return null;
    }

    EventOutput eventOutput = new EventOutput();
    diagEventService.subscribe(sub, eventOutput, request.getRemoteAddr());
    return eventOutput;
  }

}