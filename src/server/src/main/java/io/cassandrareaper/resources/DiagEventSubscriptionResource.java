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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
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

import com.google.common.base.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/diag_event/subscription")
@Produces(MediaType.APPLICATION_JSON)
public class DiagEventSubscriptionResource {

  private static final Logger LOG = LoggerFactory.getLogger(DiagEventSubscriptionResource.class);

  private final AppContext context;

  public DiagEventSubscriptionResource(AppContext context) {
    this.context = context;
  }

  @GET
  public Response getEventSubscriptionList(
          @QueryParam("clusterName") String clusterName) {

    LOG.debug("get event subscriptions called");
    Collection<DiagEventSubscription> subscriptions = context.storage.getEventSubscriptions(clusterName);
    return Response.ok().entity(subscriptions).build();
  }

  @POST
  public Response addEventSubscription(
          @QueryParam("clusterName") String clusterName,
          @QueryParam("description") Optional<String> description,
          @QueryParam("includeNodes") String sincludeNodes,
          @QueryParam("events") String sevents,
          @QueryParam("exportSse") boolean exportSse,
          @QueryParam("exportFileLogger") String exportFileLogger,
          @QueryParam("exportHttpEndpoint") String exportHttpEndpoint) {

    List<String> events = Arrays.asList(sevents == null ? new String[]{} : sevents.split(","));
    List<String> includeNodes = Arrays.asList(sincludeNodes == null ? new String[]{} : sincludeNodes.split(","));
    DiagEventSubscription subscription = new DiagEventSubscription(null, clusterName, description.orNull(),
            includeNodes, events, exportSse, exportFileLogger, exportHttpEndpoint);
    LOG.debug("creating event subscription: {}", subscription);
    subscription = context.storage.addEventSubscription(subscription);

    return Response.ok().entity(subscription).build();
  }

  @GET
  @Path("/{id}")
  public Response getEventSubscription(
          @PathParam("id") UUID id) {

    LOG.debug("get subscription called with id: {}", id);
    DiagEventSubscription subscription = context.storage.getEventSubscription(id);
    return Response.ok().entity(subscription).build();
  }

  @DELETE
  @Path("/{id}")
  public Response deleteEventSubscription(
          @PathParam("id") UUID id) {

    LOG.debug("deleting subscription with id: {}", id);
    DiagEventSubscription subscription = context.storage.getEventSubscription(id);
    if (subscription == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    assert id.equals(subscription.getId());
    if (context.storage.deleteEventSubscription(id)) {
      return Response.ok().entity(subscription).build();
    } else {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }
}