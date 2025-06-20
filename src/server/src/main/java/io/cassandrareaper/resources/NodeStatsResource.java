/*
 * Copyright 2018-2019 The Last Pickle Ltd
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cassandrareaper.resources;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.management.ClusterFacade;
import io.cassandrareaper.service.CompactionService;
import io.cassandrareaper.service.MetricsService;

import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import jakarta.annotation.security.RolesAllowed;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/node")
@Produces(MediaType.APPLICATION_JSON)
public final class NodeStatsResource {

  private static final Logger LOG = LoggerFactory.getLogger(NodeStatsResource.class);

  private final AppContext context;

  private final MetricsService metricsGrabber;
  private final CompactionService compactionService;

  public NodeStatsResource(AppContext context) throws ReaperException, InterruptedException {
    this.context = context;
    this.metricsGrabber = MetricsService.create(context);
    this.compactionService = CompactionService.create(context);
  }

  /**
   * Endpoint used to collect thread pool stats for a node.
   *
   * @return a list of thread pools if ok, and a status code 500 in case of errors.
   */
  @GET
  @Path("/tpstats/{clusterName}/{host}")
  @RolesAllowed({"user", "operator"})
  public Response getTpStats(
      @Context UriInfo uriInfo,
      @PathParam("clusterName") String clusterName,
      @PathParam("host") String host) {

    try {
      Node node =
          Node.builder()
              .withCluster(context.storage.getClusterDao().getCluster(clusterName))
              .withHostname(host)
              .build();

      return Response.ok().entity(metricsGrabber.getTpStats(node)).build();
    } catch (RuntimeException | ReaperException e) {
      LOG.error(e.getMessage(), e);
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  /**
   * Endpoint used to collect dropped messages stats for a node.
   *
   * @return a list of dropped messages metrics if ok, and a status code 500 in case of errors.
   */
  @GET
  @Path("/dropped/{clusterName}/{host}")
  @RolesAllowed({"user", "operator"})
  public Response getDroppedMessages(
      @Context UriInfo uriInfo,
      @PathParam("clusterName") String clusterName,
      @PathParam("host") String host) {

    try {
      Node node =
          Node.builder()
              .withCluster(context.storage.getClusterDao().getCluster(clusterName))
              .withHostname(host)
              .build();

      return Response.ok().entity(metricsGrabber.getDroppedMessages(node)).build();
    } catch (RuntimeException | ReaperException e) {
      LOG.error(e.getMessage(), e);
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  /**
   * Endpoint used to collect client request latencies for a node.
   *
   * @return a list of latency histograms if ok, and a status code 500 in case of errors.
   */
  @GET
  @Path("/clientRequestLatencies/{clusterName}/{host}")
  @RolesAllowed({"user", "operator"})
  public Response getClientRequestLatencies(
      @Context UriInfo uriInfo,
      @PathParam("clusterName") String clusterName,
      @PathParam("host") String host) {

    try {
      Node node =
          Node.builder()
              .withCluster(context.storage.getClusterDao().getCluster(clusterName))
              .withHostname(host)
              .build();

      return Response.ok().entity(metricsGrabber.getClientRequestLatencies(node)).build();
    } catch (RuntimeException | ReaperException e) {
      LOG.error(e.getMessage(), e);
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  /**
   * Endpoint used to collect thread pool stats for a node.
   *
   * @return a list of thread pools if ok, and a status code 500 in case of errors.
   */
  @GET
  @Path("/compactions/{clusterName}/{host}")
  @RolesAllowed({"user", "operator"})
  public Response listCompactions(
      @Context UriInfo uriInfo,
      @PathParam("clusterName") String clusterName,
      @PathParam("host") String host) {

    try {
      Node node =
          Node.builder()
              .withCluster(context.storage.getClusterDao().getCluster(clusterName))
              .withHostname(host)
              .build();

      return Response.ok().entity(compactionService.listActiveCompactions(node)).build();
    } catch (RuntimeException | ReaperException e) {
      LOG.error(e.getMessage(), e);
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  /**
   * Endpoint used to list the tokens owned by a node.
   *
   * @return a list of tokens.
   */
  @GET
  @Path("/tokens/{clusterName}/{host}")
  @RolesAllowed({"user", "operator"})
  public Response listTokens(
      @Context UriInfo uriInfo,
      @PathParam("clusterName") String clusterName,
      @PathParam("host") String host) {

    try {
      Preconditions.checkState(
          clusterName != null && !clusterName.isEmpty(), "Cluster name must be set");

      Map<String, List<String>> tokens =
          ClusterFacade.create(context)
              .getTokensByNode(context.storage.getClusterDao().getCluster(clusterName));

      return Response.ok().entity(tokens.get(host)).build();
    } catch (RuntimeException | ReaperException e) {
      LOG.error(e.getMessage(), e);
      return Response.serverError().entity(e.getMessage()).build();
    }
  }
}
