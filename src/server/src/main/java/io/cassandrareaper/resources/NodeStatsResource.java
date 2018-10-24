/*
 * Copyright 2018-2018 The Last Pickle Ltd
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
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.StreamSession;
import io.cassandrareaper.jmx.JmxProxy;
import io.cassandrareaper.jmx.StorageServiceProxy;
import io.cassandrareaper.service.CompactionService;
import io.cassandrareaper.service.MetricsService;
import io.cassandrareaper.service.StreamService;

import java.util.List;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/node")
@Produces(MediaType.APPLICATION_JSON)
public final class NodeStatsResource {

  private static final Logger LOG = LoggerFactory.getLogger(NodeStatsResource.class);

  private final AppContext context;
  private final StreamService streamManager;
  private final MetricsService metricsGrabber;
  private final CompactionService compactionService;

  public NodeStatsResource(AppContext context) {
    this.context = context;
    this.streamManager = StreamService.create(context);
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
  public Response getTpStats(
      @Context UriInfo uriInfo,
      @PathParam("clusterName") String clusterName,
      @PathParam("host") String host) {

    try {
      Node node = Node.builder().withClusterName(clusterName).withHostname(host).build();
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
  public Response getDroppedMessages(
      @Context UriInfo uriInfo,
      @PathParam("clusterName") String clusterName,
      @PathParam("host") String host) {

    try {
      Node node = Node.builder().withClusterName(clusterName).withHostname(host).build();
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
  public Response getClientRequestLatencies(
      @Context UriInfo uriInfo,
      @PathParam("clusterName") String clusterName,
      @PathParam("host") String host) {

    try {
      Node node = Node.builder().withClusterName(clusterName).withHostname(host).build();
      return Response.ok().entity(metricsGrabber.getClientRequestLatencies(node)).build();
    } catch (RuntimeException | ReaperException e) {
      LOG.error(e.getMessage(), e);
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  /**
   * Endpoint used to collect streams currently happening on a node.
   *
   * @return a list of {@link StreamSession} if ok, or a status code 500 in case of errors.
   */
  @GET
  @Path("/streams/{clusterName}/{host}")
  public Response getStreams(@PathParam("clusterName") String clusterName,
                             @PathParam("host") String host
  ) {
    try {
      Node node = Node.builder().withClusterName(clusterName).withHostname(host).build();
      List<StreamSession> streams = streamManager.listStreams(node);
      return Response.ok().entity(streams).build();
    } catch (ReaperException e) {
      LOG.error(e.getMessage(), e);
      return Response.status(500).entity(e.getMessage()).build();
    }
  }

  /**
   * Endpoint used to collect thread pool stats for a node.
   *
   * @return a list of thread pools if ok, and a status code 500 in case of errors.
   */
  @GET
  @Path("/compactions/{clusterName}/{host}")
  public Response listCompactions(
      @Context UriInfo uriInfo,
      @PathParam("clusterName") String clusterName,
      @PathParam("host") String host) {

    try {
      Node node = Node.builder().withClusterName(clusterName).withHostname(host).build();
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
  public Response listTokens(
      @Context UriInfo uriInfo,
      @PathParam("clusterName") String clusterName,
      @PathParam("host") String host)
      throws InterruptedException {

    try {
      JmxProxy jmxProxy
          = context.jmxConnectionFactory.connect(
              Node.builder().withClusterName(clusterName).withHostname(host).build(),
              context.config.getJmxConnectionTimeoutInSeconds());

      StorageServiceProxy ssProxy = StorageServiceProxy.create(jmxProxy);
      Map<String, List<String>> tokens = ssProxy.getTokensByNode();
      return Response.ok().entity(tokens.get(host)).build();
    } catch (RuntimeException | ReaperException e) {
      LOG.error(e.getMessage(), e);
      return Response.serverError().entity(e.getMessage()).build();
    }
  }
}
