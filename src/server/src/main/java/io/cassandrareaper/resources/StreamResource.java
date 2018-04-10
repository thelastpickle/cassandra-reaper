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
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.Stream;
import io.cassandrareaper.core.StreamSession;

import java.util.List;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/stream")
@Produces(MediaType.APPLICATION_JSON)
public final class StreamResource {

  private static final Logger LOG = LoggerFactory.getLogger(StreamResource.class);

  private final AppContext context;

  public StreamResource(AppContext context) {
    this.context = context;
  }

  @GET
  @Path("/{clusterName}/{host}")
  public Response getStreams(
      @PathParam("clusterName") String clusterName, @PathParam("host") String host) {
    List<Stream> streams;
    try {
      streams = context
          .streamManager
          .listStreams(Node.builder().withClusterName(clusterName).withHostname(host).build());
      return Response.ok().entity(streams).build();
    } catch (ReaperException e) {
      LOG.error(e.getMessage(), e);
      return Response.status(500).entity(e.getMessage()).build();
    }
  }

  @GET
  @Path("/cluster/{clusterName}")
  public Response getStreamsClusterWide(@PathParam("clusterName") String clusterName) {
    Map<String, StreamSession> streams;
    try {
      streams = context.streamManager.listStreams(clusterName);
      return Response.ok().entity(streams).build();
    } catch (ReaperException e) {
      LOG.error(e.getMessage(), e);
      return Response.status(500).entity(e.getMessage()).build();
    }
  }

}
