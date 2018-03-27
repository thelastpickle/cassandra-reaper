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
import io.cassandrareaper.core.Snapshot;

import java.util.List;
import java.util.Map;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/snapshot")
@Produces(MediaType.APPLICATION_JSON)
public final class SnapshotResource {

  private static final Logger LOG = LoggerFactory.getLogger(SnapshotResource.class);

  private final AppContext context;

  public SnapshotResource(AppContext context) {
    this.context = context;
  }

  /**
   * Endpoint used to create a snapshot.
   *
   * @return nothing in case everything is ok, and a status code 500 in case of errors.
   */
  @POST
  @Path("/{clusterName}/{host}")
  public Response createSnapshot(
      @Context UriInfo uriInfo,
      @PathParam("clusterName") String clusterName,
      @PathParam("host") Optional<String> host,
      @QueryParam("keyspace") Optional<String> keyspace,
      @QueryParam("tables") Optional<String> tableNamesParam,
      @QueryParam("snapshot_name") Optional<String> snapshotName) {

    try {
      Node node = Node.builder().withClusterName(clusterName).withHostname(host.get()).build();
      Pair<Node, String> snapshot = Pair.of(node, "none");
      if (host.isPresent()) {
        if (keyspace.isPresent()) {
          snapshot =
              context.snapshotManager.takeSnapshotForKeyspaces(
                  context.snapshotManager.formatSnapshotName(
                      snapshotName.or(context.snapshotManager.SNAPSHOT_PREFIX)),
                  node,
                  keyspace.get());
        } else {
          snapshot =
              context.snapshotManager.takeSnapshot(
                  context.snapshotManager.formatSnapshotName(
                      snapshotName.or(context.snapshotManager.SNAPSHOT_PREFIX)),
                  node);
        }
        return Response.ok().entity(snapshot).build();
      } else {
        return Response.status(Status.BAD_REQUEST).entity("No host was specified for taking the snapshot.").build();
      }
    } catch (ReaperException e) {
      LOG.error(e.getMessage(), e);
      return Response.status(500).entity(e.getMessage()).build();
    }
  }

  /**
   * Endpoint used to create a snapshot.
   *
   * @return nothing in case everything is ok, and a status code 500 in case of errors.
   */
  @POST
  @Path("/cluster/{clusterName}")
  public Response createSnapshotClusterWide(
      @Context UriInfo uriInfo,
      @PathParam("clusterName") Optional<String> clusterName,
      @QueryParam("keyspace") Optional<String> keyspace,
      @QueryParam("snapshot_name") Optional<String> snapshotName,
      @QueryParam("owner") Optional<String> owner,
      @QueryParam("cause") Optional<String> cause) {

    try {
      List<Pair<Node, String>> snapshot = Lists.newArrayList();
      if (clusterName.isPresent()) {
        if (keyspace.isPresent() && !keyspace.get().isEmpty()) {
          snapshot =
              context.snapshotManager.takeSnapshotClusterWide(
                  context.snapshotManager.formatSnapshotName(
                      snapshotName.or(context.snapshotManager.SNAPSHOT_PREFIX)),
                  clusterName.get(),
                  owner.or("reaper"),
                  cause.or("Snapshot taken with Reaper"),
                  keyspace.get());
        } else {
          snapshot =
              context.snapshotManager.takeSnapshotClusterWide(
                  context.snapshotManager.formatSnapshotName(
                      snapshotName.or(context.snapshotManager.SNAPSHOT_PREFIX)),
                  clusterName.get(),
                  owner.or("reaper"),
                  cause.or("Snapshot taken with Reaper"));
        }
        return Response.ok().entity(snapshot).build();
      } else {
        return Response.status(Status.BAD_REQUEST)
            .entity("No cluster was specified for taking the snapshot.")
            .build();
      }
    } catch (ReaperException e) {
      LOG.error(e.getMessage(), e);
      return Response.status(500).entity(e.getMessage()).build();
    }
  }

  @GET
  @Path("/{clusterName}/{host}")
  public Response listSnapshots(
      @PathParam("clusterName") String clusterName, @PathParam("host") String host) {
    Map<String, List<Snapshot>> snapshots;
    try {
      snapshots =
          context.snapshotManager.listSnapshotsGroupedByName(
              Node.builder().withClusterName(clusterName).withHostname(host).build());
      return Response.ok().entity(snapshots).build();
    } catch (ReaperException e) {
      LOG.error(e.getMessage(), e);
      return Response.status(500).entity(e.getMessage()).build();
    }
  }

  @GET
  @Path("/cluster/{clusterName}")
  public Response listSnapshotsClusterWide(@PathParam("clusterName") String clusterName) {
    Map<String, Map<String, List<Snapshot>>> snapshots;
    try {
      snapshots = context.snapshotManager.listSnapshotsClusterWide(clusterName);
      return Response.ok().entity(snapshots).build();
    } catch (UnsupportedOperationException e) {
      return Response.status(Status.NOT_IMPLEMENTED).entity(e.getMessage()).build();
    } catch (ReaperException e) {
      LOG.error(e.getMessage(), e);
      return Response.status(500).entity(e.getMessage()).build();
    }
  }

  /**
   * Endpoint used to delete a snapshot.
   *
   * @return nothing in case everything is ok, and a status code 500 in case of errors.
   */
  @DELETE
  @Path("/{clusterName}/{host}/{snapshotName}")
  public Response clearSnapshot(
      @Context UriInfo uriInfo,
      @PathParam("clusterName") String clusterName,
      @PathParam("host") Optional<String> host,
      @PathParam("snapshotName") Optional<String> snapshotName) {

    try {
      if (host.isPresent() && snapshotName.isPresent()) {
        context.snapshotManager.clearSnapshot(
            snapshotName.get(),
            Node.builder().withClusterName(clusterName).withHostname(host.get()).build());
        return Response.ok().entity("Snapshot successfully cleared").build();
      } else {
        return Response.status(Status.BAD_REQUEST)
            .entity("Host and snapshot name are mandatory for clearing a snapshot.")
            .build();
      }
    } catch (ReaperException e) {
      LOG.error(e.getMessage(), e);
      return Response.status(500).entity(e.getMessage()).build();
    }
  }

  /**
   * Endpoint used to delete a snapshot.
   *
   * @return nothing in case everything is ok, and a status code 500 in case of errors.
   */
  @DELETE
  @Path("/cluster/{clusterName}/{snapshotName}")
  public Response clearSnapshotClusterWide(
      @Context UriInfo uriInfo,
      @PathParam("clusterName") Optional<String> clusterName,
      @PathParam("snapshotName") Optional<String> snapshotName) {

    try {
      if (clusterName.isPresent() && snapshotName.isPresent()) {
        context.snapshotManager.clearSnapshotClusterWide(snapshotName.get(), clusterName.get());
        return Response.ok().entity("Snapshots successfully cleared").build();
      } else {
        return Response.status(Status.BAD_REQUEST)
            .entity("Cluster and snapshot names are mandatory for clearing a snapshot.")
            .build();
      }
    } catch (ReaperException e) {
      LOG.error(e.getMessage(), e);
      return Response.status(500).entity(e.getMessage()).build();
    }
  }

}
