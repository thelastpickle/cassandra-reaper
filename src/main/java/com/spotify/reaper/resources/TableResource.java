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
package com.spotify.reaper.resources;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import com.spotify.reaper.ReaperApplicationConfiguration;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.ColumnFamily;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.resources.view.ColumnFamilyStatus;
import com.spotify.reaper.service.JmxConnectionFactory;
import com.spotify.reaper.service.RingRange;
import com.spotify.reaper.service.SegmentGenerator;
import com.spotify.reaper.service.RepairRunner;
import com.spotify.reaper.storage.IStorage;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Set;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

@Path("/table")
@Produces(MediaType.APPLICATION_JSON)
public class TableResource {

  private static final Logger LOG = LoggerFactory.getLogger(TableResource.class);

  private final IStorage storage;
  private final ReaperApplicationConfiguration config;

  public TableResource(ReaperApplicationConfiguration config, IStorage storage) {
    this.storage = storage;
    this.config = config;
  }

  @GET
  @Path("/{clusterName}/{keyspace}/{table}")
  public Response getTable(@PathParam("clusterName") String clusterName,
                             @PathParam("keyspace") String keyspace,
                             @PathParam("table") String table) {
    LOG.info("get table called with: clusterName = {}, keyspace = {}, table = {}",
             clusterName, keyspace, table);
    return Response.ok().entity("not implemented yet").build();
  }

  @POST
  public Response addTable(@Context UriInfo uriInfo,
                           @QueryParam("clusterName") Optional<String> clusterName,
                           @QueryParam("seedHost") Optional<String> seedHost,
                           @QueryParam("keyspace") Optional<String> keyspace,
                           @QueryParam("table") Optional<String> table,
                           @QueryParam("startRepair") Optional<Boolean> startRepair,
                           @QueryParam("owner") Optional<String> owner,
                           @QueryParam("cause") Optional<String> cause) {
    LOG.info("add table called with: clusterName = {}, seedHost = {}, keyspace = {}, table = {}, "
             + "owner = {}, cause = {}", clusterName, seedHost, keyspace, table, owner, cause);

    if (!keyspace.isPresent()) {
      return Response.status(400)
          .entity("Query parameter \"keyspace\" required").build();
    }
    if (!table.isPresent()) {
      return Response.status(400)
          .entity("Query parameter \"table\" required").build();
    }
    if (!owner.isPresent()) {
      return Response.status(400)
          .entity("Query parameter \"owner\" required").build();
    }

    // TODO: split this method and clean-up when MVP feature "complete"

    Cluster targetCluster;
    if (seedHost.isPresent()) {
      try {
        targetCluster = ClusterResource.createClusterWithSeedHost(seedHost.get());
      } catch (ReaperException e) {
        e.printStackTrace();
        return Response.status(400)
            .entity("failed creating cluster with seed host: " + seedHost.get()).build();
      }
      Cluster existingCluster = storage.getCluster(targetCluster.getName());
      if (existingCluster == null) {
        LOG.info("creating new cluster based on given seed host: {}", seedHost);
        storage.addCluster(targetCluster);
      } else if (!existingCluster.equals(targetCluster)) {
        LOG.info("cluster information has changed for cluster: {}", targetCluster.getName());
        storage.updateCluster(targetCluster);
      }
    } else if (clusterName.isPresent()) {
      targetCluster = storage.getCluster(clusterName.get());
      if (null == targetCluster) {
        return Response.status(404)
            .entity("cluster \"" + clusterName + "\" does not exist").build();
      }
    } else {
      return Response.status(400)
          .entity("Query parameter \"clusterName\" or \"seedHost\" required").build();
    }

    String newTablePathPart = targetCluster.getName() + "/" + keyspace.get()
                              + "/" + table.get();
    URI createdURI;
    try {
      createdURI = (new URL(uriInfo.getAbsolutePath().toURL(), newTablePathPart)).toURI();
    } catch (Exception e) {
      String errMsg = "failed creating target URI for table: " + newTablePathPart;
      LOG.error(errMsg);
      e.printStackTrace();
      return Response.status(400).entity(errMsg).build();
    }

    // TODO: verify that the table exists in the cluster.
    ColumnFamily existingTable =
        storage.getColumnFamily(targetCluster.getName(), keyspace.get(), table.get());
    if (existingTable == null) {
      LOG.info("storing new table");

      existingTable = storage.addColumnFamily(
          new ColumnFamily.Builder(targetCluster.getName(), keyspace.get(), table.get(),
                                   config.getSegmentCount(), config.getSnapshotRepair()));

      if (existingTable == null) {
        return Response.status(500)
            .entity("failed creating table into Reaper storage: " + newTablePathPart).build();
      }
    }

    // Start repairing the table if the startRepair query parameter is given at all,
    // i.e. possible value not checked, and not required.
    if (!startRepair.isPresent()) {
      return Response.created(createdURI).entity(new ColumnFamilyStatus(existingTable)).build();
    }

    // create segments
    List<RingRange> segments = null;
    String usedSeedHost = null;
    try {
      SegmentGenerator sg = new SegmentGenerator(targetCluster.getPartitioner());
      Set<String> seedHosts = targetCluster.getSeedHosts();
      for (String host : seedHosts) {
        try {
          JmxProxy jmxProxy = JmxProxy.connect(host);
          List<BigInteger> tokens = jmxProxy.getTokens();
          segments = sg.generateSegments(existingTable.getSegmentCount(), tokens);
          jmxProxy.close();
          usedSeedHost = host;
          break;
        } catch (ReaperException e) {
          LOG.info("couldn't connect to host: {}", host);
        }
      }

      if (segments == null || seedHosts.isEmpty()) {
        return Response.status(404)
            .entity("couldn't connect to any of the seed hosts in cluster \"" + existingTable
                .getClusterName() + "\"").build();
      }
    } catch (ReaperException e) {
      String errMsg = "failed generating segments for new table: " + existingTable;
      LOG.error(errMsg);
      e.printStackTrace();
      return Response.status(400).entity(errMsg).build();
    }

    RepairRun newRepairRun =
        storage.addRepairRun(new RepairRun.Builder(targetCluster.getName(),
                                                   existingTable.getId(),
                                                   RepairRun.RunState.NOT_STARTED,
                                                   DateTime.now(),
                                                   config.getRepairIntensity())
            .cause(cause.isPresent() ? cause.get() : "no cause specified")
            .owner(owner.get()));
    if (newRepairRun == null) {
      return Response.status(500)
          .entity("failed creating repair run into Reaper storage for owner: " + owner.get())
          .build();
    }

    // Notice that our RepairRun core object doesn't contain pointer to
    // the set of RepairSegments in the run, as they are accessed separately.
    // RepairSegment has a pointer to the RepairRun it lives in.
    List<RepairSegment.Builder> repairSegments = Lists.newArrayList();
    for (RingRange range : segments) {
      repairSegments
          .add(new RepairSegment.Builder(newRepairRun.getId(), range,
              RepairSegment.State.NOT_STARTED).columnFamilyId(existingTable.getId()));
    }
    storage.addRepairSegments(repairSegments);

    RepairRunner.startNewRepairRun(storage, newRepairRun.getId(), new JmxConnectionFactory());

    String newRepairRunPathPart = "repair_run/" + newRepairRun.getId();
    URI createdRepairRunURI;
    try {
      createdRepairRunURI = (new URL(uriInfo.getBaseUri().toURL(), newRepairRunPathPart)).toURI();
    } catch (Exception e) {
      String errMsg = "failed creating target URI for new repair run: " + newRepairRunPathPart;
      LOG.error(errMsg);
      e.printStackTrace();
      return Response.status(400).entity(errMsg).build();
    }

    return Response.created(createdRepairRunURI)
        .entity(new ColumnFamilyStatus(existingTable)).build();
  }

}
