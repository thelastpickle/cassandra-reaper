package com.spotify.reaper.resources;

import com.google.common.base.Optional;

import com.spotify.reaper.ReaperApplicationConfiguration;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.ColumnFamily;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.service.SegmentGenerator;
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
  public Response getCluster(@PathParam("clusterName") String clusterName,
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
                           @QueryParam("startRepair") Optional<Boolean> startRepair) {
    LOG.info("add table called with: clusterName = {}, seedHost = {}, keyspace = {}, table = {}",
             clusterName, seedHost, keyspace, table);

    if (!keyspace.isPresent()) {
      return Response.status(400)
          .entity("Query parameter \"keyspace\" required").build();
    }
    if (!table.isPresent()) {
      return Response.status(400)
          .entity("Query parameter \"table\" required").build();
    }

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
      if (null == existingCluster) {
        LOG.info("creating new cluster based on given seed host: {}", seedHost);
        storage.addCluster(targetCluster);
      }
      if (!existingCluster.equals(targetCluster)) {
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

    // TODO: verify that the table exists in the cluster.
    ColumnFamily newTable = new ColumnFamily.Builder()
        .cluster(targetCluster)
        .keyspaceName(keyspace.get())
        .name(table.get())
        .keyspaceName(keyspace.get())
        .snapshotRepair(config.getSnapshotRepair())
        .segmentCount(config.getSegmentCount())
        .build();

    String newTablePathPart = newTable.getCluster().getName() + "/" + newTable
        .getKeyspaceName() + "/" + newTable.getName();
    if (!storage.addColumnFamily(newTable)) {
      return Response.status(500)
          .entity("failed creating table: " + newTablePathPart).build();
    }

    URI createdURI = null;
    try {
      createdURI = (new URL(uriInfo.getAbsolutePath().toURL(), newTablePathPart)).toURI();
    } catch (Exception e) {
      String errMsg = "failed creating target URI for new table: " + newTablePathPart;
      LOG.error(errMsg);
      e.printStackTrace();
      return Response.status(400).entity(errMsg).build();
    }

    // If startRepair query parameter is given at all, i.e. value not checked.
    if (startRepair.isPresent()) {
      // create repair run
      RepairRun repairRun =
          storage.addRepairRun("Manually invoked", "No owner specified", DateTime.now(),
                               config.getRepairIntensity());

      // create segments
      List<RepairSegment> segments = null;
      try {
        SegmentGenerator sg = new SegmentGenerator(targetCluster.getPartitioner());
        Set<String> seedHosts = targetCluster.getSeedHosts();
        for (String host : seedHosts) {
          try {
            JmxProxy jmxProxy = JmxProxy.connect(host);
            List<BigInteger> tokens = jmxProxy.getTokens();
            segments =
                sg.generateSegments(newTable.getSegmentCount(), tokens, repairRun.getId(),
                                    newTable);
            jmxProxy.close();
            break;
          } catch (ReaperException e) {
            LOG.info("couldn't connect to host: {}", host);
          }
        }

        if (segments == null) {
          String errMsg =
              "couldn't connect to any of the seed hosts in cluster \"" + clusterName + "\"";
          LOG.info(errMsg);
          throw new ReaperException(errMsg);
        }
      } catch (ReaperException e) {
        String errMsg = "failed generating segments for new table: " + newTable;
        LOG.error(errMsg);
        e.printStackTrace();
        return Response.status(400).entity(errMsg).build();
      }

      // TODO:
      // store segments
      // initialize segment states
      // store repair run
      // create new runner for the run
      // start the runner and return pointer to new RepairRun
      // runner holds open jmx proxy to update segment states
      // runner checks storage after every segment, if run state has changed (paused etc.)
    }

    return Response.created(createdURI).entity(newTable).build();
  }

}
