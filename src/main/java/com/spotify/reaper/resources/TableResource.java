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
import com.spotify.reaper.service.RepairRunner;
import com.spotify.reaper.service.RingRange;
import com.spotify.reaper.service.SegmentGenerator;
import com.spotify.reaper.storage.IStorage;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
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

import static com.google.common.base.Preconditions.checkNotNull;

@Path("/table")
@Produces(MediaType.APPLICATION_JSON)
public class TableResource {

  private static final Logger LOG = LoggerFactory.getLogger(TableResource.class);

  private final IStorage storage;
  private final ReaperApplicationConfiguration config;
  private final JmxConnectionFactory jmxFactory;

  public TableResource(ReaperApplicationConfiguration config, IStorage storage) {
    this.storage = storage;
    this.config = config;
    this.jmxFactory = new JmxConnectionFactory();
  }

  public TableResource(ReaperApplicationConfiguration config, IStorage storage,
      JmxConnectionFactory jmxFactory) {
    this.storage = storage;
    this.config = config;
    this.jmxFactory = jmxFactory;
  }


  /**
   * Will return repair status of a table.
   * @return
   */
  @GET
  @Path("/{clusterName}/{keyspace}/{table}")
  public Response getTable(
      @PathParam("clusterName") String clusterName,
      @PathParam("keyspace") String keyspace,
      @PathParam("table") String table) {
    LOG.info("get table called with: clusterName = {}, keyspace = {}, table = {}",
        clusterName, keyspace, table);
    return Response.ok().entity("not implemented yet").build();
  }

  /**
   * Very beastly endpoint to register a new table, with the option to immediately trigger
   * a repair.
   *
   * @return 400 if some args are missing, 500 if something goes wrong, 200 if requested operation
   * is successful.
   */
  @POST
  public Response addTable(
      @Context UriInfo uriInfo,
      @QueryParam("clusterName") Optional<String> clusterName,
      @QueryParam("seedHost") Optional<String> seedHost,
      @QueryParam("keyspace") Optional<String> keyspace,
      @QueryParam("table") Optional<String> tableName,
      @QueryParam("startRepair") Optional<Boolean> startRepair,
      @QueryParam("owner") Optional<String> owner,
      @QueryParam("cause") Optional<String> cause) {

    LOG.info("add table called with: clusterName = {}, seedHost = {}, keyspace = {}, table = {}, "
        + "owner = {}, cause = {}", clusterName, seedHost, keyspace, tableName, owner, cause);

    if (!keyspace.isPresent()) {
      return Response.status(400).entity("Query parameter \"keyspace\" required").build();
    }
    if (!tableName.isPresent()) {
      return Response.status(400).entity("Query parameter \"table\" required").build();
    }
    if (!owner.isPresent()) {
      return Response.status(400).entity("Query parameter \"owner\" required").build();
    }
    if (!seedHost.isPresent() && !clusterName.isPresent()) {
      String msg = "Either \"clusterName\" or \"seedHost\" is required";
      return Response.status(400).entity(msg).build();
    }

    try {
      URI tableUri = buildTableUri(uriInfo, clusterName, keyspace, tableName);

      ColumnFamily table = registerNewTable(tableUri, seedHost, clusterName, keyspace, tableName);

      if (!startRepair.isPresent()) {
        return Response.created(tableUri).entity(new ColumnFamilyStatus(table)).build();
      }

      URI createdRepairRunURI = triggerRepairRun(uriInfo, table, cause, owner);

      Response response = Response.created(createdRepairRunURI)
          .entity(new ColumnFamilyStatus(table))
          .build();
      return response;
    } catch (ReaperException e) {
      LOG.error(e.getMessage());
      e.printStackTrace();
      return Response.status(500).entity(e.getMessage()).build();
    }
  }

  /**
   * Builds an URI used to describe a table.
   * @throws ReaperException
   */
  private URI buildTableUri(UriInfo uriInfo, Optional<String> clusterName,
      Optional<String> keyspace, Optional<String> table) throws ReaperException {
    String tablePath = String.format("%s/%s/%s", clusterName.get(), keyspace.get(), table.get());
    try {
      return new URL(uriInfo.getAbsolutePath().toURL(), tablePath).toURI();
    } catch (MalformedURLException | URISyntaxException e) {
      e.printStackTrace();
      throw new ReaperException(e);
    }
  }

  /**
   * Registers a new table by first fetching a cluster info from the storage backend, and
   * consequently storing the table.
   *
   * Cluster is keyed by seedHost if present, otherwise clusterName is used.
   *
   * @return
   * @throws ReaperException from below
   */
  private ColumnFamily registerNewTable(URI tableUri, Optional<String> seedHost,
      Optional<String> clusterName, Optional<String> keyspace, Optional<String> table)
      throws ReaperException {
    // fetch information about the cluster the table is added to
    Cluster targetCluster = null;
    if (seedHost.isPresent()) {
      targetCluster = getClusterBySeed(seedHost.get());
    } else {
      targetCluster = getClusterByName(clusterName.get());
    }
    String msg = String.format("Failed to fetch cluster for table \"%s\"", table.get());
    checkNotNull(targetCluster, msg);

    // store the new table
    ColumnFamily existingTable = storeNewTable(tableUri, targetCluster, keyspace.get(), table.get());
    String errMsg = String.format("failed creating new table: \"%s\"", table.get());
    checkNotNull(tableUri, errMsg);
    checkNotNull(existingTable, errMsg);
    return existingTable;
  }

  /**
   * Queries the storage backend for cluster information based on a seedHost.
   * @return
   * @throws ReaperException if cluster can't be found
   */
  private Cluster getClusterBySeed(String seedHost) throws ReaperException {
    Cluster targetCluster;
    try {
      targetCluster = ClusterResource.createClusterWithSeedHost(seedHost, jmxFactory);
      Cluster existingCluster = storage.getCluster(targetCluster.getName());
      if (existingCluster == null) {
        LOG.info("creating new cluster based on given seed host: {}", seedHost);
        storage.addCluster(targetCluster);
      } else if (!existingCluster.equals(targetCluster)) {
        LOG.info("cluster information has changed for cluster: {}", targetCluster.getName());
        storage.updateCluster(targetCluster);
      }
      return targetCluster;
    } catch (ReaperException e) {
      e.printStackTrace();
      throw new ReaperException("failed creating cluster with seed host: " + seedHost);
    }
  }

  /**
   * Queries the storage backed for cluster information based on clusterName.
   * @return
   * @throws ReaperException if cluster can't be found
   */
  private Cluster getClusterByName(String clusterName) throws ReaperException {
    Cluster targetCluster = storage.getCluster(clusterName);
    if (targetCluster == null) {
      throw new ReaperException(String.format("Cluster \"%s\" does not exist.", clusterName));
    }
    return targetCluster;
  }

  /**
   * Stores a table information into the storage.
   * @return
   * @throws ReaperException if table already exists in the storage or in Cassandra cluster.
   */
  private ColumnFamily storeNewTable(URI createdUri, Cluster cluster, String keyspace,
      String table) throws ReaperException {
    String clusterName = cluster.getName();

    // check if the table doesn't already exists in Reaper's storage
    ColumnFamily existingTable = storage.getColumnFamily(clusterName, keyspace, table);
    if (existingTable != null) {
      String errMsg = String.format("table \"%s\" already exists", createdUri.toString());
      throw new ReaperException(errMsg);
    }

    // check if the table actually exists in the Cassandra cluster
    if (!existsInCluster(cluster, keyspace, table)) {
      String errMsg = String.format("table \"%s\" doesn't exists in Cassandra",
          createdUri.toString());
      throw new ReaperException(errMsg);
    }

    // actually store the new table
    LOG.info(String.format("storing new table \"%s\"", createdUri.toString()));
    ColumnFamily.Builder newCf = new ColumnFamily.Builder(clusterName, keyspace, table,
        config.getSegmentCount(), config.getSnapshotRepair());
    existingTable = storage.addColumnFamily(newCf);
    if (existingTable == null) {
      String errMsg = String.format("failed storing new table \"%s\"", createdUri.toString());
      throw new ReaperException(errMsg);
    }
    return existingTable;
  }

  /**
   * Checks if given table actually exists in the Cassandra cluster.
   * @return
   */
  private boolean existsInCluster(Cluster cluster, String keyspace, String table) {
    String seedHost = cluster.getSeedHosts().iterator().next();
    try {
      return jmxFactory.create(seedHost).tableExists(keyspace, table);
    } catch (ReaperException e) {
      LOG.error(e.getMessage());
      e.printStackTrace();
      return false;
    }
  }

  /**
   * Triggers a repair run for the given table.
   *
   * This involves:
   *   1) split token range into segments
   *   2) create a RepairRun instance
   *   3) create RepairSegment instances linked to RepairRun. these are directly stored in storage
   *   4) change actually trigger the RepairRun
   * @return
   * @throws ReaperException from below
   */
  private URI triggerRepairRun(UriInfo uriInfo, ColumnFamily existingTable, Optional<String> cause,
      Optional<String> owner) throws ReaperException {

    Cluster targetCluster = getClusterByName(existingTable.getClusterName());

    // startRepair query parameter is present, will proceed with setting up & starting a repair
    // the first step is to generate token segments
    List<RingRange> tokenSegments = generateSegments(targetCluster, existingTable);
    checkNotNull(tokenSegments, "failed generating repair segments");

    // the next step is to prepare a repair run object
    RepairRun repairRun = prepareRepairRun(targetCluster, existingTable, cause.get(), owner.get());
    checkNotNull(repairRun, "failed preparing repair run");

    // Notice that our RepairRun core object doesn't contain pointer to
    // the set of RepairSegments in the run, as they are accessed separately.
    // However, RepairSegment has a pointer to the RepairRun it lives in

    // the next step is to generate actual repair segments
    prepareRepairSegments(tokenSegments, repairRun, existingTable);

    // with all the repair segments generated and stored, we can trigger the repair run
    RepairRunner.startNewRepairRun(storage, repairRun.getId(), jmxFactory);

    return buildRepairRunURI(uriInfo, repairRun);
  }

  /**
   * Splits a token range for given table into segments
   * @return
   * @throws ReaperException when fails to discover seeds for the cluster or fails to connect to
   *   any of the nodes in the Cluster.
   */
  private List<RingRange> generateSegments(Cluster targetCluster, ColumnFamily existingTable)
      throws ReaperException {
    List<RingRange> segments = null;
    SegmentGenerator sg = new SegmentGenerator(targetCluster.getPartitioner());
    Set<String> seedHosts = targetCluster.getSeedHosts();
    if (seedHosts.isEmpty()) {
      String errMsg = String.format("didn't get any seed hosts for cluster \"%s\"",
          existingTable.getClusterName());
      LOG.error(errMsg);
      throw new ReaperException(errMsg);
    }
    for (String host : seedHosts) {
      try {
        JmxProxy jmxProxy = jmxFactory.create(host);
        List<BigInteger> tokens = jmxProxy.getTokens();
        segments = sg.generateSegments(existingTable.getSegmentCount(), tokens);
        jmxProxy.close();
        break;
      } catch (ReaperException e) {
        LOG.warn("couldn't connect to host: {}, will try next one", host);
      }
    }
    if (segments == null) {
      String errMsg = String.format("failed to generate repair segments for cluster \"%s\"",
          existingTable.getClusterName());
      LOG.error(errMsg);
      throw new ReaperException(errMsg);
    }
    return segments;
  }

  /**
   * Instantiates a RepairRun and stores it in the storage backend.
   * @return
   * @throws ReaperException when fails to store the RepairRun.
   */
  private RepairRun prepareRepairRun(Cluster targetCluster, ColumnFamily existingTable,
      String cause, String owner) throws ReaperException {
    RepairRun.Builder runBuilder = new RepairRun.Builder(targetCluster.getName(),
        existingTable.getId(), DateTime.now(),
        config.getRepairIntensity());
    runBuilder.cause(cause == null ? "no cause specified" : cause);
    runBuilder.owner(owner);
    RepairRun newRepairRun = storage.addRepairRun(runBuilder);
    if (newRepairRun == null) {
      String errMsg = String.format("failed storing repair run for cluster \"%s/%s/%s",
          targetCluster.getName(), existingTable.getKeyspaceName(), existingTable.getName());
      LOG.error(errMsg);
      throw new ReaperException(errMsg);
    }
    return newRepairRun;
  }

  /**
   * Creates the repair runs linked to given RepairRun and stores them directly in the
   * storage backend.
   */
  private void prepareRepairSegments(List<RingRange> tokenSegments,
      RepairRun repairRun, ColumnFamily existingTable) {
    List <RepairSegment.Builder> repairSegmentBuilders = Lists.newArrayList();
    for (RingRange range : tokenSegments) {
      RepairSegment.Builder repairSegment =
          new RepairSegment.Builder(repairRun.getId(), range, existingTable.getId());
      repairSegmentBuilders.add(repairSegment);
    }
    storage.addRepairSegments(repairSegmentBuilders, repairRun.getId());
  }

  /**
   * Crafts an URI used to identify given repair run.
   * @return
   */
  private URI buildRepairRunURI(UriInfo uriInfo, RepairRun repairRun) {
    String newRepairRunPathPart = "repair_run/" + repairRun.getId();
    URI runUri = null;
    try {
      runUri = new URL(uriInfo.getBaseUri().toURL(), newRepairRunPathPart).toURI();
    } catch (MalformedURLException | URISyntaxException e) {
      LOG.error(e.getMessage());
      e.printStackTrace();
    }
    checkNotNull(runUri, "failed to build repair run uri");
    return runUri;
  }
}
