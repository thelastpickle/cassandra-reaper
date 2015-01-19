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

import com.spotify.reaper.ReaperApplicationConfiguration;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.ColumnFamily;
import com.spotify.reaper.resources.view.ColumnFamilyStatus;
import com.spotify.reaper.service.JmxConnectionFactory;
import com.spotify.reaper.storage.IStorage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

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
   * @return 500 if something goes wrong or args are missing, 200+ if requested operation
   * is successful.
   */
  @POST
  public Response addTable(
      @Context UriInfo uriInfo,
      @QueryParam("clusterName") Optional<String> clusterName,
      @QueryParam("keyspace") Optional<String> keyspace,
      @QueryParam("table") Optional<String> tableName) {

    LOG.info("add table called with: clusterName = {}, keyspace = {}, table = {}", clusterName,
        keyspace, tableName);

    try {
      if (!clusterName.isPresent()) {
        throw new ReaperException("\"clusterName\" argument missing");
      }
      if (!keyspace.isPresent()) {
        throw new ReaperException("\"keyspace\" argument missing");
      }
      if (!tableName.isPresent()) {
        throw new ReaperException("\"tableName\" argument missing");
      }
      String clusterNameStr = clusterName.get();
      String keyspaceStr = keyspace.get();
      String tableNameStr = tableName.get();
      URI tableUri = buildTableUri(uriInfo, clusterNameStr, keyspaceStr, tableNameStr);
      ColumnFamily newTable = registerNewTable(tableUri, clusterNameStr, keyspaceStr, tableNameStr);
      return Response.created(buildTableUri(uriInfo, clusterNameStr, keyspaceStr, tableNameStr))
          .entity(new ColumnFamilyStatus(newTable))
          .build();
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
  private URI buildTableUri(UriInfo uriInfo, String clusterName, String keyspace, String table)
      throws ReaperException {
    String tablePath = String.format("%s/%s/%s", clusterName, keyspace, table);
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
  private ColumnFamily registerNewTable(URI tableUri,  String clusterName, String keyspace,
      String table) throws ReaperException {
    // fetch information about the cluster the table is added to
    Cluster targetCluster = storage.getCluster(clusterName);
    if (targetCluster == null) {
      String errMsg = String.format("Failed to fetch cluster for table \"%s\"", table);
      throw new ReaperException(errMsg);
    }

    // store the new table
    return storeNewTable(tableUri, targetCluster, keyspace, table);
  }

  /**
   * Stores a table information into the storage.
   * @return
   * @throws ReaperException if table already exists in the storage or in Cassandra cluster.
   */
  private ColumnFamily storeNewTable(URI createdUri, Cluster cluster, String keyspace, String table)
      throws ReaperException {
    String clusterName = cluster.getName();

    // check if the table doesn't already exists in Reaper's storage
    ColumnFamily existingTable = storage.getColumnFamily(clusterName, keyspace, table);
    if (existingTable != null) {
      String errMsg = String.format("table \"%s\" already exists", createdUri.toString());
      throw new ReaperException(errMsg);
    }

    // check if the table actually exists in the Cassandra cluster
    if (!existsInCluster(cluster, keyspace, table)) {
      String errMsg = String.format("table \"%s\" doesn't exist in Cassandra",
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

}
