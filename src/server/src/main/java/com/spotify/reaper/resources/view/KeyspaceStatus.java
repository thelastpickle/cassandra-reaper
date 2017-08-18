package com.spotify.reaper.resources.view;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.reaper.core.Cluster;

import java.util.Collection;

/**
 * Contains the data to be shown when querying keyspace status for a cluster.
 */
public class KeyspaceStatus {

  @JsonProperty("cluster_name")
  private final String clusterName;

  @JsonProperty()
  private final String partitioner;

  @JsonProperty()
  private Collection<String> tables;

  public KeyspaceStatus(Cluster cluster) {
    this.clusterName = cluster.getName();
    this.partitioner = cluster.getPartitioner();
  }

  public String getClusterName() {
    return clusterName;
  }

  public String getPartitioner() {
    return partitioner;
  }

  public Collection<String> getTables() {
    return tables;
  }

  public void setTables(Collection<String> tables) {
    this.tables = tables;
  }

}
