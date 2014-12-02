package com.spotify.reaper.cassandra;

import com.spotify.reaper.ReaperException;

import java.util.List;

public class ClusterInfo implements IClusterInfo {

  private String seedHost;
  private int seedPort = 0;
  private String clusterName;
  private String partitionerName;

  private List<String> tokens;

  public static ClusterInfo getInstance(String seedHost) throws ReaperException {
    return new ClusterInfo(seedHost, 0).init();
  }

  public ClusterInfo(String seedHost, int seedPort) {
    this.seedHost = seedHost;
    this.seedPort = seedPort;
  }

  public ClusterInfo init() throws ReaperException {
    JMXProxy jmx =
        seedPort == 0 ? JMXProxy.connect(seedHost) : JMXProxy.connect(seedHost, seedPort);
    tokens = jmx.getTokens();
    clusterName = jmx.getClusterName();
    partitionerName = jmx.getPartitionerName();
    jmx.close();
    return this;
  }

  @Override
  public List<String> getTokens() {
    return tokens;
  }

  @Override
  public String getClusterName() {
    return clusterName;
  }

  public static String toSymbolicName(String s) {
    return s.toLowerCase().replaceAll("[^a-z0-9_]", "");
  }

  @Override
  public String getSymbolicName() {
    return toSymbolicName(clusterName);
  }

  @Override
  public String getPartitionerName() {
    return partitionerName;
  }

}
