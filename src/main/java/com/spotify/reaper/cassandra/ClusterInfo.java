package com.spotify.reaper.cassandra;

import com.spotify.reaper.ReaperException;

import java.util.List;

public class ClusterInfo implements IClusterInfo {

  private String seedHost;
  private int seedPort = 0;

  private List<String> tokens;

  public ClusterInfo(String seedHost, int seedPort) {
    this.seedHost = seedHost;
    this.seedPort = seedPort;
  }

  public void init() throws ReaperException {
    JMXProxy jmx = seedPort == 0 ? JMXProxy.connect(seedHost) : JMXProxy.connect(seedHost, seedPort);
    tokens = jmx.getTokens();
    jmx.close();
  }

  @Override
  public List<String> getTokens() {
    return tokens;
  }

}
