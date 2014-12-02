package com.spotify.reaper.cassandra;

import java.util.List;

public interface IClusterInfo {

  public List<String> getTokens();

  public String getClusterName();
}
