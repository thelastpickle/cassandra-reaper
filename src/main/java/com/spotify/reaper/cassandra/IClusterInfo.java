package com.spotify.reaper.cassandra;

import java.util.List;

public interface IClusterInfo {
  List<String> getTokens();
  String getClusterName();
  String getSymbolicName();
  String getPartitionerName();
}
