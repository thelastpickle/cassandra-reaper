package com.spotify.reaper.cassandra;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ClusterInfoTest {
  @Test
  public void testGetSymbolicName() {
    assertEquals("example2cluster", ClusterInfo.toSymbolicName("Example2 Cluster"));
    assertEquals("example2_cluster", ClusterInfo.toSymbolicName("Example2_Cluster"));
  }
}
