package com.spotify.reaper.cassandra;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JmxProxyTest {

  @Test
  public void testGetSymbolicName() {
    assertEquals("example2cluster", JmxProxy.toSymbolicName("Example2 Cluster"));
    assertEquals("example2_cluster", JmxProxy.toSymbolicName("Example2_Cluster"));
  }
}
