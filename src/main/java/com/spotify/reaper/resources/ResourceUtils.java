package com.spotify.reaper.resources;

import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.cassandra.JmxConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 * Generic utilities for actions within the resources.
 * Handles JMX and Storage layer access.
 */
public class ResourceUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceUtils.class);

  public static Cluster createClusterWithSeedHost(String seedHost, JmxConnectionFactory factory)
      throws ReaperException {
    String clusterName;
    String partitioner;
    try {
      JmxProxy jmxProxy = factory.create(seedHost);
      clusterName = jmxProxy.getClusterName();
      partitioner = jmxProxy.getPartitioner();
      jmxProxy.close();
    } catch (ReaperException e) {
      LOG.error("failed to create cluster with seed host: " + seedHost);
      e.printStackTrace();
      throw e;
    }
    return new Cluster(clusterName, partitioner, Collections.singleton(seedHost));
  }

}
