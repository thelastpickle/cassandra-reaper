package com.spotify.reaper.core;

public enum DatacenterAvailability {
  /* We require direct JMX access to all nodes across all datacenters */
  ALL,

  /* We require jmx access to all nodes in the local datacenter */
  LOCAL,

  /* Each datacenter requires at minimum one reaper instance that has jmx access to all nodes in that datacenter */
  EACH
}
