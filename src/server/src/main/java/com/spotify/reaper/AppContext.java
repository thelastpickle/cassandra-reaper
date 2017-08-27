package com.spotify.reaper;

import com.codahale.metrics.MetricRegistry;
import com.spotify.reaper.cassandra.JmxConnectionFactory;
import com.spotify.reaper.service.RepairManager;
import com.spotify.reaper.storage.IStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Single class to hold all application global interfacing objects,
 * and app global options.
 */
public final class AppContext {

  public static final UUID REAPER_INSTANCE_ID = UUID.randomUUID();
  private static final String DEFAULT_INSTANCE_ADDRESS = "127.0.0.1";
  private static final Logger LOG = LoggerFactory.getLogger(AppContext.class);

  public static final String REAPER_INSTANCE_ADDRESS = initialiseInstanceAddress();
  public final AtomicBoolean isRunning = new AtomicBoolean(true);
  public IStorage storage;
  public RepairManager repairManager;
  public JmxConnectionFactory jmxConnectionFactory;
  public ReaperApplicationConfiguration config;
  public MetricRegistry metricRegistry = new MetricRegistry();

  private static String initialiseInstanceAddress() {
    String reaperInstanceAddress;
    try {
      reaperInstanceAddress = InetAddress.getLocalHost().getHostAddress();
    } catch(UnknownHostException e) {
      LOG.warn("Cannot get instance address", e);
     reaperInstanceAddress = DEFAULT_INSTANCE_ADDRESS;
    }
    return reaperInstanceAddress;
  }
}
