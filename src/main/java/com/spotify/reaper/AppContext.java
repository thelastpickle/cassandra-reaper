package com.spotify.reaper;

import com.spotify.reaper.cassandra.JmxConnectionFactory;
import com.spotify.reaper.service.RepairManager;
import com.spotify.reaper.storage.IStorage;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Single class to hold all application global interfacing objects,
 * and app global options.
 */
public final class AppContext {

  public final AtomicBoolean isRunning = new AtomicBoolean(true);
  public IStorage storage;
  public RepairManager repairManager;
  public JmxConnectionFactory jmxConnectionFactory;
  public ReaperApplicationConfiguration config;
}
