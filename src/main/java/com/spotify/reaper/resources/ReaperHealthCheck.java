package com.spotify.reaper.resources;

import com.spotify.reaper.storage.IStorage;

/**
 * Provides an endpoint to check the health of the running Reaper instance.
 */
public class ReaperHealthCheck extends com.codahale.metrics.health.HealthCheck {

  private IStorage storage;

  public ReaperHealthCheck(IStorage storage) {
    this.storage = storage;
  }

  @Override
  protected Result check() throws Exception {
    // Should check some other pre-conditions here for a healthy Reaper instance?
    if (storage.isStorageConnected()) {
      return Result.healthy();
    }
    return Result.unhealthy("storage not connected");
  }
}
