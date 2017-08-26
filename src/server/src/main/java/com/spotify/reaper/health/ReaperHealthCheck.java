package com.spotify.reaper.health;

import com.codahale.metrics.health.HealthCheck;
import com.spotify.reaper.AppContext;

/**
 * Provides an endpoint to check the health of the running Reaper instance.
 */
public class ReaperHealthCheck extends HealthCheck {

  private AppContext context;

  public ReaperHealthCheck(AppContext context) {
    this.context = context;
  }

  @Override
  protected Result check() throws Exception {
    // Should check some other pre-conditions here for a healthy Reaper instance?
    if (context.storage.isStorageConnected()) {
      return Result.healthy();
    }
    return Result.unhealthy("storage not connected");
  }
}
