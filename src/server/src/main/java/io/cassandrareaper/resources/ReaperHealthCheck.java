/*
 * Copyright 2014-2017 Spotify AB
 * Copyright 2016-2018 The Last Pickle Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.cassandrareaper.resources;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperException;

import java.util.concurrent.TimeUnit;

/**
 * Provides an endpoint to check the health of the running Reaper instance.
 */
public final class ReaperHealthCheck extends com.codahale.metrics.health.HealthCheck {

  private static final long HEALTH_CHECK_INTERVAL
      = TimeUnit.SECONDS.toMillis(Long.getLong("ReaperHealthCheck.interval.seconds", 5));

  private final AppContext context;
  private volatile long nextCheck = 0;
  private volatile Result lastResult;

  public ReaperHealthCheck(AppContext context) {
    this.context = context;
  }

  @Override
  protected Result check() throws ReaperException {
    if (context.config.isInKubernetesSidecarMode()) {
      if (context.isRunning.get()) {
        return Result.healthy();
      } else {
        return Result.unhealthy("Reaper is not running");
      }
    }

    if (context.storage.isStorageConnected()) {
      if (System.currentTimeMillis() > nextCheck) {
        nextCheck = System.currentTimeMillis() + HEALTH_CHECK_INTERVAL;
        try {
          context.storage.getClusters();
          context.storage.getAllRepairSchedules();
          lastResult = Result.healthy();
        } catch (RuntimeException ex) {
          lastResult = Result.unhealthy(ex);
        }
      }
      return lastResult;
    }
    return Result.unhealthy("storage not connected");
  }
}
