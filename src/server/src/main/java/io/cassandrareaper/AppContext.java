/*
 * Copyright 2015-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
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

package io.cassandrareaper;

import io.cassandrareaper.management.IManagementConnectionFactory;
import io.cassandrareaper.service.RepairManager;
import io.cassandrareaper.service.SchedulingManager;
import io.cassandrareaper.storage.IStorageDao;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single class to hold all application global interfacing objects, and app global options.
 */
public final class AppContext {

  public static final String REAPER_INSTANCE_ADDRESS = Private.initialiseInstanceAddress();

  public final UUID reaperInstanceId = UUIDs.timeBased();
  public final AtomicBoolean isRunning = new AtomicBoolean(true);
  public final AtomicBoolean isDistributed = new AtomicBoolean(false);
  public IStorageDao storage;
  public RepairManager repairManager;
  public SchedulingManager schedulingManager;
  public IManagementConnectionFactory managementConnectionFactory;
  public ReaperApplicationConfiguration config;
  public MetricRegistry metricRegistry = new MetricRegistry();
  volatile String localNodeAddress = null;

  public String getLocalNodeAddress() {
    Preconditions.checkState(config.isInSidecarMode());
    return localNodeAddress;
  }

  private static final class Private {
    private static final Logger LOG = LoggerFactory.getLogger(AppContext.class);
    private static final String DEFAULT_INSTANCE_ADDRESS = "127.0.0.1";

    private static String initialiseInstanceAddress() {
      try {
        return InetAddress.getLocalHost().getHostAddress();
      } catch (UnknownHostException e) {
        LOG.warn("Cannot get instance address", e);
      }
      return DEFAULT_INSTANCE_ADDRESS;
    }
  }
}