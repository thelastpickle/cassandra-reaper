/*
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

import io.cassandrareaper.jmx.JmxConnectionFactory;
import io.cassandrareaper.service.MetricsGrabber;
import io.cassandrareaper.service.PurgeManager;
import io.cassandrareaper.service.RepairManager;
import io.cassandrareaper.service.SnapshotManager;
import io.cassandrareaper.storage.IStorage;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single class to hold all application global interfacing objects, and app global options.
 */
public final class AppContext {

  public static final UUID REAPER_INSTANCE_ID = UUID.randomUUID();
  public static final String REAPER_INSTANCE_ADDRESS = initialiseInstanceAddress();

  private static final String DEFAULT_INSTANCE_ADDRESS = "127.0.0.1";
  private static final Logger LOG = LoggerFactory.getLogger(AppContext.class);

  public final AtomicBoolean isRunning = new AtomicBoolean(true);
  public IStorage storage;
  public RepairManager repairManager;
  public JmxConnectionFactory jmxConnectionFactory;
  public ReaperApplicationConfiguration config;
  public MetricRegistry metricRegistry = new MetricRegistry();
  public SnapshotManager snapshotManager;
  public PurgeManager purgeManager;
  public MetricsGrabber metricsGrabber;

  private static String initialiseInstanceAddress() {
    String reaperInstanceAddress;
    try {
      reaperInstanceAddress = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      LOG.warn("Cannot get instance address", e);
      reaperInstanceAddress = DEFAULT_INSTANCE_ADDRESS;
    }
    return reaperInstanceAddress;
  }
}
