/*
 * Copyright 2018-2019 The Last Pickle Ltd
 *
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

package io.cassandrareaper.jmx;

import io.cassandrareaper.service.DiagEventSubscriptionService;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.SortedMap;
import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.NotificationListener;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class DiagnosticProxy {

  private static final Logger LOG = LoggerFactory.getLogger(DiagEventSubscriptionService.class);

  private final JmxProxyImpl proxy;

  private DiagnosticProxy(JmxProxyImpl proxy) {
    this.proxy = proxy;
  }

  public static DiagnosticProxy create(JmxProxy proxy) {
    Preconditions.checkArgument(proxy instanceof JmxProxyImpl, "only JmxProxyImpl is supported");
    return new DiagnosticProxy((JmxProxyImpl)proxy);
  }

  public void enableEventPersistence(String eventClazz) {
    proxy.getDiagnosticEventPersistenceMBean().enableEventPersistence(eventClazz);
  }

  public void disableEventPersistence(String eventClazz) {
    proxy.getDiagnosticEventPersistenceMBean().disableEventPersistence(eventClazz);
  }

  public Map<String, Comparable> getLastEventIdsIfModified(long lastUpdated) {
    return proxy.getLastEventIdBroadcasterMBean().getLastEventIdsIfModified(lastUpdated);
  }

  public SortedMap<Long, Map<String, Serializable>> readEvents(String eventClazz, Long lastKey, int limit) {
    return proxy.getDiagnosticEventPersistenceMBean().readEvents(eventClazz, lastKey, limit);
  }

  public void subscribeNotifications(NotificationListener listener) {
    try {
      LOG.debug("Subscribing to notifications on {} ({})", proxy.getHost(), proxy.getClusterName());
      proxy.addConnectionNotificationListener(listener);
      proxy.addNotificationListener(listener, null);
    } catch (InstanceNotFoundException ex) {
      LOG.error(
          String.format(
              "Failed to subscribe on %s (%s): incompatible Cassandra version (>=4.0 required)",
              proxy.getHost(), proxy.getClusterName()),
          ex);

    } catch (IOException | JMException | RuntimeException e) {
      LOG.error(String.format("Failed to subscribe on %s (%s)", proxy.getHost(), proxy.getClusterName()), e);
    }
  }

  public void unsubscribeNotifications(NotificationListener listener) {
    try {
      LOG.debug("Unsubscribing from notifications on {} ({})", proxy.getHost(), proxy.getClusterName());
      proxy.removeConnectionNotificationListener(listener);
      proxy.removeNotificationListener(listener);
    } catch (IOException | JMException | RuntimeException e) {
      LOG.error(String.format("Failed to unsubscribe on %s (%s)", proxy.getHost(), proxy.getClusterName()), e);
    }
  }

}
