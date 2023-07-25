/*
 * Copyright 2018-2018 Stefan Podkowinski
 * Copyright 2019-2019 The Last Pickle Ltd
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

package io.cassandrareaper.service;

import io.cassandrareaper.core.Node;
import io.cassandrareaper.management.DiagnosticProxy;
import io.cassandrareaper.management.ICassandraManagementProxy;
import io.cassandrareaper.resources.view.DiagnosticEvent;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class DiagEventPoller {

  private static final Logger LOG = LoggerFactory.getLogger(DiagEventPoller.class);

  // update summaries should be broadcasted as JMX notifications and we just poll periodically as fallback just in case
  private static final int SUMMARY_POLL_INTERVAL_SECONDS = 60;
  private static final int EVENTS_LIMIT = 10;

  private final Node node;
  private final DiagnosticProxy diagnostics;
  private Collection<String> enabledEvents = Collections.emptySet();

  private final Pattern clazzNameValidator = Pattern.compile("org\\.apache\\.cassandra\\.[a-zA-Z0-9_$.]+");
  private long lastUpdatedAt = 0;
  private final Map<String, Long> lastKeysByEvent = new ConcurrentHashMap<>();
  private ScheduledFuture<?> schedule;
  private final Consumer<DiagnosticEvent> eventConsumer;
  private final ScheduledExecutorService scheduledExecutor;

  DiagEventPoller(
      Node node,
      ICassandraManagementProxy cassandraManagementProxy,
      Consumer<DiagnosticEvent> eventConsumer,
      ScheduledExecutorService scheduledExecutor) {

    this.node = node;
    this.diagnostics = DiagnosticProxy.create(cassandraManagementProxy);
    this.eventConsumer = eventConsumer;
    this.scheduledExecutor = scheduledExecutor;
  }

  void start() {
    if (this.schedule == null || this.schedule.isDone()) {
      scheduledExecutor.scheduleAtFixedRate(this::pollSummary, 0, SUMMARY_POLL_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }
  }

  void stop() {
    schedule.cancel(false);
  }

  void onSummary(Map<String, Comparable> summary) {
    LOG.debug("Received event update summary");
    for (String summaryEntryKey : summary.keySet()) {

      if (summaryEntryKey.equals("last_updated_at")) {
        lastUpdatedAt = (Long) summary.get(summaryEntryKey);
        continue;
      }

      if (!clazzNameValidator.matcher(summaryEntryKey).matches()) {
        LOG.warn("Invalid class name: {}", summaryEntryKey);
        continue;
      }

      // check if any updates happened for the event and schedule polling
      Long lastKeyServer = (Long) summary.get(summaryEntryKey);
      Long lastKeyLocally = lastKeysByEvent.getOrDefault(summaryEntryKey, 0L);

      int iterations = 0;
      while (lastKeyLocally < lastKeyServer && iterations++ < 10) {
        LOG.debug("New events for {} ({} > {})", summaryEntryKey, lastKeyServer, lastKeyLocally);
        // We need to avoid getting stuck in case of any event specific error and make sure that we always move
        // ahead to newer ids. That's why we will deploy some extra sanity checks and reset the last key if something
        // unexpected happens.
        // TODO: report missing events
        try {
          retrieveEvents(summaryEntryKey, lastKeyLocally);
        } catch (RuntimeException e) {
          LOG.error("Error while retrieving events. Resetting last key to " + lastKeyServer, e);
          lastKeysByEvent.put(summaryEntryKey, lastKeyServer);
          break;
        }
        Long newKeyLocally = lastKeysByEvent.get(summaryEntryKey);
        if (newKeyLocally == null || newKeyLocally == 0 || newKeyLocally.equals(lastKeyLocally)) {
          LOG.error("Unexpected new event key: {}. Resetting last key to {}", newKeyLocally, lastKeyServer);
          lastKeysByEvent.put(summaryEntryKey, lastKeyServer);
          break;
        }
        lastKeyLocally = newKeyLocally;
      }
    }
  }

  private void retrieveEvents(String eventClazz, Long startingKey) {
    LOG.debug("Retrieving last {} events since key {}", EVENTS_LIMIT, startingKey);
    SortedMap<Long, Map<String, Serializable>> events = diagnostics.readEvents(eventClazz, startingKey, EVENTS_LIMIT);
    if (events == null || events.isEmpty()) {
      LOG.debug("No {} events for {}", eventClazz, startingKey);
      return;
    }

    LOG.debug("Received {} {} events for {}", events.size(), eventClazz, startingKey);
    List<DiagnosticEvent> diagEvents = new ArrayList<>(events.size());
    for (Map.Entry<Long, Map<String, Serializable>> event : events.entrySet()) {
      lastKeysByEvent.put(eventClazz, event.getKey());
      Map<String, Serializable> eventPayload = event.getValue();
      String evClazz = (String) eventPayload.get("class");
      String evType = (String) eventPayload.get("type");
      Long evTs = (Long) eventPayload.get("ts");

      diagEvents.add(
          new DiagnosticEvent(node.getClusterName(), node.getHostname(), evClazz, evType, evTs, eventPayload));
    }

    diagEvents.forEach(eventConsumer);
  }

  private void pollSummary() {
    LOG.debug("Polling event update summary");
    Map<String, Comparable> summary = diagnostics.getLastEventIdsIfModified(lastUpdatedAt);
    if (summary == null) {
      LOG.debug("No summary updates since {}", lastUpdatedAt);
    } else {
      onSummary(summary);
    }
  }

  Collection<String> getEnabledEvents() {
    return enabledEvents;
  }

  void setEnabledEvents(Collection<String> enabledEvents) {
    this.enabledEvents = Collections.unmodifiableCollection(enabledEvents);
  }
}