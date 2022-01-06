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

package io.cassandrareaper.acceptance;

import io.cassandrareaper.core.DiagEventSubscription;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.ws.rs.sse.InboundSseEvent;
import javax.ws.rs.sse.SseEventSource;

import com.google.common.collect.Lists;

/**
 * Helper class for holding acceptance test scenario state.
 * Contains also methods for getting related resources for testing, like mocks etc.
 */
public final class TestContext {

  public static String TEST_USER = "test_user";
  public static String SEED_HOST;
  public static String TEST_CLUSTER;
  public static UUID FINISHED_SEGMENT;

  /* Testing cluster seed host mapped to cluster name. */
  public static Map<String, String> TEST_CLUSTER_SEED_HOSTS = new HashMap<>();

  /* Testing cluster name mapped to keyspace name mapped to tables list. */
  public static Map<String, Map<String, Set<String>>> TEST_CLUSTER_INFO = new HashMap<>();

  /* Diagnostic events storage */
  public static SseEventSource sseEventSource;
  public static List<InboundSseEvent> diagnosticEvents = new ArrayList<>();

  /* Used for targeting an object accessed in last test step. */
  private final List<UUID> currentSchedules = Lists.newCopyOnWriteArrayList();
  private final List<UUID> currentRepairs = Lists.newCopyOnWriteArrayList();
  private final List<DiagEventSubscription> currentEventSubscriptions = Lists.newCopyOnWriteArrayList();
  private final List<DiagEventSubscription> retrievedEventSubscriptions = Lists.newCopyOnWriteArrayList();
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  TestContext() {}

  /**
   * Adds testing cluster information for testing purposes.
   * Used to create mocks and prepare testing access for added testing clusters.
   */
  public static void addClusterInfo(String clusterName, String keyspace, Set<String> tables) {
    if (!TEST_CLUSTER_INFO.containsKey(clusterName)) {
      TEST_CLUSTER_INFO.put(clusterName, new HashMap<>());
    }
    TEST_CLUSTER_INFO.get(clusterName).put(keyspace, tables);
  }

  public static void addSeedHostToClusterMapping(String seedHost, String clusterName) {
    TEST_CLUSTER_SEED_HOSTS.put(seedHost, clusterName);
  }

  void addCurrentScheduleId(UUID id) {
    lock.writeLock().lock();
    try {
      if (currentSchedules.isEmpty() || !getCurrentScheduleId().equals(id)) {
        currentSchedules.add(id);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  void addCurrentRepairId(UUID id) {
    lock.writeLock().lock();
    try {
      if (currentRepairs.isEmpty() || !getCurrentRepairId().equals(id)) {
        currentRepairs.add(id);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  void addCurrentEventSubscription(DiagEventSubscription eventSubscription) {
    lock.writeLock().lock();
    try {
      if (currentEventSubscriptions.isEmpty() || !getCurrentEventSubscription().equals(eventSubscription)) {
        currentEventSubscriptions.add(eventSubscription);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  void updateRetrievedEventSubscriptions(List<DiagEventSubscription> eventSubscriptions) {
    lock.writeLock().lock();
    try {
      retrievedEventSubscriptions.clear();
      retrievedEventSubscriptions.addAll(eventSubscriptions);
    } finally {
      lock.writeLock().unlock();
    }
  }

  UUID getCurrentScheduleId() {
    lock.readLock().lock();
    try {
      return currentSchedules.get(currentSchedules.size() - 1);
    } finally {
      lock.readLock().unlock();
    }
  }

  UUID getCurrentRepairId() {
    lock.readLock().lock();
    try {
      return currentRepairs.get(currentRepairs.size() - 1);
    } finally {
      lock.readLock().unlock();
    }
  }

  DiagEventSubscription getCurrentEventSubscription() {
    lock.readLock().lock();
    try {
      return currentEventSubscriptions.get(currentEventSubscriptions.size() - 1);
    } finally {
      lock.readLock().unlock();
    }
  }

  DiagEventSubscription removeCurrentEventSubscription() {
    lock.readLock().lock();
    try {
      return currentEventSubscriptions.remove(currentEventSubscriptions.size() - 1);
    } finally {
      lock.readLock().unlock();
    }
  }

  List<UUID> getCurrentRepairIds() {
    lock.readLock().lock();
    try {
      return Collections.unmodifiableList(currentRepairs);
    } finally {
      lock.readLock().unlock();
    }
  }

  List<DiagEventSubscription> getRetrievedEventSubscriptions() {
    lock.readLock().lock();
    try {
      return Collections.unmodifiableList(retrievedEventSubscriptions);
    } finally {
      lock.readLock().unlock();
    }
  }
}
