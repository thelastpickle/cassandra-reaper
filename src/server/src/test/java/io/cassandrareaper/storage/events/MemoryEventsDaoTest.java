/*
 * Copyright 2024 The Last Pickle Ltd
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

package io.cassandrareaper.storage.events;

import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.DiagEventSubscription;
import io.cassandrareaper.storage.MemoryStorageFacade;

import java.util.Collection;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class MemoryEventsDaoTest {

  private MemoryStorageFacade storage;
  private String clusterName;

  @Before
  public void setUp() {
    storage = new MemoryStorageFacade();
    clusterName = "test_cluster";

    // Create cluster
    Cluster cluster =
        Cluster.builder()
            .withName(clusterName)
            .withPartitioner("org.apache.cassandra.dht.Murmur3Partitioner")
            .withSeedHosts(ImmutableSet.of("127.0.0.1"))
            .withState(Cluster.State.ACTIVE)
            .build();
    storage.getClusterDao().addCluster(cluster);
  }

  @After
  public void tearDown() {
    if (storage != null) {
      storage.clearDatabase();
      storage.stop();
    }
  }

  @Test
  public void testAddAndRetrieveEventSubscription() {
    DiagEventSubscription subscription =
        new DiagEventSubscription(
            Optional.empty(),
            clusterName,
            Optional.of("Test subscription"),
            Sets.newHashSet("127.0.0.1", "127.0.0.2"),
            Sets.newHashSet("event1", "event2"),
            true,
            null,
            "http://example.com/endpoint");

    DiagEventSubscription added = storage.getEventsDao().addEventSubscription(subscription);
    assertNotNull(added);
    assertTrue(added.getId().isPresent());

    // Retrieve and verify
    DiagEventSubscription retrieved =
        storage.getEventsDao().getEventSubscription(added.getId().get());
    assertNotNull(retrieved);
    assertEquals(clusterName, retrieved.getCluster());
    assertEquals("Test subscription", retrieved.getDescription());
    assertEquals(2, retrieved.getNodes().size());
    assertTrue(retrieved.getNodes().contains("127.0.0.1"));
    assertEquals(2, retrieved.getEvents().size());
    assertTrue(retrieved.getEvents().contains("event1"));
    assertTrue(retrieved.getExportSse());
    assertNull(retrieved.getExportFileLogger());
    assertEquals("http://example.com/endpoint", retrieved.getExportHttpEndpoint());
  }

  @Test
  public void testDeleteEventSubscription() {
    DiagEventSubscription subscription =
        new DiagEventSubscription(
            Optional.empty(),
            clusterName,
            Optional.of("Subscription to delete"),
            Sets.newHashSet("127.0.0.1"),
            Sets.newHashSet("event1"),
            true,
            null,
            null);

    DiagEventSubscription added = storage.getEventsDao().addEventSubscription(subscription);

    // Delete subscription
    boolean deleted = storage.getEventsDao().deleteEventSubscription(added.getId().get());
    assertTrue(deleted);

    // Verify it's deleted
    DiagEventSubscription retrieved =
        storage.getEventsDao().getEventSubscription(added.getId().get());
    assertNull(retrieved);
  }

  @Test
  public void testGetEventSubscriptions() {
    // Create multiple subscriptions
    for (int i = 0; i < 5; i++) {
      DiagEventSubscription subscription =
          new DiagEventSubscription(
              Optional.empty(),
              clusterName,
              Optional.of("Subscription " + i),
              Sets.newHashSet("127.0.0." + (i + 1)),
              Sets.newHashSet("event_" + i),
              true,
              null,
              null);

      storage.getEventsDao().addEventSubscription(subscription);
    }

    // Get all subscriptions
    Collection<DiagEventSubscription> allSubscriptions =
        storage.getEventsDao().getEventSubscriptions();
    assertEquals(5, allSubscriptions.size());

    // Get subscriptions for cluster
    Collection<DiagEventSubscription> clusterSubscriptions =
        storage.getEventsDao().getEventSubscriptions(clusterName);
    assertEquals(5, clusterSubscriptions.size());
  }

  @Test
  public void testGetEventSubscriptionsForDifferentClusters() {
    String cluster2Name = "cluster2";
    Cluster cluster2 =
        Cluster.builder()
            .withName(cluster2Name)
            .withPartitioner("org.apache.cassandra.dht.Murmur3Partitioner")
            .withSeedHosts(ImmutableSet.of("127.0.0.2"))
            .withState(Cluster.State.ACTIVE)
            .build();
    storage.getClusterDao().addCluster(cluster2);

    // Create subscriptions for different clusters
    DiagEventSubscription subscription1 =
        new DiagEventSubscription(
            Optional.empty(),
            clusterName,
            Optional.of("Cluster 1 subscription"),
            Sets.newHashSet("127.0.0.1"),
            Sets.newHashSet("event1"),
            true,
            null,
            null);
    storage.getEventsDao().addEventSubscription(subscription1);

    DiagEventSubscription subscription2 =
        new DiagEventSubscription(
            Optional.empty(),
            cluster2Name,
            Optional.of("Cluster 2 subscription"),
            Sets.newHashSet("127.0.0.2"),
            Sets.newHashSet("event2"),
            true,
            null,
            null);
    storage.getEventsDao().addEventSubscription(subscription2);

    // Verify subscriptions are separated by cluster
    Collection<DiagEventSubscription> cluster1Subs =
        storage.getEventsDao().getEventSubscriptions(clusterName);
    assertEquals(1, cluster1Subs.size());

    Collection<DiagEventSubscription> cluster2Subs =
        storage.getEventsDao().getEventSubscriptions(cluster2Name);
    assertEquals(1, cluster2Subs.size());
  }

  @Test
  public void testEventSubscriptionWithNoHttpEndpoint() {
    DiagEventSubscription subscription =
        new DiagEventSubscription(
            Optional.empty(),
            clusterName,
            Optional.of("No HTTP endpoint"),
            Sets.newHashSet("127.0.0.1"),
            Sets.newHashSet("event1"),
            true,
            null,
            null);

    DiagEventSubscription added = storage.getEventsDao().addEventSubscription(subscription);

    // Retrieve and verify
    DiagEventSubscription retrieved =
        storage.getEventsDao().getEventSubscription(added.getId().get());
    assertNotNull(retrieved);
    assertNull(retrieved.getExportHttpEndpoint());
  }

  @Test
  public void testEventSubscriptionWithMultipleExportMethods() {
    DiagEventSubscription subscription =
        new DiagEventSubscription(
            Optional.empty(),
            clusterName,
            Optional.of("Multiple export methods"),
            Sets.newHashSet("127.0.0.1"),
            Sets.newHashSet("event1"),
            true,
            "file.log",
            "http://example.com/events");

    DiagEventSubscription added = storage.getEventsDao().addEventSubscription(subscription);

    // Retrieve and verify all export methods
    DiagEventSubscription retrieved =
        storage.getEventsDao().getEventSubscription(added.getId().get());
    assertNotNull(retrieved);
    assertTrue(retrieved.getExportSse());
    assertNotNull(retrieved.getExportFileLogger());
    assertNotNull(retrieved.getExportHttpEndpoint());
  }

  @Test
  public void testEventSubscriptionWithMultipleNodes() {
    DiagEventSubscription subscription =
        new DiagEventSubscription(
            Optional.empty(),
            clusterName,
            Optional.of("Multiple nodes"),
            Sets.newHashSet("node1", "node2", "node3", "node4", "node5"),
            Sets.newHashSet("event1"),
            true,
            null,
            null);

    DiagEventSubscription added = storage.getEventsDao().addEventSubscription(subscription);

    // Retrieve and verify
    DiagEventSubscription retrieved =
        storage.getEventsDao().getEventSubscription(added.getId().get());
    assertNotNull(retrieved);
    assertEquals(5, retrieved.getNodes().size());
    assertTrue(retrieved.getNodes().contains("node1"));
    assertTrue(retrieved.getNodes().contains("node5"));
  }

  @Test
  public void testEventSubscriptionWithMultipleEvents() {
    DiagEventSubscription subscription =
        new DiagEventSubscription(
            Optional.empty(),
            clusterName,
            Optional.of("Multiple events"),
            Sets.newHashSet("127.0.0.1"),
            Sets.newHashSet("compaction", "flush", "repair", "hint", "batch"),
            true,
            null,
            null);

    DiagEventSubscription added = storage.getEventsDao().addEventSubscription(subscription);

    // Retrieve and verify
    DiagEventSubscription retrieved =
        storage.getEventsDao().getEventSubscription(added.getId().get());
    assertNotNull(retrieved);
    assertEquals(5, retrieved.getEvents().size());
    assertTrue(retrieved.getEvents().contains("compaction"));
    assertTrue(retrieved.getEvents().contains("repair"));
  }

  @Test
  public void testEventSubscriptionWithSpecialCharacters() {
    DiagEventSubscription subscription =
        new DiagEventSubscription(
            Optional.empty(),
            clusterName,
            Optional.of("Special chars: !@#$%^&*()"),
            Sets.newHashSet("node-with-dashes", "node_with_underscores"),
            Sets.newHashSet("event:type", "event.name"),
            true,
            null,
            "http://example.com/endpoint?param=value&other=123");

    DiagEventSubscription added = storage.getEventsDao().addEventSubscription(subscription);

    // Retrieve and verify special characters are preserved
    DiagEventSubscription retrieved =
        storage.getEventsDao().getEventSubscription(added.getId().get());
    assertNotNull(retrieved);
    assertEquals("Special chars: !@#$%^&*()", retrieved.getDescription());
    assertTrue(retrieved.getExportHttpEndpoint().contains("?param=value&other=123"));
  }

  @Test
  public void testDeleteAllEventSubscriptionsForCluster() {
    // Create multiple subscriptions for the cluster
    for (int i = 0; i < 3; i++) {
      DiagEventSubscription subscription =
          new DiagEventSubscription(
              Optional.empty(),
              clusterName,
              Optional.of("Subscription " + i),
              Sets.newHashSet("127.0.0." + (i + 1)),
              Sets.newHashSet("event_" + i),
              true,
              null,
              null);

      storage.getEventsDao().addEventSubscription(subscription);
    }

    assertEquals(3, storage.getEventsDao().getEventSubscriptions(clusterName).size());

    // Delete all subscriptions for the cluster
    Collection<DiagEventSubscription> subscriptions =
        storage.getEventsDao().getEventSubscriptions(clusterName);
    for (DiagEventSubscription sub : subscriptions) {
      storage.getEventsDao().deleteEventSubscription(sub.getId().get());
    }

    assertEquals(0, storage.getEventsDao().getEventSubscriptions(clusterName).size());
  }
}
