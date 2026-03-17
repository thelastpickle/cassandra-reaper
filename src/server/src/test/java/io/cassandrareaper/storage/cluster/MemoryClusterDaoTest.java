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

package io.cassandrareaper.storage.cluster;

import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.JmxCredentials;
import io.cassandrareaper.storage.MemoryStorageFacade;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class MemoryClusterDaoTest {

  private MemoryStorageFacade storage;

  @Before
  public void setUp() {
    storage = new MemoryStorageFacade();
  }

  @After
  public void tearDown() {
    if (storage != null) {
      storage.clearDatabase();
      storage.stop();
    }
  }

  @Test
  public void testAddClusterWithJmxCredentials() {
    String clusterName = "test_cluster";
    String username = "jmx_user";
    String password = "jmx_password";

    JmxCredentials credentials =
        JmxCredentials.builder().withUsername(username).withPassword(password).build();

    Cluster cluster =
        Cluster.builder()
            .withName(clusterName)
            .withPartitioner("org.apache.cassandra.dht.Murmur3Partitioner")
            .withSeedHosts(ImmutableSet.of("127.0.0.1"))
            .withState(Cluster.State.ACTIVE)
            .withJmxCredentials(credentials)
            .build();

    boolean added = storage.getClusterDao().addCluster(cluster);
    assertTrue("Cluster should be added successfully", added);

    // Retrieve and verify
    Cluster retrieved = storage.getClusterDao().getCluster(clusterName);
    assertNotNull("Retrieved cluster should not be null", retrieved);
    assertTrue("Cluster should have JMX credentials", retrieved.getJmxCredentials().isPresent());

    JmxCredentials retrievedCreds = retrieved.getJmxCredentials().get();
    assertEquals("Username should match", username, retrievedCreds.getUsername());
    assertEquals("Password should match", password, retrievedCreds.getPassword());
  }

  @Test
  public void testAddClusterWithoutJmxCredentials() {
    String clusterName = "test_cluster_no_creds";

    Cluster cluster =
        Cluster.builder()
            .withName(clusterName)
            .withPartitioner("org.apache.cassandra.dht.Murmur3Partitioner")
            .withSeedHosts(ImmutableSet.of("127.0.0.1"))
            .withState(Cluster.State.ACTIVE)
            .build();

    boolean added = storage.getClusterDao().addCluster(cluster);
    assertTrue("Cluster should be added successfully", added);

    // Retrieve and verify
    Cluster retrieved = storage.getClusterDao().getCluster(clusterName);
    assertNotNull("Retrieved cluster should not be null", retrieved);
    assertFalse(
        "Cluster should not have JMX credentials", retrieved.getJmxCredentials().isPresent());
  }

  @Test
  public void testUpdateClusterJmxCredentials() {
    String clusterName = "test_cluster_update";

    // Create cluster without credentials
    Cluster cluster =
        Cluster.builder()
            .withName(clusterName)
            .withPartitioner("org.apache.cassandra.dht.Murmur3Partitioner")
            .withSeedHosts(ImmutableSet.of("127.0.0.1"))
            .withState(Cluster.State.ACTIVE)
            .build();

    storage.getClusterDao().addCluster(cluster);

    // Update with credentials
    JmxCredentials newCredentials =
        JmxCredentials.builder().withUsername("new_user").withPassword("new_password").build();

    Cluster updatedCluster =
        Cluster.builder()
            .withName(clusterName)
            .withPartitioner("org.apache.cassandra.dht.Murmur3Partitioner")
            .withSeedHosts(ImmutableSet.of("127.0.0.1"))
            .withState(Cluster.State.ACTIVE)
            .withJmxCredentials(newCredentials)
            .build();

    boolean updated = storage.getClusterDao().updateCluster(updatedCluster);
    assertTrue("Cluster should be updated successfully", updated);

    // Retrieve and verify
    Cluster retrieved = storage.getClusterDao().getCluster(clusterName);
    assertTrue(
        "Updated cluster should have JMX credentials", retrieved.getJmxCredentials().isPresent());
    assertEquals("new_user", retrieved.getJmxCredentials().get().getUsername());
    assertEquals("new_password", retrieved.getJmxCredentials().get().getPassword());
  }

  @Test
  public void testJmxCredentialsWithSpecialCharacters() {
    String clusterName = "test_cluster_special";
    String username = "user@domain.com";
    String password = "p@ssw0rd!#$%^&*()";

    JmxCredentials credentials =
        JmxCredentials.builder().withUsername(username).withPassword(password).build();

    Cluster cluster =
        Cluster.builder()
            .withName(clusterName)
            .withPartitioner("org.apache.cassandra.dht.Murmur3Partitioner")
            .withSeedHosts(ImmutableSet.of("127.0.0.1"))
            .withState(Cluster.State.ACTIVE)
            .withJmxCredentials(credentials)
            .build();

    storage.getClusterDao().addCluster(cluster);

    // Retrieve and verify special characters are preserved
    Cluster retrieved = storage.getClusterDao().getCluster(clusterName);
    assertTrue(retrieved.getJmxCredentials().isPresent());
    assertEquals(username, retrieved.getJmxCredentials().get().getUsername());
    assertEquals(password, retrieved.getJmxCredentials().get().getPassword());
  }

  @Test
  public void testGetAllClustersWithMixedCredentials() {
    // Create clusters with and without credentials
    Cluster cluster1 =
        Cluster.builder()
            .withName("cluster_with_creds")
            .withPartitioner("org.apache.cassandra.dht.Murmur3Partitioner")
            .withSeedHosts(ImmutableSet.of("127.0.0.1"))
            .withState(Cluster.State.ACTIVE)
            .withJmxCredentials(
                JmxCredentials.builder().withUsername("user1").withPassword("pass1").build())
            .build();

    Cluster cluster2 =
        Cluster.builder()
            .withName("cluster_without_creds")
            .withPartitioner("org.apache.cassandra.dht.Murmur3Partitioner")
            .withSeedHosts(ImmutableSet.of("127.0.0.2"))
            .withState(Cluster.State.ACTIVE)
            .build();

    storage.getClusterDao().addCluster(cluster1);
    storage.getClusterDao().addCluster(cluster2);

    // Verify both are returned correctly
    assertEquals(2, storage.getClusterDao().getClusters().size());

    Cluster retrieved1 = storage.getClusterDao().getCluster("cluster_with_creds");
    assertTrue(retrieved1.getJmxCredentials().isPresent());

    Cluster retrieved2 = storage.getClusterDao().getCluster("cluster_without_creds");
    assertFalse(retrieved2.getJmxCredentials().isPresent());
  }

  @Test
  public void testDeleteClusterWithJmxCredentials() {
    String clusterName = "cluster_to_delete";

    Cluster cluster =
        Cluster.builder()
            .withName(clusterName)
            .withPartitioner("org.apache.cassandra.dht.Murmur3Partitioner")
            .withSeedHosts(ImmutableSet.of("127.0.0.1"))
            .withState(Cluster.State.ACTIVE)
            .withJmxCredentials(
                JmxCredentials.builder().withUsername("user").withPassword("pass").build())
            .build();

    storage.getClusterDao().addCluster(cluster);
    assertNotNull(storage.getClusterDao().getCluster(clusterName));

    // Delete cluster
    Cluster deleted = storage.getClusterDao().deleteCluster(clusterName);
    assertNotNull("Cluster should be deleted successfully", deleted);

    // Verify it's gone
    Cluster retrieved = storage.getClusterDao().getCluster(clusterName);
    assertNull("Deleted cluster should not be found", retrieved);
  }

  @Test
  public void testCaseInsensitiveClusterNameRetrieval() {
    String clusterName = "Test_Cluster";

    Cluster cluster =
        Cluster.builder()
            .withName(clusterName)
            .withPartitioner("org.apache.cassandra.dht.Murmur3Partitioner")
            .withSeedHosts(ImmutableSet.of("127.0.0.1"))
            .withState(Cluster.State.ACTIVE)
            .build();

    storage.getClusterDao().addCluster(cluster);

    // Cluster names are normalized to lowercase, so retrieval should work with any case
    Cluster retrievedLower = storage.getClusterDao().getCluster(clusterName.toLowerCase());
    assertNotNull("Should retrieve with lowercase", retrievedLower);

    Cluster retrievedUpper = storage.getClusterDao().getCluster(clusterName.toUpperCase());
    assertNotNull("Should retrieve with uppercase", retrievedUpper);
  }

  @Test
  public void testEmptyJmxUsername() {
    String clusterName = "cluster_empty_username";

    // JmxCredentials with empty username should still be stored
    JmxCredentials credentials =
        JmxCredentials.builder().withUsername("").withPassword("password").build();

    Cluster cluster =
        Cluster.builder()
            .withName(clusterName)
            .withPartitioner("org.apache.cassandra.dht.Murmur3Partitioner")
            .withSeedHosts(ImmutableSet.of("127.0.0.1"))
            .withState(Cluster.State.ACTIVE)
            .withJmxCredentials(credentials)
            .build();

    storage.getClusterDao().addCluster(cluster);

    Cluster retrieved = storage.getClusterDao().getCluster(clusterName);
    assertTrue(retrieved.getJmxCredentials().isPresent());
    assertEquals("", retrieved.getJmxCredentials().get().getUsername());
  }
}
