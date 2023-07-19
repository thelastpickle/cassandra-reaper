/*
 * Copyright 2020-2020 The Last Pickle Ltd
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

package io.cassandrareaper.management.jmx;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.JmxCredentials;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.crypto.Cryptograph;
import io.cassandrareaper.management.jmx.JmxConnectionFactory;

import java.util.HashMap;
import java.util.Map;

import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class JmxConnectionFactoryTest {

  @Test
  public void fetchingJmxCredentialsPrioritizesThoseInStorageFirst() {
    JmxCredentials globalYamlJmxAuth = JmxCredentials.builder()
            .withUsername("global").withPassword("foo1").build();
    JmxCredentials clusterYamlJmxAuth = JmxCredentials.builder()
            .withUsername("cluster").withPassword("foo2").build();
    JmxCredentials clusterStorageJmxAuth = JmxCredentials.builder()
            .withUsername("storage").withPassword("foo3").build();
    Cluster cluster = Cluster.builder().withName("FooCluster")
            .withSeedHosts(ImmutableSet.of("127.0.0.1")).withJmxCredentials(clusterStorageJmxAuth).build();

    JmxConnectionFactory connectionFactory = new JmxConnectionFactory(mock(AppContext.class), mock(Cryptograph.class));
    connectionFactory.setJmxAuth(globalYamlJmxAuth);
    connectionFactory.setJmxCredentials(ImmutableMap.of(cluster.getName(), clusterYamlJmxAuth));

    Optional<JmxCredentials> jmxCredentials = connectionFactory.getJmxCredentialsForCluster(Optional.of(cluster));

    assertTrue(jmxCredentials.isPresent());
    assertEquals(clusterStorageJmxAuth.getUsername(), jmxCredentials.get().getUsername());
    assertEquals(clusterStorageJmxAuth.getPassword(), jmxCredentials.get().getPassword());
  }

  @Test
  public void fetchingJmxCredentialsPrioritizesClusterSpecificWhenMissingInStorage() {
    JmxCredentials globalYamlJmxAuth = JmxCredentials.builder()
            .withUsername("global").withPassword("foo1").build();
    JmxCredentials clusterYamlJmxAuth = JmxCredentials.builder()
            .withUsername("cluster").withPassword("foo2").build();
    Cluster cluster = Cluster.builder().withName("FooCluster")
            .withSeedHosts(ImmutableSet.of("127.0.0.1")).build();

    JmxConnectionFactory connectionFactory = new JmxConnectionFactory(mock(AppContext.class), mock(Cryptograph.class));
    connectionFactory.setJmxAuth(globalYamlJmxAuth);
    connectionFactory.setJmxCredentials(ImmutableMap.of(cluster.getName(), clusterYamlJmxAuth));

    Optional<JmxCredentials> jmxCredentials = connectionFactory.getJmxCredentialsForCluster(Optional.of(cluster));

    assertTrue(jmxCredentials.isPresent());
    assertEquals(clusterYamlJmxAuth.getUsername(), jmxCredentials.get().getUsername());
    assertEquals(clusterYamlJmxAuth.getPassword(), jmxCredentials.get().getPassword());
  }

  @Test
  public void fetchingJmxCredentialsDefaultsToGlobalWhenMissingEverywhereElse() {
    JmxCredentials globalYamlJmxAuth = JmxCredentials.builder()
            .withUsername("global").withPassword("foo1").build();
    Cluster cluster = Cluster.builder().withName("FooCluster")
            .withSeedHosts(ImmutableSet.of("127.0.0.1")).build();

    JmxConnectionFactory connectionFactory = new JmxConnectionFactory(mock(AppContext.class), mock(Cryptograph.class));
    connectionFactory.setJmxAuth(globalYamlJmxAuth);

    Optional<JmxCredentials> jmxCredentials = connectionFactory.getJmxCredentialsForCluster(Optional.of(cluster));

    assertTrue(jmxCredentials.isPresent());
    assertEquals(globalYamlJmxAuth.getUsername(), jmxCredentials.get().getUsername());
    assertEquals(globalYamlJmxAuth.getPassword(), jmxCredentials.get().getPassword());
  }

  @Test
  public void fetchingJmxCredentialsIsntPresentWhenNotDefinedAnywhere() {
    Cluster cluster = Cluster.builder().withName("FooCluster")
            .withSeedHosts(ImmutableSet.of("127.0.0.1")).build();

    JmxConnectionFactory connectionFactory = new JmxConnectionFactory(mock(AppContext.class), mock(Cryptograph.class));

    Optional<JmxCredentials> jmxCredentials = connectionFactory.getJmxCredentialsForCluster(Optional.of(cluster));

    assertFalse(jmxCredentials.isPresent());
  }

  @Test
  public void ensureIPv6HostCanBeUsed() {
    JmxConnectionFactory connectionFactory = new JmxConnectionFactory(mock(AppContext.class), mock(Cryptograph.class));
    String host = connectionFactory.determineHost(Node.builder()
            .withHostname("cc43:a32a:604:20b8:8201:ef29:3c20:c1d2")
            .build());
    assertEquals("[cc43:a32a:604:20b8:8201:ef29:3c20:c1d2]:7199", host);
  }

  @Test
  public void ensureJMXPortsHostCanBeUsed() {
    Map<String, Integer> jmxPorts = new HashMap<String,Integer>();
    jmxPorts.put("127.0.0.1", 7100);
    jmxPorts.put("127.0.0.2", 7200);
    jmxPorts.put("127.0.0.3", 7300);

    JmxConnectionFactory connectionFactory = new JmxConnectionFactory(mock(AppContext.class), mock(Cryptograph.class));
    connectionFactory.setJmxPorts(jmxPorts);
    String host = connectionFactory.determineHost(Node.builder().withHostname("127.0.0.2").build());
    assertEquals("127.0.0.2:7200", host);
  }

  @Test
  public void ensureIPv4HostCanBeUsed() {
    JmxConnectionFactory connectionFactory = new JmxConnectionFactory(mock(AppContext.class), mock(Cryptograph.class));
    String host = connectionFactory.determineHost(Node.builder().withHostname("127.0.0.1").build());
    assertEquals("127.0.0.1:7199", host);
  }
}