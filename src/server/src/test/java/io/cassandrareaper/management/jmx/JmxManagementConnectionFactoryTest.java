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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.JmxCredentials;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.crypto.Cryptograph;
import io.cassandrareaper.storage.IStorageDao;
import io.cassandrareaper.storage.cluster.IClusterDao;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

public class JmxManagementConnectionFactoryTest {

  @Test
  public void fetchingJmxCredentialsPrioritizesThoseInStorageFirst() {
    AppContext context = mock(AppContext.class);
    context.config = new ReaperApplicationConfiguration();
    context.storage = mock(IStorageDao.class);
    when(context.storage.getClusterDao()).thenReturn(mock(IClusterDao.class));
    context.metricRegistry = mock(MetricRegistry.class);
    when(context.metricRegistry.timer(any())).thenReturn(mock(com.codahale.metrics.Timer.class));

    JmxCredentials globalYamlJmxAuth =
        JmxCredentials.builder().withUsername("global").withPassword("foo1").build();
    JmxCredentials clusterYamlJmxAuth =
        JmxCredentials.builder().withUsername("cluster").withPassword("foo2").build();
    JmxCredentials clusterStorageJmxAuth =
        JmxCredentials.builder().withUsername("storage").withPassword("foo3").build();
    Cluster cluster =
        Cluster.builder()
            .withName("FooCluster")
            .withSeedHosts(ImmutableSet.of("127.0.0.1"))
            .withJmxCredentials(clusterStorageJmxAuth)
            .build();

    JmxManagementConnectionFactory connectionFactory =
        new JmxManagementConnectionFactory(context, mock(Cryptograph.class));
    connectionFactory.setJmxAuth(globalYamlJmxAuth);
    connectionFactory.setJmxCredentials(ImmutableMap.of(cluster.getName(), clusterYamlJmxAuth));

    Optional<JmxCredentials> jmxCredentials =
        connectionFactory.getJmxCredentialsForCluster(Optional.of(cluster));

    assertTrue(jmxCredentials.isPresent());
    assertEquals(clusterStorageJmxAuth.getUsername(), jmxCredentials.get().getUsername());
    assertEquals(clusterStorageJmxAuth.getPassword(), jmxCredentials.get().getPassword());
  }

  @Test
  public void fetchingJmxCredentialsPrioritizesClusterSpecificWhenMissingInStorage() {
    AppContext context = mock(AppContext.class);
    context.config = new ReaperApplicationConfiguration();
    context.storage = mock(IStorageDao.class);
    when(context.storage.getClusterDao()).thenReturn(mock(IClusterDao.class));
    context.metricRegistry = mock(MetricRegistry.class);
    when(context.metricRegistry.timer(any())).thenReturn(mock(com.codahale.metrics.Timer.class));

    JmxCredentials globalYamlJmxAuth =
        JmxCredentials.builder().withUsername("global").withPassword("foo1").build();
    JmxCredentials clusterYamlJmxAuth =
        JmxCredentials.builder().withUsername("cluster").withPassword("foo2").build();
    Cluster cluster =
        Cluster.builder()
            .withName("FooCluster")
            .withSeedHosts(ImmutableSet.of("127.0.0.1"))
            .build();
    JmxManagementConnectionFactory connectionFactory =
        new JmxManagementConnectionFactory(context, mock(Cryptograph.class));
    connectionFactory.setJmxAuth(globalYamlJmxAuth);
    connectionFactory.setJmxCredentials(ImmutableMap.of(cluster.getName(), clusterYamlJmxAuth));

    Optional<JmxCredentials> jmxCredentials =
        connectionFactory.getJmxCredentialsForCluster(Optional.of(cluster));

    assertTrue(jmxCredentials.isPresent());
    assertEquals(clusterYamlJmxAuth.getUsername(), jmxCredentials.get().getUsername());
    assertEquals(clusterYamlJmxAuth.getPassword(), jmxCredentials.get().getPassword());
  }

  @Test
  public void fetchingJmxCredentialsDefaultsToGlobalWhenMissingEverywhereElse() {
    AppContext context = mock(AppContext.class);
    context.config = new ReaperApplicationConfiguration();
    context.storage = mock(IStorageDao.class);
    when(context.storage.getClusterDao()).thenReturn(mock(IClusterDao.class));
    context.metricRegistry = mock(MetricRegistry.class);
    when(context.metricRegistry.timer(any())).thenReturn(mock(com.codahale.metrics.Timer.class));

    JmxCredentials globalYamlJmxAuth =
        JmxCredentials.builder().withUsername("global").withPassword("foo1").build();
    Cluster cluster =
        Cluster.builder()
            .withName("FooCluster")
            .withSeedHosts(ImmutableSet.of("127.0.0.1"))
            .build();

    JmxManagementConnectionFactory connectionFactory =
        new JmxManagementConnectionFactory(context, mock(Cryptograph.class));
    connectionFactory.setJmxAuth(globalYamlJmxAuth);

    Optional<JmxCredentials> jmxCredentials =
        connectionFactory.getJmxCredentialsForCluster(Optional.of(cluster));

    assertTrue(jmxCredentials.isPresent());
    assertEquals(globalYamlJmxAuth.getUsername(), jmxCredentials.get().getUsername());
    assertEquals(globalYamlJmxAuth.getPassword(), jmxCredentials.get().getPassword());
  }

  @Test
  public void fetchingJmxCredentialsIsntPresentWhenNotDefinedAnywhere() {
    AppContext context = mock(AppContext.class);
    context.config = new ReaperApplicationConfiguration();
    context.storage = mock(IStorageDao.class);
    context.metricRegistry = mock(MetricRegistry.class);
    when(context.metricRegistry.timer(any())).thenReturn(mock(com.codahale.metrics.Timer.class));
    when(context.storage.getClusterDao()).thenReturn(mock(IClusterDao.class));

    Cluster cluster =
        Cluster.builder()
            .withName("FooCluster")
            .withSeedHosts(ImmutableSet.of("127.0.0.1"))
            .build();

    JmxManagementConnectionFactory connectionFactory =
        new JmxManagementConnectionFactory(context, mock(Cryptograph.class));

    Optional<JmxCredentials> jmxCredentials =
        connectionFactory.getJmxCredentialsForCluster(Optional.of(cluster));

    assertFalse(jmxCredentials.isPresent());
  }

  @Test
  public void ensureIPv6HostCanBeUsed() {
    AppContext context = mock(AppContext.class);
    context.config = new ReaperApplicationConfiguration();
    context.storage = mock(IStorageDao.class);
    when(context.storage.getClusterDao()).thenReturn(mock(IClusterDao.class));
    context.metricRegistry = mock(MetricRegistry.class);
    when(context.metricRegistry.timer(any())).thenReturn(mock(com.codahale.metrics.Timer.class));
    JmxManagementConnectionFactory connectionFactory =
        new JmxManagementConnectionFactory(context, mock(Cryptograph.class));
    String host =
        connectionFactory.determineHost(
            Node.builder().withHostname("cc43:a32a:604:20b8:8201:ef29:3c20:c1d2").build());
    assertEquals("[cc43:a32a:604:20b8:8201:ef29:3c20:c1d2]:7199", host);
  }

  @Test
  public void ensureJMXPortsHostCanBeUsed() {
    Map<String, Integer> jmxPorts = new HashMap<String, Integer>();
    jmxPorts.put("127.0.0.1", 7100);
    jmxPorts.put("127.0.0.2", 7200);
    jmxPorts.put("127.0.0.3", 7300);
    AppContext context = mock(AppContext.class);
    context.config = new ReaperApplicationConfiguration();
    context.storage = mock(IStorageDao.class);
    when(context.storage.getClusterDao()).thenReturn(mock(IClusterDao.class));
    context.metricRegistry = mock(MetricRegistry.class);
    when(context.metricRegistry.timer(any())).thenReturn(mock(com.codahale.metrics.Timer.class));
    JmxManagementConnectionFactory connectionFactory =
        new JmxManagementConnectionFactory(context, mock(Cryptograph.class));
    connectionFactory.setJmxPorts(jmxPorts);
    String host = connectionFactory.determineHost(Node.builder().withHostname("127.0.0.2").build());
    assertEquals("127.0.0.2:7200", host);
  }

  @Test
  public void ensureIPv4HostCanBeUsed() {
    AppContext context = mock(AppContext.class);
    context.storage = mock(IStorageDao.class);
    when(context.storage.getClusterDao()).thenReturn(mock(IClusterDao.class));
    context.config = new ReaperApplicationConfiguration();
    context.metricRegistry = mock(MetricRegistry.class);
    when(context.metricRegistry.timer(any())).thenReturn(mock(com.codahale.metrics.Timer.class));
    JmxManagementConnectionFactory connectionFactory =
        new JmxManagementConnectionFactory(context, mock(Cryptograph.class));
    String host = connectionFactory.determineHost(Node.builder().withHostname("127.0.0.1").build());
    assertEquals("127.0.0.1:7199", host);
  }
}
