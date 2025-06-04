/*
 * Copyright 2014-2017 Spotify AB
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

package io.cassandrareaper.core;

import java.time.LocalDate;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableSet;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public final class ClusterTest {

  @Test
  public void testGetSymbolicName() {
    assertEquals("example2cluster", Cluster.toSymbolicName("Example2 Cluster"));
    assertEquals("example2_cluster", Cluster.toSymbolicName("Example2_Cluster"));
  }

  @Test
  public void testCreate_0() {
    Cluster cluster =
        Cluster.builder().withName("test").withSeedHosts(ImmutableSet.of("127.0.0.1")).build();

    Assertions.assertThat(cluster.getName()).isEqualTo("test");
    Assertions.assertThat(cluster.getSeedHosts()).hasSize(1);
    Assertions.assertThat(cluster.getSeedHosts()).contains("127.0.0.1");
    Assertions.assertThat(cluster.getPartitioner()).isEmpty();
    Assertions.assertThat(cluster.getJmxPort()).isEqualTo(Cluster.DEFAULT_JMX_PORT);
    Assertions.assertThat(cluster.getState()).isEqualTo(Cluster.State.UNKNOWN);
    Assertions.assertThat(cluster.getLastContact()).isEqualTo(LocalDate.MIN);
  }

  @Test
  public void testCreate_1() {
    Cluster cluster =
        Cluster.builder()
            .withName("test")
            .withSeedHosts(ImmutableSet.of("127.0.0.1", "127.0.0.2"))
            .build();

    Assertions.assertThat(cluster.getName()).isEqualTo("test");
    Assertions.assertThat(cluster.getSeedHosts()).hasSize(2);
    Assertions.assertThat(cluster.getSeedHosts()).contains("127.0.0.1", "127.0.0.2");
    Assertions.assertThat(cluster.getPartitioner()).isEmpty();
    Assertions.assertThat(cluster.getJmxPort()).isEqualTo(Cluster.DEFAULT_JMX_PORT);
    Assertions.assertThat(cluster.getState()).isEqualTo(Cluster.State.UNKNOWN);
    Assertions.assertThat(cluster.getLastContact()).isEqualTo(LocalDate.MIN);
  }

  @Test
  public void testCreate_2() {
    Cluster cluster =
        Cluster.builder()
            .withName("test")
            .withSeedHosts(ImmutableSet.of("127.0.0.1", "127.0.0.2"))
            .withPartitioner("murmur3")
            .build();

    Assertions.assertThat(cluster.getName()).isEqualTo("test");
    Assertions.assertThat(cluster.getSeedHosts()).hasSize(2);
    Assertions.assertThat(cluster.getSeedHosts()).contains("127.0.0.1", "127.0.0.2");
    Assertions.assertThat(cluster.getPartitioner()).isPresent();
    Assertions.assertThat(cluster.getPartitioner().get()).isEqualTo("murmur3");
    Assertions.assertThat(cluster.getJmxPort()).isEqualTo(Cluster.DEFAULT_JMX_PORT);
    Assertions.assertThat(cluster.getState()).isEqualTo(Cluster.State.UNKNOWN);
    Assertions.assertThat(cluster.getLastContact()).isEqualTo(LocalDate.MIN);
  }

  @Test
  public void testCreate_3() {
    Cluster cluster =
        Cluster.builder()
            .withName("test")
            .withSeedHosts(ImmutableSet.of("127.0.0.1", "127.0.0.2"))
            .withPartitioner("murmur3")
            .withJmxPort(9999)
            .build();

    Assertions.assertThat(cluster.getName()).isEqualTo("test");
    Assertions.assertThat(cluster.getSeedHosts()).hasSize(2);
    Assertions.assertThat(cluster.getSeedHosts()).contains("127.0.0.1", "127.0.0.2");
    Assertions.assertThat(cluster.getPartitioner()).isPresent();
    Assertions.assertThat(cluster.getPartitioner().get()).isEqualTo("murmur3");
    Assertions.assertThat(cluster.getJmxPort()).isEqualTo(9999);
    Assertions.assertThat(cluster.getState()).isEqualTo(Cluster.State.UNKNOWN);
    Assertions.assertThat(cluster.getLastContact()).isEqualTo(LocalDate.MIN);
  }

  @Test
  public void testCreate_4() {
    Cluster cluster =
        Cluster.builder()
            .withName("test")
            .withSeedHosts(ImmutableSet.of("127.0.0.1", "127.0.0.2"))
            .withPartitioner("murmur3")
            .withJmxPort(9999)
            .withState(Cluster.State.ACTIVE)
            .build();

    Assertions.assertThat(cluster.getName()).isEqualTo("test");
    Assertions.assertThat(cluster.getSeedHosts()).hasSize(2);
    Assertions.assertThat(cluster.getSeedHosts()).contains("127.0.0.1", "127.0.0.2");
    Assertions.assertThat(cluster.getPartitioner()).isPresent();
    Assertions.assertThat(cluster.getPartitioner().get()).isEqualTo("murmur3");
    Assertions.assertThat(cluster.getJmxPort()).isEqualTo(9999);
    Assertions.assertThat(cluster.getState()).isEqualTo(Cluster.State.ACTIVE);
    Assertions.assertThat(cluster.getLastContact()).isEqualTo(LocalDate.MIN);
  }

  @Test
  public void testCreate_5() {
    Cluster cluster =
        Cluster.builder()
            .withName("test")
            .withSeedHosts(ImmutableSet.of("127.0.0.1", "127.0.0.2"))
            .withPartitioner("murmur3")
            .withJmxPort(9999)
            .withState(Cluster.State.ACTIVE)
            .withLastContact(LocalDate.now())
            .build();

    Assertions.assertThat(cluster.getName()).isEqualTo("test");
    Assertions.assertThat(cluster.getSeedHosts()).hasSize(2);
    Assertions.assertThat(cluster.getSeedHosts()).contains("127.0.0.1", "127.0.0.2");
    Assertions.assertThat(cluster.getPartitioner()).isPresent();
    Assertions.assertThat(cluster.getPartitioner().get()).isEqualTo("murmur3");
    Assertions.assertThat(cluster.getJmxPort()).isEqualTo(9999);
    Assertions.assertThat(cluster.getState()).isEqualTo(Cluster.State.ACTIVE);
    Assertions.assertThat(cluster.getLastContact()).isEqualTo(LocalDate.now());
  }

  @Test
  public void testCreate_6() {
    Cluster cluster =
        Cluster.builder()
            .withName("test")
            .withSeedHosts(ImmutableSet.of("127.0.0.1", "127.0.0.2"))
            .withPartitioner("murmur3")
            .withJmxPort(9999)
            .withState(Cluster.State.UNREACHABLE)
            .withLastContact(LocalDate.now().minusDays(1))
            .build();

    Assertions.assertThat(cluster.getName()).isEqualTo("test");
    Assertions.assertThat(cluster.getSeedHosts()).hasSize(2);
    Assertions.assertThat(cluster.getSeedHosts()).contains("127.0.0.1", "127.0.0.2");
    Assertions.assertThat(cluster.getPartitioner()).isPresent();
    Assertions.assertThat(cluster.getPartitioner().get()).isEqualTo("murmur3");
    Assertions.assertThat(cluster.getJmxPort()).isEqualTo(9999);
    Assertions.assertThat(cluster.getState()).isEqualTo(Cluster.State.UNREACHABLE);
    Assertions.assertThat(cluster.getLastContact()).isEqualTo(LocalDate.now().minusDays(1));
  }

  @Test(expected = NullPointerException.class)
  public void testCreate_fail_0() {
    Cluster.builder()
        .withSeedHosts(ImmutableSet.of("127.0.0.1", "127.0.0.2"))
        .withPartitioner("murmur3")
        .withJmxPort(9999)
        .withState(Cluster.State.UNREACHABLE)
        .withLastContact(LocalDate.now().minusDays(1))
        .build();
  }

  @Test(expected = NullPointerException.class)
  public void testCreate_fail_1() {
    Cluster.builder()
        .withName("test")
        .withPartitioner("murmur3")
        .withJmxPort(9999)
        .withState(Cluster.State.UNREACHABLE)
        .withLastContact(LocalDate.now().minusDays(1))
        .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreate_fail_2() {
    Cluster.builder()
        .withName("test")
        .withSeedHosts(ImmutableSet.of())
        .withPartitioner("murmur3")
        .withJmxPort(9999)
        .withState(Cluster.State.UNREACHABLE)
        .withLastContact(LocalDate.now().minusDays(1))
        .build();
  }
}
