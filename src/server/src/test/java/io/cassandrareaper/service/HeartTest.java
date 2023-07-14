/*
 * Copyright 2017-2017 Spotify AB
 * Copyright 2017-2019 The Last Pickle Ltd
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

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.Cluster.State;
import io.cassandrareaper.crypto.NoopCrypotograph;
import io.cassandrareaper.jmx.JmxConnectionFactory;
import io.cassandrareaper.jmx.JmxProxy;
import io.cassandrareaper.storage.MemoryStorageFacade;
import io.cassandrareaper.storage.cassandra.CassandraStorageFacade;
import io.cassandrareaper.storage.repairschedule.IRepairSchedule;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.collections.Sets;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;

public final class HeartTest {

  private static final int REPAIR_TIMEOUT_S = 60;
  private static final int RETRY_DELAY_S = 10;

  @Test
  public void testBeat_memoryStorage() throws ReaperException, InterruptedException {
    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.storage = Mockito.mock(MemoryStorageFacade.class);
    Cluster cluster = Cluster.builder()
        .withName("test")
        .withSeedHosts(Sets.newSet("127.0.0.1"))
        .withJmxPort(7199)
        .withPartitioner("Murmur3Partitioner")
        .withState(State.ACTIVE)
        .build();
    Mockito.when(context.storage.getClusters()).thenReturn(Arrays.asList(cluster));
    try (Heart heart = Heart.create(context)) {
      heart.beat();
    }
    Mockito.verify(context.storage, Mockito.times(1)).getClusters();
  }

  @Test
  public void testBeat_distributedStorage_noDatacenterAvailability()
      throws InterruptedException, ReaperException {

    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.storage = Mockito.mock(CassandraStorageFacade.class);
    Cluster cluster = Cluster.builder()
        .withName("test")
        .withSeedHosts(Sets.newSet("127.0.0.1"))
        .withJmxPort(7199)
        .withPartitioner("Murmur3Partitioner")
        .withState(State.ACTIVE)
        .build();
    Mockito.when(context.storage.getClusters()).thenReturn(Arrays.asList(cluster));
    IRepairSchedule mockedRepairScheduleDao = Mockito.mock(IRepairSchedule.class);
    Mockito.when(context.storage.getRepairScheduleDao()).thenReturn(mockedRepairScheduleDao);
    Mockito.when(
        mockedRepairScheduleDao.getRepairSchedulesForCluster(any(), anyBoolean())).thenReturn(Collections.emptyList());

    try (Heart heart = Heart.create(context)) {
      heart.beat();
      Awaitility.await().until(() -> {
        try {
          Mockito.verify((CassandraStorageFacade) context.storage, Mockito.times(1)).saveHeartbeat();
          return true;
        } catch (AssertionError ex) {
          return false;
        }
      });
      Assertions.assertThat(heart.isCurrentlyUpdatingNodeMetrics().get()).isFalse();
      Thread.sleep(500);
    }

    Mockito.verify(context.storage, Mockito.times(1)).getClusters();
  }

  @Test
  public void testBeat_distributedStorage_allDatacenterAvailability()
      throws InterruptedException, ReaperException {

    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.config.setDatacenterAvailability(ReaperApplicationConfiguration.DatacenterAvailability.ALL);
    context.storage = Mockito.mock(CassandraStorageFacade.class);
    Cluster cluster = Cluster.builder()
        .withName("test")
        .withSeedHosts(Sets.newSet("127.0.0.1"))
        .withJmxPort(7199)
        .withPartitioner("Murmur3Partitioner")
        .withState(State.ACTIVE)
        .build();
    Mockito.when(context.storage.getClusters()).thenReturn(Arrays.asList(cluster));
    IRepairSchedule mockedRepairScheduleDao = Mockito.mock(IRepairSchedule.class);
    Mockito.when(context.storage.getRepairScheduleDao()).thenReturn(mockedRepairScheduleDao);
    Mockito.when(mockedRepairScheduleDao.getRepairSchedulesForCluster(any(), anyBoolean()))
        .thenReturn(Collections.emptyList());

    try (Heart heart = Heart.create(context)) {
      heart.beat();
      Awaitility.await().until(() -> {
        try {
          Mockito.verify((CassandraStorageFacade) context.storage, Mockito.times(1)).saveHeartbeat();
          return true;
        } catch (AssertionError ex) {
          return false;
        }
      });
      Assertions.assertThat(heart.isCurrentlyUpdatingNodeMetrics().get()).isFalse();
      Thread.sleep(500);
    }
    Mockito.verify((CassandraStorageFacade) context.storage, Mockito.times(1)).saveHeartbeat();
    Mockito.verify(context.storage, Mockito.times(1)).getClusters();
  }

  @Test
  public void testBeat_distributedStorage_eachDatacenterAvailability() throws InterruptedException, ReaperException {

    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.config.setDatacenterAvailability(ReaperApplicationConfiguration.DatacenterAvailability.EACH);
    context.storage = Mockito.mock(CassandraStorageFacade.class);
    context.jmxConnectionFactory = new JmxConnectionFactory(context, new NoopCrypotograph());
    IRepairSchedule mockedRepairScheduleDao = Mockito.mock(IRepairSchedule.class);
    Mockito.when(context.storage.getRepairScheduleDao()).thenReturn(mockedRepairScheduleDao);
    Mockito.when(mockedRepairScheduleDao.getRepairSchedulesForCluster(any(), anyBoolean()))
        .thenReturn(Collections.emptyList());

    try (Heart heart = Heart.create(context)) {
      context.isDistributed.set(true);
      heart.beat();
      Thread.sleep(500);
    }
    Mockito.verify((CassandraStorageFacade) context.storage, Mockito.times(1)).saveHeartbeat();
  }

  @Test
  public void testBeat_distributedStorage_eachDatacenterAvailability_repairs()
      throws InterruptedException, ReaperException {

    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.config.setDatacenterAvailability(ReaperApplicationConfiguration.DatacenterAvailability.EACH);
    context.storage = new MemoryStorageFacade();
    context.repairManager = RepairManager.create(
        context,
        Executors.newScheduledThreadPool(1),
        RETRY_DELAY_S,
        TimeUnit.SECONDS,
        1,
        context.storage.getRepairRunDao());

    context.repairManager.repairRunners.put(UUID.randomUUID(), Mockito.mock(RepairRunner.class));
    context.repairManager.repairRunners.put(UUID.randomUUID(), Mockito.mock(RepairRunner.class));

    context.storage = Mockito.mock(CassandraStorageFacade.class);
    context.jmxConnectionFactory = new JmxConnectionFactory(context, new NoopCrypotograph());

    try (Heart heart = Heart.create(context)) {
      context.isDistributed.set(true);
      heart.beat();
      Thread.sleep(500);
    }
    Mockito.verify((CassandraStorageFacade) context.storage, Mockito.times(1)).saveHeartbeat();
  }

  @Test
  public void testBeat_distributedStorage_eachDatacenterAvailability_repairs_noMetrics()
      throws InterruptedException, ReaperException {

    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.config.setDatacenterAvailability(ReaperApplicationConfiguration.DatacenterAvailability.EACH);
    context.storage = new MemoryStorageFacade();
    context.repairManager = RepairManager.create(
        context,
        Executors.newScheduledThreadPool(1),
        RETRY_DELAY_S,
        TimeUnit.SECONDS,
        1,
        context.storage.getRepairRunDao());

    context.repairManager.repairRunners.put(UUID.randomUUID(), Mockito.mock(RepairRunner.class));
    context.repairManager.repairRunners.put(UUID.randomUUID(), Mockito.mock(RepairRunner.class));

    context.storage = Mockito.mock(CassandraStorageFacade.class);
    context.jmxConnectionFactory = Mockito.mock(JmxConnectionFactory.class);

    try (Heart heart = Heart.create(context)) {
      context.isDistributed.set(true);
      heart.beat();
      Thread.sleep(500);
    }

    Mockito.verify((CassandraStorageFacade) context.storage, Mockito.times(1)).saveHeartbeat();
    Mockito.verify(context.jmxConnectionFactory, Mockito.times(0)).connectAny(any(Collection.class));
  }

  @Test
  public void testBeat_distributedStorage_eachDatacenterAvailability_repairs_noRequests()
      throws InterruptedException, ReaperException {

    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.config.setDatacenterAvailability(ReaperApplicationConfiguration.DatacenterAvailability.EACH);
    context.storage = Mockito.mock(CassandraStorageFacade.class);

    context.repairManager = RepairManager.create(
        context,
        Executors.newScheduledThreadPool(1),
        RETRY_DELAY_S,
        TimeUnit.SECONDS,
        1,
        context.storage.getRepairRunDao());

    context.repairManager.repairRunners.put(UUID.randomUUID(), Mockito.mock(RepairRunner.class));
    context.repairManager.repairRunners.put(UUID.randomUUID(), Mockito.mock(RepairRunner.class));

    context.storage = Mockito.mock(CassandraStorageFacade.class);
    context.jmxConnectionFactory = Mockito.mock(JmxConnectionFactory.class);

    JmxProxy nodeProxy = Mockito.mock(JmxProxy.class);

    Mockito.when(context.jmxConnectionFactory.connectAny(any(Collection.class))).thenReturn(nodeProxy);

    try (Heart heart = Heart.create(context)) {
      context.isDistributed.set(true);
      heart.beat();
      Thread.sleep(500);
    }

    Mockito.verify((CassandraStorageFacade) context.storage, Mockito.times(1)).saveHeartbeat();
    Mockito.verify(context.jmxConnectionFactory, Mockito.times(0)).connectAny(any(Collection.class));
  }

  @Test
  public void testBeat_distributedStorage_eachDatacenterAvailability_repairs_requests()
      throws InterruptedException, ReaperException {

    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.config.setDatacenterAvailability(ReaperApplicationConfiguration.DatacenterAvailability.EACH);
    context.storage = Mockito.mock(CassandraStorageFacade.class);

    context.repairManager = RepairManager.create(
        context,
        Executors.newScheduledThreadPool(1),
        RETRY_DELAY_S,
        TimeUnit.SECONDS,
        1,
        context.storage.getRepairRunDao());

    context.repairManager.repairRunners.put(UUID.randomUUID(), Mockito.mock(RepairRunner.class));
    context.repairManager.repairRunners.put(UUID.randomUUID(), Mockito.mock(RepairRunner.class));

    context.storage = Mockito.mock(CassandraStorageFacade.class);
    context.jmxConnectionFactory = Mockito.mock(JmxConnectionFactory.class);

    Mockito.when(((CassandraStorageFacade) context.storage).getCluster(any()))
        .thenReturn(
            Cluster.builder()
                .withName("cluster1")
                .withSeedHosts(ImmutableSet.of("test"))
                .withJmxPort(7199)
                .build());

    JmxProxy nodeProxy = Mockito.mock(JmxProxy.class);

    Mockito.when(context.jmxConnectionFactory.connectAny(any(Collection.class))).thenReturn(nodeProxy);

    try (Heart heart = Heart.create(context)) {
      context.isDistributed.set(true);
      heart.beat();
      Thread.sleep(500);
    }

    Mockito.verify((CassandraStorageFacade) context.storage, Mockito.times(1)).saveHeartbeat();
  }
}