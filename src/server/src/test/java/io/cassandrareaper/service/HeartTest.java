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
import io.cassandrareaper.management.ICassandraManagementProxy;
import io.cassandrareaper.management.jmx.JmxCassandraManagementProxy;
import io.cassandrareaper.management.jmx.JmxManagementConnectionFactory;
import io.cassandrareaper.storage.IStorageDao;
import io.cassandrareaper.storage.MemoryStorageFacade;
import io.cassandrareaper.storage.cassandra.CassandraStorageFacade;
import io.cassandrareaper.storage.cluster.IClusterDao;
import io.cassandrareaper.storage.repairschedule.IRepairScheduleDao;

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
    IStorageDao storage = Mockito.mock(IStorageDao.class);
    IClusterDao mockedClusterDao = Mockito.mock(IClusterDao.class);
    Mockito.when(storage.getClusterDao()).thenReturn(mockedClusterDao);
    context.storage = storage;
    Cluster cluster = Cluster.builder()
        .withName("test")
        .withSeedHosts(Sets.newSet("127.0.0.1"))
        .withJmxPort(7199)
        .withPartitioner("Murmur3Partitioner")
        .withState(State.ACTIVE)
        .build();
    Mockito.when(context.storage.getClusterDao().getClusters()).thenReturn(Arrays.asList(cluster));
    try (Heart heart = Heart.create(context)) {
      heart.beat();
    }
    Mockito.verify(mockedClusterDao, Mockito.times(1)).getClusters();
  }

  @Test
  public void testBeat_distributedStorage_noDatacenterAvailability()
      throws InterruptedException, ReaperException {

    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    IStorageDao storage = Mockito.mock(CassandraStorageFacade.class);
    IClusterDao mockedClusterDao = Mockito.mock(IClusterDao.class);
    Mockito.when(storage.getClusterDao()).thenReturn(mockedClusterDao);
    context.storage = storage;
    Cluster cluster = Cluster.builder()
        .withName("test")
        .withSeedHosts(Sets.newSet("127.0.0.1"))
        .withJmxPort(7199)
        .withPartitioner("Murmur3Partitioner")
        .withState(State.ACTIVE)
        .build();
    Mockito.when(context.storage.getClusterDao().getClusters()).thenReturn(Arrays.asList(cluster));
    IRepairScheduleDao mockedRepairScheduleDao = Mockito.mock(IRepairScheduleDao.class);
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

    Mockito.verify(mockedClusterDao, Mockito.times(1)).getClusters();
  }

  @Test
  public void testBeat_distributedStorage_allDatacenterAvailability()
      throws InterruptedException, ReaperException {

    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.config.setDatacenterAvailability(ReaperApplicationConfiguration.DatacenterAvailability.ALL);
    IStorageDao storage = Mockito.mock(CassandraStorageFacade.class);
    IClusterDao mockedClusterDao = Mockito.mock(IClusterDao.class);
    Mockito.when(storage.getClusterDao()).thenReturn(mockedClusterDao);
    context.storage = storage;
    Cluster cluster = Cluster.builder()
        .withName("test")
        .withSeedHosts(Sets.newSet("127.0.0.1"))
        .withJmxPort(7199)
        .withPartitioner("Murmur3Partitioner")
        .withState(State.ACTIVE)
        .build();
    Mockito.when(context.storage.getClusterDao().getClusters()).thenReturn(Arrays.asList(cluster));
    IRepairScheduleDao mockedRepairScheduleDao = Mockito.mock(IRepairScheduleDao.class);
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
    Mockito.verify(mockedClusterDao, Mockito.times(1)).getClusters();
  }

  @Test
  public void testBeat_distributedStorage_eachDatacenterAvailability() throws InterruptedException, ReaperException {

    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.config.setDatacenterAvailability(ReaperApplicationConfiguration.DatacenterAvailability.EACH);
    context.storage = Mockito.mock(CassandraStorageFacade.class);
    Mockito.when(context.storage.getClusterDao()).thenReturn(Mockito.mock(IClusterDao.class));
    context.managementConnectionFactory = new JmxManagementConnectionFactory(context, new NoopCrypotograph());
    IRepairScheduleDao mockedRepairScheduleDao = Mockito.mock(IRepairScheduleDao.class);
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
    Mockito.when(context.storage.getClusterDao()).thenReturn(Mockito.mock(IClusterDao.class));
    context.managementConnectionFactory = new JmxManagementConnectionFactory(context, new NoopCrypotograph());

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
    context.managementConnectionFactory = Mockito.mock(JmxManagementConnectionFactory.class);

    try (Heart heart = Heart.create(context)) {
      context.isDistributed.set(true);
      heart.beat();
      Thread.sleep(500);
    }

    Mockito.verify((CassandraStorageFacade) context.storage, Mockito.times(1)).saveHeartbeat();
    Mockito.verify(((JmxManagementConnectionFactory) context.managementConnectionFactory),
            Mockito.times(0)).connectAny(any(Collection.class));
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
    context.managementConnectionFactory = Mockito.mock(JmxManagementConnectionFactory.class);

    ICassandraManagementProxy nodeProxy = Mockito.mock(ICassandraManagementProxy.class);

    Mockito.when(((JmxManagementConnectionFactory) context.managementConnectionFactory)
            .connectAny(any(Collection.class))).thenReturn((JmxCassandraManagementProxy) nodeProxy);

    try (Heart heart = Heart.create(context)) {
      context.isDistributed.set(true);
      heart.beat();
      Thread.sleep(500);
    }

    Mockito.verify((CassandraStorageFacade) context.storage, Mockito.times(1)).saveHeartbeat();
    Mockito.verify(((JmxManagementConnectionFactory) context.managementConnectionFactory),
            Mockito.times(0)).connectAny(any(Collection.class));
  }

  @Test
  public void testBeat_distributedStorage_eachDatacenterAvailability_repairs_requests()
      throws InterruptedException, ReaperException {

    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.config.setDatacenterAvailability(ReaperApplicationConfiguration.DatacenterAvailability.EACH);
    IStorageDao storage = Mockito.mock(IStorageDao.class);
    IClusterDao mockedClusterDao = Mockito.mock(IClusterDao.class);
    Mockito.when(storage.getClusterDao()).thenReturn(mockedClusterDao);
    context.storage = storage;

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
    context.managementConnectionFactory = Mockito.mock(JmxManagementConnectionFactory.class);

    Mockito.when(mockedClusterDao.getCluster(any()))
        .thenReturn(
            Cluster.builder()
                .withName("cluster1")
                .withSeedHosts(ImmutableSet.of("test"))
                .withJmxPort(7199)
                .build());

    ICassandraManagementProxy nodeProxy = Mockito.mock(ICassandraManagementProxy.class);

    Mockito.when(((JmxManagementConnectionFactory) context.managementConnectionFactory)
            .connectAny(any(Collection.class))).thenReturn((JmxCassandraManagementProxy) nodeProxy);

    try (Heart heart = Heart.create(context)) {
      context.isDistributed.set(true);
      heart.beat();
      Thread.sleep(500);
    }

    Mockito.verify((CassandraStorageFacade) context.storage, Mockito.times(1)).saveHeartbeat();
  }
}