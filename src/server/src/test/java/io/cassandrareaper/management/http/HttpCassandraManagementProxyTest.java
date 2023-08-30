/*
 * Copyright 2023-2023 DataStax, Inc.
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

package io.cassandrareaper.management.http;

import io.cassandrareaper.core.Snapshot;
import io.cassandrareaper.core.Table;
import io.cassandrareaper.management.RepairStatusHandler;
import io.cassandrareaper.management.http.models.JobStatusTracker;

import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import com.datastax.mgmtapi.client.api.DefaultApi;
import com.datastax.mgmtapi.client.model.Job;
import com.datastax.mgmtapi.client.model.SnapshotDetails;
import com.datastax.mgmtapi.client.model.StatusChange;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.commons.lang3.concurrent.ConcurrentUtils;
import org.junit.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class HttpCassandraManagementProxyTest {

  @Test
  public void testConvertSnapshots() throws Exception {
    DefaultApi mockClient = Mockito.mock(DefaultApi.class);

    HttpCassandraManagementProxy proxy = mockProxy(mockClient);

    SnapshotDetails details = jsonFromResourceFile("example_snapshot_details.json", SnapshotDetails.class);
    List<Snapshot> snapshots = proxy.convertSnapshots(details);
    assertEquals(3, snapshots.size());
    // we have 3 sets of snapshot data
    Snapshot snapshot0 = null;
    Snapshot snapshot1 = null;
    Snapshot snapshot2 = null;
    for (int i = 0; i < snapshots.size(); ++i) {
      Snapshot snapshot = snapshots.get(i);
      switch (snapshot.getTable()) {
        case "booya0":
          snapshot0 = snapshot;
          break;
        case "booya1":
          snapshot1 = snapshot;
          break;
        case "booya2":
          snapshot2 = snapshot;
          break;
        default:
          fail("Unexpected Table name for snapshot: " + snapshot.getTable());
          break;
      }
    }
    // make sure all 3 snapshots have been found
    assertNotNull(snapshot0);
    assertNotNull(snapshot1);
    assertNotNull(snapshot2);
    // assert common values
    for (Snapshot snapshot : snapshots) {
      assertEquals("localhost", snapshot.getHost());
      assertEquals("testSnapshot", snapshot.getName());
      assertEquals("booya", snapshot.getKeyspace());
    }
    // assert sizes
    assertEquals("Size on disk mismatch", 122122.24, snapshot0.getSizeOnDisk(), 0d);
    assertEquals("Size on disk mismatch", 8488.96, snapshot1.getSizeOnDisk(), 0d);
    assertEquals("Size on disk mismatch", 8468.48, snapshot2.getSizeOnDisk(), 0d);
    assertEquals("True size mismatch", 413d, snapshot0.getTrueSize(), 0d);
    assertEquals("True size mismatch", 256d, snapshot1.getTrueSize(), 0d);
    assertEquals("True size mismatch", 10d, snapshot2.getTrueSize(), 0d);
  }

  private <T extends Object> T jsonFromResourceFile(String filename, Class<T> clazz) throws Exception {
    return new ObjectMapper().readValue(
        this.getClass().getResource(filename).openStream(), clazz);
  }

  // Verify all the maps are correctly updated in the triggerRepair and removeRepairHandler which get called
  // from other classes
  @Test
  public void testRepairProcessMapHandlers() throws Exception {
    DefaultApi mockClient = Mockito.mock(DefaultApi.class);
    when(mockClient.repair1(any())).thenReturn("repair-123456789");

    HttpCassandraManagementProxy httpCassandraManagementProxy = mockProxy(mockClient);

    RepairStatusHandler repairStatusHandler = Mockito.mock(RepairStatusHandler.class);

    int repairNo = httpCassandraManagementProxy.triggerRepair(BigInteger.ZERO, BigInteger.ONE, "ks",
        RepairParallelism.PARALLEL,
        Collections.singleton("table"), true, Collections.emptyList(), repairStatusHandler, Collections.emptyList(), 1);

    assertEquals(123456789, repairNo);
    assertEquals(1, httpCassandraManagementProxy.jobTracker.size());
    String jobId = String.format("repair-%d", repairNo);
    assertTrue(httpCassandraManagementProxy.jobTracker.containsKey(jobId));
    JobStatusTracker jobStatus = httpCassandraManagementProxy.jobTracker.get(jobId);
    assertEquals(0, jobStatus.latestNotificationCount.get());
    assertEquals(1, httpCassandraManagementProxy.repairStatusExecutors.size());
    assertEquals(1, httpCassandraManagementProxy.repairStatusHandlers.size());
    assertTrue(httpCassandraManagementProxy.repairStatusHandlers.containsKey(repairNo));

    httpCassandraManagementProxy.removeRepairStatusHandler(repairNo);
    assertEquals(0, httpCassandraManagementProxy.jobTracker.size());
    assertEquals(0, httpCassandraManagementProxy.repairStatusExecutors.size());
    assertEquals(0, httpCassandraManagementProxy.repairStatusHandlers.size());
  }

  @Test
  public void testNotificationsTracker() throws Exception {
    DefaultApi mockClient = Mockito.mock(DefaultApi.class);
    when(mockClient.repair1(any())).thenReturn("repair-123456789");

    HttpCassandraManagementProxy httpCassandraManagementProxy = mockProxy(mockClient);

    // Since we don't have existing implementation of RepairStatusHandler interface, we'll create a small "mock
    // implementation" here to catch all the calls to the handler() method
    final AtomicInteger callTimes = new AtomicInteger(0);
    RepairStatusHandler workAroundHandler = (repairNumber, status, progress, message, cassandraManagementProxy)
        -> callTimes.incrementAndGet();

    int repairNo = httpCassandraManagementProxy.triggerRepair(BigInteger.ZERO, BigInteger.ONE, "ks",
        RepairParallelism.PARALLEL,
        Collections.singleton("table"), true, Collections.emptyList(), workAroundHandler,
        Collections.emptyList(), 1);

    // We want the execution to happen in the same thread for this test
    httpCassandraManagementProxy.repairStatusExecutors.put(repairNo, MoreExecutors.newDirectExecutorService());

    Job job = new Job();
    job.setId("repair-123456789");
    StatusChange firstSc = new StatusChange();
    firstSc.setStatus("START");
    firstSc.setMessage("");
    List<StatusChange> statusChanges = new ArrayList<>();
    statusChanges.add(firstSc);
    job.setStatusChanges(statusChanges);
    when(mockClient.getJobStatus("repair-123456789")).thenReturn(job);

    httpCassandraManagementProxy.notificationsTracker().run();

    assertEquals(1, httpCassandraManagementProxy.jobTracker.size());
    String jobId = String.format("repair-%d", repairNo);
    assertTrue(httpCassandraManagementProxy.jobTracker.containsKey(jobId));
    JobStatusTracker jobStatus = httpCassandraManagementProxy.jobTracker.get(jobId);
    assertEquals(1, jobStatus.latestNotificationCount.get());

    verify(mockClient, times(1)).getJobStatus(any());
    assertEquals(1, callTimes.get());

    StatusChange secondSc = new StatusChange();
    secondSc.setStatus("COMPLETE");
    secondSc.setMessage("");
    statusChanges.add(secondSc);

    httpCassandraManagementProxy.notificationsTracker().run();
    jobStatus = httpCassandraManagementProxy.jobTracker.get(jobId);
    assertEquals(2, jobStatus.latestNotificationCount.get());
    assertEquals(2, callTimes.get());
  }

  @Test
  public void testGetKeyspaces() throws Exception {
    DefaultApi mockClient = Mockito.mock(DefaultApi.class);
    when(mockClient.listKeyspaces("")).thenReturn(ImmutableList.of("ks1", "ks2", "ks3"));

    HttpCassandraManagementProxy proxy = mockProxy(mockClient);
    List<String> keyspaces = proxy.getKeyspaces();

    assertThat(keyspaces).containsExactly("ks1", "ks2", "ks3");
    verify(mockClient).listKeyspaces("");
    verifyNoMoreInteractions(mockClient);
  }

  @Test
  public void testGetTablesForKeyspace() throws Exception {
    DefaultApi mockClient = Mockito.mock(DefaultApi.class);
    when(mockClient.listTablesV1("ks")).thenReturn(ImmutableList.of(
        new com.datastax.mgmtapi.client.model.Table().name("tbl1").putCompactionItem("class", "Compaction1"),
        new com.datastax.mgmtapi.client.model.Table().name("tbl2").putCompactionItem("class", "Compaction2"),
        new com.datastax.mgmtapi.client.model.Table().name("tbl3").putCompactionItem("class", "Compaction3")
    ));

    HttpCassandraManagementProxy proxy = mockProxy(mockClient);
    Set<Table> tables = proxy.getTablesForKeyspace("ks");

    assertThat(tables)
        .extracting(Table::getName, Table::getCompactionStrategy) // Table does not override equals
        .containsOnly(
            tuple("tbl1", "Compaction1"),
            tuple("tbl2", "Compaction2"),
            tuple("tbl3", "Compaction3"));
    verify(mockClient).listTablesV1("ks");
    verifyNoMoreInteractions(mockClient);
  }

  private static HttpCassandraManagementProxy mockProxy(DefaultApi mockClient) {
    ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
    when(executorService.submit(any(Callable.class))).thenAnswer(i -> {
      Callable<Object> callable = i.getArgument(0);
      callable.call();
      return ConcurrentUtils.constantFuture(null);
    });

    return new HttpCassandraManagementProxy(
        null, "/", InetSocketAddress.createUnresolved("localhost", 8080), executorService, mockClient);
  }
}
