/*
 * Copyright 2018-2019 The Last Pickle Ltd
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

package io.cassandrareaper.service;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.StreamSession;
import io.cassandrareaper.management.ClusterFacade;
import io.cassandrareaper.management.HostConnectionCounters;
import io.cassandrareaper.management.ICassandraManagementProxy;
import io.cassandrareaper.management.jmx.CassandraManagementProxyTest;
import io.cassandrareaper.management.jmx.JmxManagementConnectionFactory;

import java.io.IOException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import javax.management.openmbean.ArrayType;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class StreamServiceTest {

  @Test
  public void testListStreams()
      throws ReaperException, ClassNotFoundException, InterruptedException, UnknownHostException {
    ICassandraManagementProxy proxy = CassandraManagementProxyTest.mockJmxProxyImpl();

    AppContext cxt = new AppContext();
    cxt.config = TestRepairConfiguration.defaultConfig();
    cxt.managementConnectionFactory = mock(JmxManagementConnectionFactory.class);
    when(cxt.managementConnectionFactory.connectAny(Mockito.anyList())).thenReturn(proxy);
    ClusterFacade clusterFacadeSpy = Mockito.spy(ClusterFacade.create(cxt));
    Mockito.doReturn("dc1").when(clusterFacadeSpy).getDatacenter(any(), any());

    // do the actual pullStreams() call, which should succeed
    List<StreamSession> result = StreamService
        .create(() -> clusterFacadeSpy)
        .listStreams(Node.builder().withHostname("127.0.0.1").withCluster(Cluster.builder().withJmxPort(7199)
            .withSeedHosts(ImmutableSet.of("127.0.0.1")).withName("test").build()).build());

    verify(proxy, times(1)).getCurrentStreams();
  }

  @Test
  public void testGetStreams_2_0_17()
      throws OpenDataException, IOException, ReaperException, ClassNotFoundException, InterruptedException {

    // fake the response a 2.0.17 would return when asked for streams
    CompositeData streamSession = makeCompositeData_2_0_17();

    // compare the test payload with an actual payload grabbed from a 2.0.17 ccm node
    URL url = Resources.getResource("repair-samples/stream-report-2-0-17.txt");
    String ref = Resources.toString(url, Charsets.UTF_8);
    assertEquals(ref.replaceAll("\\s", ""), streamSession.toString().replaceAll("\\s", ""));

    // init the stream manager
    ICassandraManagementProxy proxy = (ICassandraManagementProxy) mock(
        Class.forName("io.cassandrareaper.management.jmx.JmxCassandraManagementProxy"));

    AppContext cxt = new AppContext();
    cxt.config = TestRepairConfiguration.defaultConfig();
    cxt.managementConnectionFactory = mock(JmxManagementConnectionFactory.class);
    when(cxt.managementConnectionFactory.connectAny(Mockito.anyList())).thenReturn(proxy);
    HostConnectionCounters connectionCounters = mock(HostConnectionCounters.class);
    when(cxt.managementConnectionFactory.getHostConnectionCounters()).thenReturn(connectionCounters);
    when(connectionCounters.getSuccessfulConnections(any())).thenReturn(1);
    ClusterFacade clusterFacadeSpy = Mockito.spy(ClusterFacade.create(cxt));
    Mockito.doReturn("dc1").when(clusterFacadeSpy).getDatacenter(any(), any());
    when(proxy.getCurrentStreams()).thenReturn(ImmutableSet.of(streamSession));

    // do the actual pullStreams() call, which should succeed
    List<StreamSession> result = StreamService
        .create(() -> clusterFacadeSpy)
        .listStreams(Node.builder().withHostname("127.0.0.1").withCluster(Cluster.builder().withJmxPort(7199)
            .withSeedHosts(ImmutableSet.of("127.0.0.1")).withName("test").build()).build());

    verify(proxy, times(1)).getCurrentStreams();
    assertEquals(1, result.size());
  }

  @Test
  public void testGetStreams_2_1_20()
      throws OpenDataException, IOException, ReaperException, ClassNotFoundException, InterruptedException {

    // fake the response a 2.1.20 would return when asked for streams
    CompositeData streamSession = makeCompositeData_2_1_20();

    // compare the test payload with an actual payload grabbed from a 2.1.20 ccm node
    URL url = Resources.getResource("repair-samples/stream-report-2-1-20.txt");
    String ref = Resources.toString(url, Charsets.UTF_8);
    assertEquals(ref.replaceAll("\\s", ""), streamSession.toString().replaceAll("\\s", ""));

    // init the stream manager
    ICassandraManagementProxy proxy = (ICassandraManagementProxy) mock(
        Class.forName("io.cassandrareaper.management.jmx.JmxCassandraManagementProxy"));

    AppContext cxt = new AppContext();
    cxt.config = TestRepairConfiguration.defaultConfig();
    cxt.managementConnectionFactory = mock(JmxManagementConnectionFactory.class);
    when(cxt.managementConnectionFactory.connectAny(Mockito.anyList())).thenReturn(proxy);
    HostConnectionCounters connectionCounters = mock(HostConnectionCounters.class);
    when(cxt.managementConnectionFactory.getHostConnectionCounters()).thenReturn(connectionCounters);
    when(connectionCounters.getSuccessfulConnections(any())).thenReturn(1);
    ClusterFacade clusterFacadeSpy = Mockito.spy(ClusterFacade.create(cxt));
    Mockito.doReturn("dc1").when(clusterFacadeSpy).getDatacenter(any(), any());
    when(proxy.getCurrentStreams()).thenReturn(ImmutableSet.of(streamSession));

    // do the actual pullStreams() call, which should succeed
    List<StreamSession> result = StreamService
        .create(() -> clusterFacadeSpy)
        .listStreams(Node.builder().withHostname("127.0.0.1").withCluster(Cluster.builder().withJmxPort(7199)
            .withSeedHosts(ImmutableSet.of("127.0.0.1")).withName("test").build()).build());

    verify(proxy, times(1)).getCurrentStreams();
    assertEquals(1, result.size());
  }

  @Test
  public void testGetStreams_2_2_12()
      throws IOException, ReaperException, OpenDataException, ClassNotFoundException, InterruptedException {

    // fake the response a 2.2.12 would return when asked for streams
    CompositeData streamSession = makeCompositeData_2_2_12();

    // compare the test payload with an actual payload grabbed from a 2.1.20 ccm node
    URL url = Resources.getResource("repair-samples/stream-report-2-2-12.txt");
    String ref = Resources.toString(url, Charsets.UTF_8);
    assertEquals(ref.replaceAll("\\s", ""), streamSession.toString().replaceAll("\\s", ""));

    // init the stream manager
    ICassandraManagementProxy proxy = (ICassandraManagementProxy) mock(
        Class.forName("io.cassandrareaper.management.jmx.JmxCassandraManagementProxy"));

    AppContext cxt = new AppContext();
    cxt.config = TestRepairConfiguration.defaultConfig();
    cxt.managementConnectionFactory = mock(JmxManagementConnectionFactory.class);
    when(cxt.managementConnectionFactory.connectAny(Mockito.anyList())).thenReturn(proxy);
    HostConnectionCounters connectionCounters = mock(HostConnectionCounters.class);
    when(cxt.managementConnectionFactory.getHostConnectionCounters()).thenReturn(connectionCounters);
    when(connectionCounters.getSuccessfulConnections(any())).thenReturn(1);
    ClusterFacade clusterFacadeSpy = Mockito.spy(ClusterFacade.create(cxt));
    Mockito.doReturn("dc1").when(clusterFacadeSpy).getDatacenter(any(), any());
    when(proxy.getCurrentStreams()).thenReturn(ImmutableSet.of(streamSession));

    // do the actual pullStreams() call, which should succeed
    List<StreamSession> result = StreamService
        .create(() -> clusterFacadeSpy)
        .listStreams(Node.builder().withHostname("127.0.0.1").withCluster(Cluster.builder().withJmxPort(7199)
            .withSeedHosts(ImmutableSet.of("127.0.0.1")).withName("test").build()).build());

    verify(proxy, times(1)).getCurrentStreams();
    assertEquals(1, result.size());
  }

  @Test
  public void testGetStreams_3_11_2()
      throws OpenDataException, IOException, ReaperException, ClassNotFoundException, InterruptedException {

    // fake the response a 2.2.12 would return when asked for streams
    CompositeData streamSession = makeCompositeData_3_11_2();

    // compare the test payload with an actual payload grabbed from a 2.1.20 ccm node
    URL url = Resources.getResource("repair-samples/stream-report-3-11-2.txt");
    String ref = Resources.toString(url, Charsets.UTF_8);
    assertEquals(ref.replaceAll("\\s", ""), streamSession.toString().replaceAll("\\s", ""));

    // init the stream manager
    ICassandraManagementProxy proxy = (ICassandraManagementProxy) mock(
        Class.forName("io.cassandrareaper.management.jmx.JmxCassandraManagementProxy"));

    AppContext cxt = new AppContext();
    cxt.config = TestRepairConfiguration.defaultConfig();
    cxt.managementConnectionFactory = mock(JmxManagementConnectionFactory.class);
    when(cxt.managementConnectionFactory.connectAny(Mockito.anyList())).thenReturn(proxy);
    HostConnectionCounters connectionCounters = mock(HostConnectionCounters.class);
    when(cxt.managementConnectionFactory.getHostConnectionCounters()).thenReturn(connectionCounters);
    when(connectionCounters.getSuccessfulConnections(any())).thenReturn(1);
    ClusterFacade clusterFacadeSpy = Mockito.spy(ClusterFacade.create(cxt));
    Mockito.doReturn("dc1").when(clusterFacadeSpy).getDatacenter(any(), any());
    when(proxy.getCurrentStreams()).thenReturn(ImmutableSet.of(streamSession));

    // do the actual pullStreams() call, which should succeed
    List<StreamSession> result = StreamService
        .create(() -> clusterFacadeSpy)
        .listStreams(Node.builder().withHostname("127.0.0.1").withCluster(Cluster.builder().withJmxPort(7199)
            .withSeedHosts(ImmutableSet.of("127.0.0.1")).withName("test").build()).build());

    verify(proxy, times(1)).getCurrentStreams();
    assertEquals(1, result.size());
  }

  @Test
  public void testGetStreams_4_0_0()
      throws OpenDataException, IOException, ClassNotFoundException, ReaperException, InterruptedException {

    CompositeData streamSession = makeCompositeData_4_0_0();

    URL url = Resources.getResource("repair-samples/stream-report-4-0-0.txt");
    String ref = Resources.toString(url, Charsets.UTF_8);
    assertEquals(ref.replaceAll("\\s", ""), streamSession.toString().replaceAll("\\s", ""));

    ICassandraManagementProxy proxy = (ICassandraManagementProxy) mock(
        Class.forName("io.cassandrareaper.management.jmx.JmxCassandraManagementProxy"));

    AppContext cxt = new AppContext();
    cxt.config = TestRepairConfiguration.defaultConfig();
    cxt.managementConnectionFactory = mock(JmxManagementConnectionFactory.class);
    when(cxt.managementConnectionFactory.connectAny(Mockito.anyList())).thenReturn(proxy);
    HostConnectionCounters connectionCounters = mock(HostConnectionCounters.class);
    when(cxt.managementConnectionFactory.getHostConnectionCounters()).thenReturn(connectionCounters);
    when(connectionCounters.getSuccessfulConnections(any())).thenReturn(1);
    ClusterFacade clusterFacadeSpy = Mockito.spy(ClusterFacade.create(cxt));
    Mockito.doReturn("dc1").when(clusterFacadeSpy).getDatacenter(any(), any());

    when(proxy.getCurrentStreams()).thenReturn(ImmutableSet.of(streamSession));

    // do the actual pullStreams() call, which should succeed
    List<StreamSession> result = StreamService
        .create(() -> clusterFacadeSpy)
        .listStreams(Node.builder().withHostname("127.0.0.1").withCluster(Cluster.builder().withJmxPort(7199)
            .withSeedHosts(ImmutableSet.of("127.0.0.1")).withName("test").build()).build());

    verify(proxy, times(1)).getCurrentStreams();
    assertEquals(1, result.size());

  }

  private CompositeData makeCompositeData_4_0_0() throws OpenDataException {

    Map<String, Object> fields = Maps.newTreeMap();
    fields.put("currentRxBytes", 9947684L);
    fields.put("currentTxBytes", 0L);
    fields.put("description", "Repair");
    fields.put("planId", "185b0440-e059-11e9-8be8-4db088857399");
    fields.put("rxPercentage", 12.0);
    fields.put("sessions", new CompositeData[]{makeSessions_4_0_0()});
    fields.put("totalRxBytes", 79807773L);
    fields.put("totalTxBytes", 0L);
    fields.put("txPercentage", 100.0);

    CompositeType compositeType = streamStateType_4_0_0();

    return new CompositeDataSupport(compositeType, fields);
  }

  private CompositeData makeCompositeData_3_11_2() throws OpenDataException {

    Map<String, Object> fields = Maps.newTreeMap();
    fields.put("currentRxBytes", 0L);
    fields.put("currentTxBytes", 10996145L);
    fields.put("description", "Repair");
    fields.put("planId", "7805a580-8038-11e8-adb7-4751e6155332");
    fields.put("rxPercentage", 100.0);
    fields.put("sessions", new CompositeData[]{makeSessions_3_11_2()});
    fields.put("totalRxBytes", 0L);
    fields.put("totalTxBytes", 130972860L);
    fields.put("txPercentage", 8.0);

    // C* 3.11.2 uses the same stream notification format like 2.1.10 and 2.2.12
    CompositeType compositeType = streamStateType_2_1_20();

    return new CompositeDataSupport(compositeType, fields);
  }

  private CompositeDataSupport makeCompositeData_2_2_12() throws OpenDataException {

    Map<String, Object> fields = Maps.newTreeMap();
    fields.put("currentRxBytes", 0L);
    fields.put("currentTxBytes", 59875L);
    fields.put("description", "Repair");
    fields.put("planId", "636b1490-7d39-11e8-a943-973315b0e477");
    fields.put("rxPercentage", 100.0);
    fields.put("sessions", new CompositeData[]{makeSessions_2_2_12()});
    fields.put("totalRxBytes", 0L);
    fields.put("totalTxBytes", 44670289L);
    fields.put("txPercentage", 0.0);

    // C* 2.2.12 uses the same stream notification format like 2.1.10
    CompositeType compositeType = streamStateType_2_1_20();

    return new CompositeDataSupport(compositeType, fields);
  }

  private CompositeDataSupport makeCompositeData_2_1_20() throws OpenDataException {

    Map<String, Object> fields = Maps.newTreeMap();
    fields.put("currentRxBytes", 123921L);
    fields.put("currentTxBytes", 1313113L);
    fields.put("description", "Repair");
    fields.put("planId", "91ed3651-7d2e-11e8-b38d-812f8df1fba1");
    fields.put("rxPercentage", 0.0);
    fields.put("sessions", new CompositeData[]{makeSessions_2_1_20()});
    fields.put("totalRxBytes", 59036414L);
    fields.put("totalTxBytes", 53556352L);
    fields.put("txPercentage", 2.0);

    CompositeType compositeType = streamStateType_2_1_20();

    return new CompositeDataSupport(compositeType, fields);
  }

  private CompositeDataSupport makeCompositeData_2_0_17() throws OpenDataException {

    Map<String, Object> fields = Maps.newTreeMap();
    fields.put("currentRxBytes", 2133928L);
    fields.put("currentTxBytes", 0L);
    fields.put("description", "Repair");
    fields.put("planId", "059686e0-796c-11e8-bd35-f598c5b775dd");
    fields.put("rxPercentage", 2.0);
    fields.put("sessions", new CompositeData[]{makeSessions_2_0_17()});
    fields.put("totalRxBytes", 90835388L);
    fields.put("totalTxBytes", 0L);
    fields.put("txPercentage", 100.0);

    CompositeType compositeType = streamStateType_2_0_17();

    return new CompositeDataSupport(compositeType, fields);
  }

  private CompositeType streamStateType_4_0_0() throws OpenDataException {
    String typeName = "org.apache.cassandra.streaming.StreamState";
    String description = "StreamState";
    String[] itemNames = {
        "currentRxBytes",
        "currentTxBytes",
        "description",
        "planId",
        "rxPercentage",
        "sessions",
        "totalRxBytes",
        "totalTxBytes",
        "txPercentage"
    };
    String[] itemDescriptions = {
        "currentRxBytes",
        "currentTxBytes",
        "description",
        "planId",
        "rxPercentage",
        "sessions",
        "totalRxBytes",
        "totalTxBytes",
        "txPercentage"
    };
    OpenType[] itemTypes = {
        SimpleType.LONG,
        SimpleType.LONG,
        SimpleType.STRING,
        SimpleType.STRING,
        SimpleType.DOUBLE,
        ArrayType.getArrayType(makeSessionsType4_0()),
        SimpleType.LONG,
        SimpleType.LONG,
        SimpleType.DOUBLE
    };

    return new CompositeType(typeName, description, itemNames, itemDescriptions, itemTypes);
  }

  private CompositeType streamStateType_2_1_20() throws OpenDataException {
    String typeName = "org.apache.cassandra.streaming.StreamState";
    String description = "StreamState";
    String[] itemNames = {
        "currentRxBytes",
        "currentTxBytes",
        "description",
        "planId",
        "rxPercentage",
        "sessions",
        "totalRxBytes",
        "totalTxBytes",
        "txPercentage"
    };
    String[] itemDescriptions = {
        "currentRxBytes",
        "currentTxBytes",
        "description",
        "planId",
        "rxPercentage",
        "sessions",
        "totalRxBytes",
        "totalTxBytes",
        "txPercentage"
    };

    OpenType[] itemTypes = {
        SimpleType.LONG,
        SimpleType.LONG,
        SimpleType.STRING,
        SimpleType.STRING,
        SimpleType.DOUBLE,
        ArrayType.getArrayType(makeSessionsTypePost2_1()),
        SimpleType.LONG,
        SimpleType.LONG,
        SimpleType.DOUBLE
    };

    return new CompositeType(typeName, description, itemNames, itemDescriptions, itemTypes);
  }

  private CompositeType streamStateType_2_0_17() throws OpenDataException {
    String typeName = "org.apache.cassandra.streaming.StreamState";
    String description = "StreamState";
    String[] itemNames = {
        "currentRxBytes",
        "currentTxBytes",
        "description",
        "planId",
        "rxPercentage",
        "sessions",
        "totalRxBytes",
        "totalTxBytes",
        "txPercentage"
    };
    String[] itemDescriptions = {
        "currentRxBytes",
        "currentTxBytes",
        "description",
        "planId",
        "rxPercentage",
        "sessions",
        "totalRxBytes",
        "totalTxBytes",
        "txPercentage"
    };
    OpenType[] itemTypes = {
        SimpleType.LONG,
        SimpleType.LONG,
        SimpleType.STRING,
        SimpleType.STRING,
        SimpleType.DOUBLE,
        ArrayType.getArrayType(makeSessionsTypePre2_1()),
        SimpleType.LONG,
        SimpleType.LONG,
        SimpleType.DOUBLE
    };

    return new CompositeType(typeName, description, itemNames, itemDescriptions, itemTypes);
  }

  private CompositeDataSupport makeSessions_4_0_0() throws OpenDataException {
    Map<String, Object> receivingFile = Maps.newTreeMap();
    receivingFile.put("currentBytes", 9947684L);
    receivingFile.put("direction", "IN");
    receivingFile.put("fileName", "tlp_stress/sensor_data");
    receivingFile.put("peer", "127.0.0.1");
    receivingFile.put("peer storage port", 7000);
    receivingFile.put("planId", "185b0440-e059-11e9-8be8-4db088857399");
    receivingFile.put("sessionIndex", 0);
    receivingFile.put("totalBytes", 71367085L);

    Map<String, Object> receivingSummary = Maps.newTreeMap();
    receivingSummary.put("files", 1);
    receivingSummary.put("tableId", "3d973a30-e04f-11e9-ae1b-71c66a07a6c0");
    receivingSummary.put("totalSize", 79807773L);

    CompositeDataSupport[] receivingFiles = {
        new CompositeDataSupport(makeFilesType_4_0_0(), receivingFile)
    };

    CompositeDataSupport[] receivingSummaries = {
        new CompositeDataSupport(makeSummariesType_4_0_0(), receivingSummary)
    };

    CompositeDataSupport[] sendingFiles = new CompositeDataSupport[]{};
    CompositeDataSupport[] sendingSummaries = new CompositeDataSupport[]{};

    Map<String, Object> fields = Maps.newTreeMap();
    fields.put("connecting", "127.0.0.1");
    fields.put("connecting_port", 7000);
    fields.put("peer", "127.0.0.1");
    fields.put("peer_port", 7000);
    fields.put("planId", "185b0440-e059-11e9-8be8-4db088857399");
    fields.put("receivingFiles", receivingFiles);
    fields.put("receivingSummaries", receivingSummaries);
    fields.put("sendingFiles", sendingFiles);
    fields.put("sendingSummaries", sendingSummaries);
    fields.put("sessionIndex", 0);
    fields.put("state", "PREPARING");

    return new CompositeDataSupport(makeSessionsType4_0(), fields);
  }

  private CompositeDataSupport makeSessions_3_11_2() throws OpenDataException {

    Map<String, Object> firstSendingFile = Maps.newTreeMap();
    firstSendingFile.put("currentBytes", 8847360L);
    firstSendingFile.put("direction", "OUT");
    firstSendingFile.put(
        "fileName",
        ".ccm/3-11-2/node1/data0/keyspace1/standard1-88dcef50803611e88422d97f789df049/mc-9-big-Data.db");
    firstSendingFile.put("peer", "127.0.0.2");
    firstSendingFile.put("planId", "7805a580-8038-11e8-adb7-4751e6155332");
    firstSendingFile.put("sessionIndex", 0);
    firstSendingFile.put("totalBytes", 70654225L);

    Map<String, Object> secondSendingFile = Maps.newTreeMap();
    secondSendingFile.put("currentBytes", 2148785L);
    secondSendingFile.put("direction", "OUT");
    secondSendingFile.put(
        "fileName",
        ".ccm/3-11-2/node1/data0/keyspace1/standard1-88dcef50803611e88422d97f789df049/mc-25-big-Data.db");
    secondSendingFile.put("peer", "127.0.0.2");
    secondSendingFile.put("planId", "7805a580-8038-11e8-adb7-4751e6155332");
    secondSendingFile.put("sessionIndex", 0);
    secondSendingFile.put("totalBytes", 2148785L);

    Map<String, Object> fields = Maps.newTreeMap();

    CompositeDataSupport[] receivingFiles = new CompositeDataSupport[]{};
    CompositeDataSupport[] receivingSummaries = new CompositeDataSupport[]{};
    fields.put("receivingFiles", receivingFiles);
    fields.put("receivingSummaries", receivingSummaries);

    CompositeDataSupport[] sendingFiles = {
        new CompositeDataSupport(makeFilesType_2_1_20(), firstSendingFile),
        new CompositeDataSupport(makeFilesType_2_1_20(), secondSendingFile)
    };
    fields.put("sendingFiles", sendingFiles);

    Map<String, Object> sendingSummariesFields = Maps.newTreeMap();
    sendingSummariesFields.put("cfId", "88dcef50-8036-11e8-8422-d97f789df049");
    sendingSummariesFields.put("files", 4);
    sendingSummariesFields.put("totalSize", 130972860L);
    CompositeDataSupport[] sendingSummaries = {
        new CompositeDataSupport(makeSummariesType_pre_4_0(), sendingSummariesFields)
    };
    fields.put("sendingSummaries", sendingSummaries);

    fields.put("connecting", "127.0.0.1");
    fields.put("peer", "127.0.0.2");
    fields.put("planId", "7805a580-8038-11e8-adb7-4751e6155332");
    fields.put("sessionIndex", 0);
    fields.put("state", "PREPARING");

    return new CompositeDataSupport(makeSessionsTypePost2_1(), fields);
  }

  private CompositeDataSupport makeSessions_2_2_12() throws OpenDataException {

    Map<String, Object> sendingFilesFields = Maps.newTreeMap();
    sendingFilesFields.put("currentBytes", 59875L);
    sendingFilesFields.put("direction", "OUT");
    sendingFilesFields.put(
        "fileName",
        ".ccm/2-2-12/node1/data0/keyspace1/standard1-81a53a907d3811e8ac4a093b83cb36bf/lb-9-big-Data.db");
    sendingFilesFields.put("peer", "127.0.0.2");
    sendingFilesFields.put("planId", "636b1490-7d39-11e8-a943-973315b0e477");
    sendingFilesFields.put("sessionIndex", 0);
    sendingFilesFields.put("totalBytes", 23361497L);
    CompositeDataSupport[] sendingFiles = {
        new CompositeDataSupport(makeFilesType_2_1_20(), sendingFilesFields)
    };

    Map<String, Object> fields = Maps.newTreeMap();
    fields.put("sendingFiles", sendingFiles);

    Map<String, Object> sendingSummariesFields = Maps.newTreeMap();
    sendingSummariesFields.put("cfId", "81a53a90-7d38-11e8-ac4a-093b83cb36bf");
    sendingSummariesFields.put("files", 2);
    sendingSummariesFields.put("totalSize", 44670289L);
    CompositeDataSupport[] sendingSummaries = {
        new CompositeDataSupport(makeSummariesType_pre_4_0(), sendingSummariesFields)
    };
    fields.put("sendingSummaries", sendingSummaries);

    CompositeDataSupport[] receivingFiles = new CompositeDataSupport[]{};
    CompositeDataSupport[] receivingSummaries = new CompositeDataSupport[]{};
    fields.put("receivingFiles", receivingFiles);
    fields.put("receivingSummaries", receivingSummaries);

    fields.put("connecting", "127.0.0.2");
    fields.put("peer", "127.0.0.2");
    fields.put("planId", "636b1490-7d39-11e8-a943-973315b0e477");
    fields.put("sessionIndex", 0);
    fields.put("state", "PREPARING");

    return new CompositeDataSupport(makeSessionsTypePost2_1(), fields);
  }

  private CompositeDataSupport makeSessions_2_1_20() throws OpenDataException {
    Map<String, Object> receivingFilesFields = Maps.newTreeMap();
    receivingFilesFields.put("currentBytes", 123921L);
    receivingFilesFields.put("direction", "IN");
    receivingFilesFields.put(
        "fileName",
        ".ccm/2-1-20/node1/data0/keyspace1/standard1-2fba77f07d2d11e8ad4d812f8df1fba1/"
            + "keyspace1-standard1-tmp-ka-24-Data.db");
    receivingFilesFields.put("peer", "127.0.0.4");
    receivingFilesFields.put("planId", "91ed3651-7d2e-11e8-b38d-812f8df1fba1");
    receivingFilesFields.put("sessionIndex", 0);
    receivingFilesFields.put("totalBytes", 1511780L);
    CompositeDataSupport[] receivingFiles = {
        new CompositeDataSupport(makeFilesType_2_1_20(), receivingFilesFields)
    };

    Map<String, Object> fields = Maps.newTreeMap();
    fields.put("receivingFiles", receivingFiles);

    Map<String, Object> receivingSummariesFields = Maps.newTreeMap();
    receivingSummariesFields.put("cfId", "2fba77f0-7d2d-11e8-ad4d-812f8df1fba1");
    receivingSummariesFields.put("files", 4);
    receivingSummariesFields.put("totalSize", 59036414L);
    CompositeDataSupport[] receivingSummaries = {
        new CompositeDataSupport(makeSummariesType_pre_4_0(), receivingSummariesFields)
    };
    fields.put("receivingSummaries", receivingSummaries);

    Map<String, Object> sendingFilesFields = Maps.newTreeMap();
    sendingFilesFields.put("currentBytes", 1313113L);
    sendingFilesFields.put("direction", "OUT");
    sendingFilesFields.put(
        "fileName",
        ".ccm/2-1-20/node1/data0/keyspace1/standard1-2fba77f07d2d11e8ad4d812f8df1fba1/"
            + "keyspace1-standard1-ka-23-Data.db");
    sendingFilesFields.put("peer", "127.0.0.4");
    sendingFilesFields.put("planId", "91ed3651-7d2e-11e8-b38d-812f8df1fba1");
    sendingFilesFields.put("sessionIndex", 0);
    sendingFilesFields.put("totalBytes", 4012961L);
    CompositeDataSupport[] sendingFiles = {
        new CompositeDataSupport(makeFilesType_2_1_20(), sendingFilesFields)
    };
    fields.put("sendingFiles", sendingFiles);

    Map<String, Object> sendingSummariesFields = Maps.newTreeMap();
    sendingSummariesFields.put("cfId", "2fba77f0-7d2d-11e8-ad4d-812f8df1fba1");
    sendingSummariesFields.put("files", 2);
    sendingSummariesFields.put("totalSize", 53556352L);
    CompositeDataSupport[] sendingSummaries = {
        new CompositeDataSupport(makeSummariesType_pre_4_0(), sendingSummariesFields)
    };
    fields.put("sendingSummaries", sendingSummaries);

    fields.put("connecting", "127.0.0.4");
    fields.put("peer", "127.0.0.4");
    fields.put("planId", "91ed3651-7d2e-11e8-b38d-812f8df1fba1");
    fields.put("sessionIndex", 0);
    fields.put("state", "PREPARING");

    return new CompositeDataSupport(makeSessionsTypePost2_1(), fields);
  }

  private CompositeDataSupport makeSessions_2_0_17() throws OpenDataException {

    Map<String, Object> receivingFilesFields = Maps.newTreeMap();
    receivingFilesFields.put("currentBytes", 2133094L);
    receivingFilesFields.put("direction", "IN");
    receivingFilesFields.put(
        "fileName",
        ".ccm/2-0-17/node1/data0/Keyspace1/Standard1/Keyspace1-Standard1-tmp-jb-1-Data.db"
    );
    receivingFilesFields.put("peer", "127.0.0.2");
    receivingFilesFields.put("planId", "059686e0-796c-11e8-bd35-f598c5b775dd");
    receivingFilesFields.put("totalBytes", 55603614L);
    CompositeDataSupport[] receivingFiles = {
        new CompositeDataSupport(makeFilesType_2_0_17(), receivingFilesFields)
    };

    Map<String, Object> fields = Maps.newTreeMap();
    fields.put("receivingFiles", receivingFiles);

    Map<String, Object> receivingSummariesFields = Maps.newTreeMap();
    receivingSummariesFields.put("cfId", "4d331c44-f018-302b-91c2-2dcf94c4bfad");
    receivingSummariesFields.put("files", 3);
    receivingSummariesFields.put("totalSize", 90835388L);
    CompositeDataSupport[] receivingSummaries = {
        new CompositeDataSupport(makeSummariesType_pre_4_0(), receivingSummariesFields)
    };
    fields.put("receivingSummaries", receivingSummaries);

    CompositeDataSupport[] sendingFiles = new CompositeDataSupport[]{};
    CompositeDataSupport[] sendingSummaries = new CompositeDataSupport[]{};
    fields.put("sendingFiles", sendingFiles);
    fields.put("sendingSummaries", sendingSummaries);

    fields.put("connecting", "127.0.0.2");
    fields.put("peer", "127.0.0.2");
    fields.put("planId", "059686e0-796c-11e8-bd35-f598c5b775dd");
    fields.put("state", "PREPARING");

    return new CompositeDataSupport(makeSessionsTypePre2_1(), fields);
  }

  private CompositeType makeSessionsType4_0() throws OpenDataException {
    String typeName = "org.apache.cassandra.streaming.SessionInfo";
    String description = "SessionInfo";
    String[] itemNames = {
        "connecting",
        "connecting_port",
        "peer",
        "peer_port",
        "planId",
        "receivingFiles",
        "receivingSummaries",
        "sendingFiles",
        "sendingSummaries",
        "sessionIndex",
        "state"
    };
    String[] itemDescriptions = {
        "connecting",
        "connecting_port",
        "peer",
        "peer_port",
        "planId",
        "receivingFiles",
        "receivingSummaries",
        "sendingFiles",
        "sendingSummaries",
        "sessionIndex",
        "state"
    };
    OpenType[] itemTypes = {
        SimpleType.STRING,
        SimpleType.INTEGER,
        SimpleType.STRING,
        SimpleType.INTEGER,
        SimpleType.STRING,
        ArrayType.getArrayType(makeFilesType_4_0_0()),
        ArrayType.getArrayType(makeSummariesType_4_0_0()),
        ArrayType.getArrayType(makeFilesType_4_0_0()),
        ArrayType.getArrayType(makeSummariesType_4_0_0()),
        SimpleType.INTEGER,
        SimpleType.STRING,
    };

    return new CompositeType(typeName, description, itemNames, itemDescriptions, itemTypes);
  }

  private CompositeType makeSessionsTypePost2_1() throws OpenDataException {
    String typeName = "org.apache.cassandra.streaming.SessionInfo";
    String description = "SessionInfo";
    String[] itemNames = {
        "connecting",
        "peer",
        "planId",
        "receivingFiles",
        "receivingSummaries",
        "sendingFiles",
        "sendingSummaries",
        "sessionIndex",
        "state"
    };
    String[] itemDescriptions = {
        "connecting",
        "peer",
        "planId",
        "receivingFiles",
        "receivingSummaries",
        "sendingFiles",
        "sendingSummaries",
        "sessionIndex",
        "state"
    };
    OpenType[] itemTypes = {
        SimpleType.STRING,
        SimpleType.STRING,
        SimpleType.STRING,
        ArrayType.getArrayType(makeFilesType_2_1_20()),
        ArrayType.getArrayType(makeSummariesType_pre_4_0()),
        ArrayType.getArrayType(makeFilesType_2_1_20()),
        ArrayType.getArrayType(makeSummariesType_pre_4_0()),
        SimpleType.INTEGER,
        SimpleType.STRING,
    };

    return new CompositeType(typeName, description, itemNames, itemDescriptions, itemTypes);

  }

  private CompositeType makeSessionsTypePre2_1() throws OpenDataException {
    String typeName = "org.apache.cassandra.streaming.SessionInfo";
    String description = "SessionInfo";
    String[] itemNames = {
        "connecting",
        "peer",
        "planId",
        "receivingFiles",
        "receivingSummaries",
        "sendingFiles",
        "sendingSummaries",
        "state"
    };
    String[] itemDescriptions = {
        "connecting",
        "peer",
        "planId",
        "receivingFiles",
        "receivingSummaries",
        "sendingFiles",
        "sendingSummaries",
        "state"
    };
    OpenType[] itemTypes = {
        SimpleType.STRING,
        SimpleType.STRING,
        SimpleType.STRING,
        ArrayType.getArrayType(makeFilesType_2_0_17()),
        ArrayType.getArrayType(makeSummariesType_pre_4_0()),
        ArrayType.getArrayType(makeFilesType_2_0_17()),
        ArrayType.getArrayType(makeSummariesType_pre_4_0()),
        SimpleType.STRING,
    };

    return new CompositeType(typeName, description, itemNames, itemDescriptions, itemTypes);
  }

  private CompositeType makeFilesType_4_0_0() throws OpenDataException {
    String typeName = "org.apache.cassandra.streaming.ProgressInfo";
    String description = "ProgressInfo";
    String[] itemNames = {
        "currentBytes",
        "direction",
        "fileName",
        "peer",
        "peer storage port",
        "planId",
        "sessionIndex",
        "totalBytes"
    };
    String[] itemDescriptions = {
        "currentBytes",
        "direction",
        "fileName",
        "peer",
        "peer storage port",
        "planId",
        "sessionIndex",
        "totalBytes"
    };
    OpenType[] itemTypes = {
        SimpleType.LONG,
        SimpleType.STRING,
        SimpleType.STRING,
        SimpleType.STRING,
        SimpleType.INTEGER,
        SimpleType.STRING,
        SimpleType.INTEGER,
        SimpleType.LONG,
    };

    return new CompositeType(typeName, description, itemNames, itemDescriptions, itemTypes);
  }

  private CompositeType makeFilesType_2_1_20() throws OpenDataException {
    String typeName = "org.apache.cassandra.streaming.ProgressInfo";
    String description = "ProgressInfo";
    String[] itemNames = {
        "currentBytes",
        "direction",
        "fileName",
        "peer",
        "planId",
        "sessionIndex",
        "totalBytes"
    };
    String[] itemDescriptions = {
        "currentBytes",
        "direction",
        "fileName",
        "peer",
        "planId",
        "sessionIndex",
        "totalBytes"
    };
    OpenType[] itemTypes = {
        SimpleType.LONG,
        SimpleType.STRING,
        SimpleType.STRING,
        SimpleType.STRING,
        SimpleType.STRING,
        SimpleType.INTEGER,
        SimpleType.LONG,
    };

    return new CompositeType(typeName, description, itemNames, itemDescriptions, itemTypes);
  }

  private CompositeType makeFilesType_2_0_17() throws OpenDataException {
    String typeName = "org.apache.cassandra.streaming.ProgressInfo";
    String description = "ProgressInfo";
    String[] itemNames = {
        "currentBytes",
        "direction",
        "fileName",
        "peer",
        "planId",
        "totalBytes"
    };
    String[] itemDescriptions = {
        "currentBytes",
        "direction",
        "fileName",
        "peer",
        "planId",
        "totalBytes"
    };
    OpenType[] itemTypes = {
        SimpleType.LONG,
        SimpleType.STRING,
        SimpleType.STRING,
        SimpleType.STRING,
        SimpleType.STRING,
        SimpleType.LONG,
    };

    return new CompositeType(typeName, description, itemNames, itemDescriptions, itemTypes);
  }

  private CompositeType makeSummariesType_4_0_0() throws OpenDataException {
    String typeName = "org.apache.cassandra.streaming.StreamSummary";
    String description = "StreamSummary";
    String[] itemNames = {
        "tableId",
        "files",
        "totalSize"
    };
    String[] itemDescriptions = {
        "tableId",
        "files",
        "totalSize"
    };
    OpenType[] itemTypes = {
        SimpleType.STRING,
        SimpleType.INTEGER,
        SimpleType.LONG
    };

    return new CompositeType(typeName, description, itemNames, itemDescriptions, itemTypes);
  }

  private CompositeType makeSummariesType_pre_4_0() throws OpenDataException {
    String typeName = "org.apache.cassandra.streaming.StreamSummary";
    String description = "StreamSummary";
    String[] itemNames = {
        "cfId",
        "files",
        "totalSize"
    };
    String[] itemDescriptions = {
        "cfId",
        "files",
        "totalSize"
    };
    OpenType[] itemTypes = {
        SimpleType.STRING,
        SimpleType.INTEGER,
        SimpleType.LONG
    };

    return new CompositeType(typeName, description, itemNames, itemDescriptions, itemTypes);
  }

}