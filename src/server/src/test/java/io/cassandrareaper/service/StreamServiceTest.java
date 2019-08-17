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
import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.StreamSession;
import io.cassandrareaper.jmx.ClusterFacade;
import io.cassandrareaper.jmx.JmxConnectionFactory;
import io.cassandrareaper.jmx.JmxProxy;
import io.cassandrareaper.jmx.JmxProxyTest;

import java.io.IOException;
import java.net.URL;
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
import org.apache.cassandra.streaming.StreamManagerMBean;
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
  public void testListStreams() throws ReaperException, ClassNotFoundException, InterruptedException {
    JmxProxy proxy = JmxProxyTest.mockJmxProxyImpl();
    StreamManagerMBean streamingManagerMBean = Mockito.mock(StreamManagerMBean.class);
    JmxProxyTest.mockGetStreamManagerMBean(proxy, streamingManagerMBean);

    AppContext cxt = new AppContext();
    cxt.config = TestRepairConfiguration.defaultConfig();
    cxt.jmxConnectionFactory = mock(JmxConnectionFactory.class);
    when(cxt.jmxConnectionFactory.connectAny(Mockito.anyList())).thenReturn(proxy);
    ClusterFacade clusterFacadeSpy = Mockito.spy(ClusterFacade.create(cxt));
    Mockito.doReturn("dc1").when(clusterFacadeSpy).getDatacenter(any());

    StreamService
        .create(() -> clusterFacadeSpy)
        .listStreams(Node.builder().withHostname("127.0.0.1").build());

    verify(streamingManagerMBean, times(1)).getCurrentStreams();
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
    JmxProxy proxy = (JmxProxy) mock(Class.forName("io.cassandrareaper.jmx.JmxProxyImpl"));
    StreamManagerMBean streamingManagerMBean = Mockito.mock(StreamManagerMBean.class);
    JmxProxyTest.mockGetStreamManagerMBean(proxy, streamingManagerMBean);
    when(streamingManagerMBean.getCurrentStreams()).thenReturn(ImmutableSet.of(streamSession));

    AppContext cxt = new AppContext();
    cxt.config = TestRepairConfiguration.defaultConfig();
    cxt.jmxConnectionFactory = mock(JmxConnectionFactory.class);
    when(cxt.jmxConnectionFactory.connectAny(Mockito.anyList())).thenReturn(proxy);
    ClusterFacade clusterFacadeSpy = Mockito.spy(ClusterFacade.create(cxt));
    Mockito.doReturn("dc1").when(clusterFacadeSpy).getDatacenter(any());

    // do the actual pullStreams() call, which should succeed
    List<StreamSession> result = StreamService
        .create(() -> clusterFacadeSpy)
        .listStreams(Node.builder().withHostname("127.0.0.1").build());

    verify(streamingManagerMBean, times(1)).getCurrentStreams();
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
    JmxProxy proxy = (JmxProxy) mock(Class.forName("io.cassandrareaper.jmx.JmxProxyImpl"));
    StreamManagerMBean streamingManagerMBean = Mockito.mock(StreamManagerMBean.class);
    JmxProxyTest.mockGetStreamManagerMBean(proxy, streamingManagerMBean);
    when(streamingManagerMBean.getCurrentStreams()).thenReturn(ImmutableSet.of(streamSession));

    AppContext cxt = new AppContext();
    cxt.config = TestRepairConfiguration.defaultConfig();
    cxt.jmxConnectionFactory = mock(JmxConnectionFactory.class);
    when(cxt.jmxConnectionFactory.connectAny(Mockito.anyList())).thenReturn(proxy);
    ClusterFacade clusterFacadeSpy = Mockito.spy(ClusterFacade.create(cxt));
    Mockito.doReturn("dc1").when(clusterFacadeSpy).getDatacenter(any());

    // do the actual pullStreams() call, which should succeed
    List<StreamSession> result = StreamService
        .create(() -> clusterFacadeSpy)
        .listStreams(Node.builder().withHostname("127.0.0.1").build());

    verify(streamingManagerMBean, times(1)).getCurrentStreams();
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
    JmxProxy proxy = (JmxProxy) mock(Class.forName("io.cassandrareaper.jmx.JmxProxyImpl"));
    StreamManagerMBean streamingManagerMBean = Mockito.mock(StreamManagerMBean.class);
    JmxProxyTest.mockGetStreamManagerMBean(proxy, streamingManagerMBean);
    when(streamingManagerMBean.getCurrentStreams()).thenReturn(ImmutableSet.of(streamSession));

    AppContext cxt = new AppContext();
    cxt.config = TestRepairConfiguration.defaultConfig();
    cxt.jmxConnectionFactory = mock(JmxConnectionFactory.class);
    when(cxt.jmxConnectionFactory.connectAny(Mockito.anyList())).thenReturn(proxy);
    ClusterFacade clusterFacadeSpy = Mockito.spy(ClusterFacade.create(cxt));
    Mockito.doReturn("dc1").when(clusterFacadeSpy).getDatacenter(any());

    // do the actual pullStreams() call, which should succeed
    List<StreamSession> result = StreamService
        .create(() -> clusterFacadeSpy)
        .listStreams(Node.builder().withHostname("127.0.0.1").build());

    verify(streamingManagerMBean, times(1)).getCurrentStreams();
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
    JmxProxy proxy = (JmxProxy) mock(Class.forName("io.cassandrareaper.jmx.JmxProxyImpl"));
    StreamManagerMBean streamingManagerMBean = Mockito.mock(StreamManagerMBean.class);
    JmxProxyTest.mockGetStreamManagerMBean(proxy, streamingManagerMBean);
    when(streamingManagerMBean.getCurrentStreams()).thenReturn(ImmutableSet.of(streamSession));

    AppContext cxt = new AppContext();
    cxt.config = TestRepairConfiguration.defaultConfig();
    cxt.jmxConnectionFactory = mock(JmxConnectionFactory.class);
    when(cxt.jmxConnectionFactory.connectAny(Mockito.anyList())).thenReturn(proxy);
    ClusterFacade clusterFacadeSpy = Mockito.spy(ClusterFacade.create(cxt));
    Mockito.doReturn("dc1").when(clusterFacadeSpy).getDatacenter(any());

    // do the actual pullStreams() call, which should succeed
    List<StreamSession> result = StreamService
        .create(() -> clusterFacadeSpy)
        .listStreams(Node.builder().withHostname("127.0.0.1").build());

    verify(streamingManagerMBean, times(1)).getCurrentStreams();
    assertEquals(1, result.size());
  }

  private CompositeData makeCompositeData_3_11_2() throws OpenDataException {

    Map<String, Object> fields = Maps.newTreeMap();
    fields.put("currentRxBytes", 0L);
    fields.put("currentTxBytes", 10996145L);
    fields.put("description", "Repair");
    fields.put("planId", "7805a580-8038-11e8-adb7-4751e6155332");
    fields.put("rxPercentage", 100.0);
    fields.put("sessions", new CompositeData[] {makeSessions_3_11_2()});
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
    fields.put("sessions", new CompositeData[] {makeSessions_2_2_12()});
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
    fields.put("sessions", new CompositeData[] {makeSessions_2_1_20()});
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
    fields.put("sessions", new CompositeData[] {makeSessions_2_0_17()});
    fields.put("totalRxBytes", 90835388L);
    fields.put("totalTxBytes", 0L);
    fields.put("txPercentage", 100.0);

    CompositeType compositeType = streamStateType_2_0_17();

    return new CompositeDataSupport(compositeType, fields);
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

    CompositeDataSupport[] receivingFiles = new CompositeDataSupport[] {};
    CompositeDataSupport[] receivingSummaries = new CompositeDataSupport[] {};
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
        new CompositeDataSupport(makeSummariesType(), sendingSummariesFields)
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
        new CompositeDataSupport(makeSummariesType(), sendingSummariesFields)
    };
    fields.put("sendingSummaries", sendingSummaries);

    CompositeDataSupport[] receivingFiles = new CompositeDataSupport[] {};
    CompositeDataSupport[] receivingSummaries = new CompositeDataSupport[] {};
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
        new CompositeDataSupport(makeSummariesType(), receivingSummariesFields)
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
        new CompositeDataSupport(makeSummariesType(), sendingSummariesFields)
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
        new CompositeDataSupport(makeSummariesType(), receivingSummariesFields)
    };
    fields.put("receivingSummaries", receivingSummaries);

    CompositeDataSupport[] sendingFiles = new CompositeDataSupport[] {} ;
    CompositeDataSupport[] sendingSummaries = new CompositeDataSupport[] {} ;
    fields.put("sendingFiles", sendingFiles);
    fields.put("sendingSummaries", sendingSummaries);

    fields.put("connecting", "127.0.0.2");
    fields.put("peer", "127.0.0.2");
    fields.put("planId", "059686e0-796c-11e8-bd35-f598c5b775dd");
    fields.put("state", "PREPARING");

    return new CompositeDataSupport(makeSessionsTypePre2_1(), fields);
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
        ArrayType.getArrayType(makeSummariesType()),
        ArrayType.getArrayType(makeFilesType_2_1_20()),
        ArrayType.getArrayType(makeSummariesType()),
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
        ArrayType.getArrayType(makeSummariesType()),
        ArrayType.getArrayType(makeFilesType_2_0_17()),
        ArrayType.getArrayType(makeSummariesType()),
        SimpleType.STRING,
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

  private CompositeType makeSummariesType() throws OpenDataException {
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
