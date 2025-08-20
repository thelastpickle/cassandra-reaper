package io.cassandrareaper.management.jmx;

import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.RepairType;
import io.cassandrareaper.management.RepairStatusHandler;
import io.cassandrareaper.service.RingRange;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.gms.FailureDetectorMBean;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.streaming.StreamManagerMBean;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class JmxCassandraManagementProxySimpleTest {

  @Mock private JMXConnector mockJmxConnector;
  @Mock private MBeanServerConnection mockMBeanServer;
  @Mock private StorageServiceMBean mockStorageService;
  @Mock private CompactionManagerMBean mockCompactionManager;
  @Mock private EndpointSnitchInfoMBean mockEndpointSnitch;
  @Mock private FailureDetectorMBean mockFailureDetector;
  @Mock private StreamManagerMBean mockStreamManager;
  @Mock private DiagnosticEventPersistenceMBean mockDiagEventProxy;
  @Mock private LastEventIdBroadcasterMBean mockLastEventIdProxy;
  @Mock private MetricRegistry mockMetricRegistry;
  @Mock private RepairStatusHandler mockRepairStatusHandler;

  private JmxCassandraManagementProxy proxy;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);

    // Setup basic mocks
    when(mockStorageService.getClusterName()).thenReturn("test-cluster");
    when(mockStorageService.getReleaseVersion()).thenReturn("3.11.0");
    when(mockJmxConnector.getConnectionId()).thenReturn("test-connection-123");
    when(mockJmxConnector.getMBeanServerConnection()).thenReturn(mockMBeanServer);
    when(mockMetricRegistry.getGauges()).thenReturn(Maps.newTreeMap());

    // Create proxy using reflection since constructor is private
    proxy = createTestProxy();
  }

  private JmxCassandraManagementProxy createTestProxy() throws Exception {
    java.lang.reflect.Constructor<JmxCassandraManagementProxy> constructor =
        JmxCassandraManagementProxy.class.getDeclaredConstructor(
            String.class,
            String.class,
            JMXConnector.class,
            StorageServiceMBean.class,
            MBeanServerConnection.class,
            CompactionManagerMBean.class,
            EndpointSnitchInfoMBean.class,
            FailureDetectorMBean.class,
            MetricRegistry.class,
            Optional.class,
            DiagnosticEventPersistenceMBean.class,
            LastEventIdBroadcasterMBean.class);

    constructor.setAccessible(true);

    return constructor.newInstance(
        "127.0.0.1",
        "127.0.0.1",
        mockJmxConnector,
        mockStorageService,
        mockMBeanServer,
        mockCompactionManager,
        mockEndpointSnitch,
        mockFailureDetector,
        mockMetricRegistry,
        Optional.of(mockStreamManager),
        mockDiagEventProxy,
        mockLastEventIdProxy);
  }

  @Test
  public void testListTablesByKeyspace() throws Exception {
    // Given
    Set<ObjectName> objectNames =
        Sets.newHashSet(
            new ObjectName(
                "org.apache.cassandra.db:type=ColumnFamilies,keyspace=test_ks,columnfamily=table1"),
            new ObjectName(
                "org.apache.cassandra.db:type=ColumnFamilies,keyspace=test_ks,columnfamily=table2"),
            new ObjectName(
                "org.apache.cassandra.db:type=ColumnFamilies,keyspace=other_ks,columnfamily=table3"));
    when(mockMBeanServer.queryNames(any(ObjectName.class), any())).thenReturn(objectNames);

    // When
    Map<String, List<String>> result = proxy.listTablesByKeyspace();

    // Then
    assertThat(result).hasSize(2);
    assertThat(result.get("test_ks")).containsExactlyInAnyOrder("table1", "table2");
    assertThat(result.get("other_ks")).containsExactly("table3");
  }

  @Test
  public void testListTablesByKeyspace_IOException() throws Exception {
    // Given
    when(mockMBeanServer.queryNames(any(ObjectName.class), any()))
        .thenThrow(new IOException("Connection failed"));

    // When
    Map<String, List<String>> result = proxy.listTablesByKeyspace();

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  public void testCancelAllRepairs() {
    // When
    proxy.cancelAllRepairs();

    // Then
    verify(mockStorageService).forceTerminateAllRepairSessions();
  }

  @Test
  public void testCancelAllRepairs_RuntimeException() {
    // Given
    doThrow(new RuntimeException("Node down"))
        .when(mockStorageService)
        .forceTerminateAllRepairSessions();

    // When - should not throw
    proxy.cancelAllRepairs();

    // Then - exception should be caught and logged
    verify(mockStorageService).forceTerminateAllRepairSessions();
  }

  @Test
  public void testTakeSnapshot_WithKeyspaces() throws Exception {
    // When
    proxy.takeSnapshot("test-snapshot", "keyspace1", "keyspace2");

    // Then
    verify(mockStorageService).takeSnapshot("test-snapshot", "keyspace1", "keyspace2");
  }

  @Test
  public void testTakeSnapshot_NoKeyspaces() throws Exception {
    // Given
    when(mockStorageService.getKeyspaces()).thenReturn(Lists.newArrayList("ks1", "ks2"));

    // When
    proxy.takeSnapshot("test-snapshot");

    // Then
    verify(mockStorageService).takeSnapshot("test-snapshot", "ks1", "ks2");
  }

  @Test
  public void testForceKeyspaceCompaction() throws Exception {
    // When
    proxy.forceKeyspaceCompaction(true, "test_keyspace", "table1", "table2");

    // Then
    verify(mockStorageService).forceKeyspaceCompaction(true, "test_keyspace", "table1", "table2");
  }

  @Test
  public void testGetCompactions() {
    // Given
    List<Map<String, String>> compactions = Lists.newArrayList();
    Map<String, String> compaction = Maps.newHashMap();
    compaction.put("id", "123");
    compaction.put("keyspace", "test_keyspace");
    compactions.add(compaction);
    when(mockCompactionManager.getCompactions()).thenReturn(compactions);

    // When
    List<Map<String, String>> result = proxy.getCompactions();

    // Then
    assertThat(result).isEqualTo(compactions);
  }

  @Test
  public void testEnableEventPersistence() {
    // When
    proxy.enableEventPersistence("test.event");

    // Then
    verify(mockDiagEventProxy).enableEventPersistence("test.event");
  }

  @Test
  public void testDisableEventPersistence() {
    // When
    proxy.disableEventPersistence("test.event");

    // Then
    verify(mockDiagEventProxy).disableEventPersistence("test.event");
  }

  @Test
  public void testGetUntranslatedHost() throws Exception {
    // When
    String result = proxy.getUntranslatedHost();

    // Then
    assertThat(result).isEqualTo("127.0.0.1");
  }

  @Test
  public void testIsConnectionAlive_True() throws Exception {
    // Given
    when(mockJmxConnector.getConnectionId()).thenReturn("valid-connection-id");

    // When
    boolean result = proxy.isConnectionAlive();

    // Then
    assertThat(result).isTrue();
  }

  @Test
  public void testIsConnectionAlive_False() throws Exception {
    // Given
    when(mockJmxConnector.getConnectionId()).thenThrow(new IOException("Connection lost"));

    // When
    boolean result = proxy.isConnectionAlive();

    // Then
    assertThat(result).isFalse();
  }

  @Test
  public void testIsConnectionAlive_NullConnectionId() throws Exception {
    // Given
    when(mockJmxConnector.getConnectionId()).thenReturn(null);

    // When
    boolean result = proxy.isConnectionAlive();

    // Then
    assertThat(result).isFalse();
  }

  @Test
  public void testIsConnectionAlive_EmptyConnectionId() throws Exception {
    // Given
    when(mockJmxConnector.getConnectionId()).thenReturn("");

    // When
    boolean result = proxy.isConnectionAlive();

    // Then
    assertThat(result).isFalse();
  }

  @Test
  public void testRemoveRepairStatusHandler() throws Exception {
    // Given - add a handler first
    java.lang.reflect.Field handlersField =
        JmxCassandraManagementProxy.class.getDeclaredField("repairStatusHandlers");
    handlersField.setAccessible(true);
    Map<Integer, RepairStatusHandler> handlers =
        (Map<Integer, RepairStatusHandler>) handlersField.get(proxy);
    handlers.put(123, mockRepairStatusHandler);

    java.lang.reflect.Field executorsField =
        JmxCassandraManagementProxy.class.getDeclaredField("repairStatusExecutors");
    executorsField.setAccessible(true);
    Map<Integer, Object> executors = (Map<Integer, Object>) executorsField.get(proxy);
    executors.put(123, java.util.concurrent.Executors.newSingleThreadExecutor());

    // When
    proxy.removeRepairStatusHandler(123);

    // Then
    assertThat(handlers).doesNotContainKey(123);
    assertThat(executors).doesNotContainKey(123);
  }

  @Test
  public void testTriggerRepair_DatacenterAwareFallback() throws Exception {
    // Given - version that doesn't support DATACENTER_AWARE
    when(mockStorageService.getReleaseVersion()).thenReturn("2.0.10");
    when(mockStorageService.repairAsync(anyString(), any(Map.class))).thenReturn(456);

    // When
    int repairNo =
        proxy.triggerRepair(
            "test_keyspace",
            RepairParallelism.DATACENTER_AWARE,
            Lists.newArrayList("table1"),
            RepairType.SUBRANGE_FULL,
            Lists.newArrayList("dc1"),
            mockRepairStatusHandler,
            Lists.newArrayList(new RingRange(new BigInteger("0"), new BigInteger("100"))),
            1);

    // Then
    assertThat(repairNo).isEqualTo(456);
    verify(mockStorageService).repairAsync(eq("test_keyspace"), any(Map.class));
  }

  @Test
  public void testTriggerRepair_RuntimeException() throws Exception {
    // Given
    when(mockStorageService.repairAsync(anyString(), any(Map.class)))
        .thenThrow(new RuntimeException("Repair failed"));

    // When/Then
    assertThatThrownBy(
            () ->
                proxy.triggerRepair(
                    "test_keyspace",
                    RepairParallelism.SEQUENTIAL,
                    Lists.newArrayList("table1"),
                    RepairType.SUBRANGE_FULL,
                    Lists.newArrayList("dc1"),
                    mockRepairStatusHandler,
                    Lists.newArrayList(),
                    1))
        .isInstanceOf(ReaperException.class);
  }

  @Test
  public void testGetRangeToEndpointMap_RuntimeException() {
    // Given
    when(mockStorageService.getRangeToEndpointMap("test_keyspace"))
        .thenThrow(new RuntimeException("Failed to get range map"));

    // When/Then
    assertThatThrownBy(() -> proxy.getRangeToEndpointMap("test_keyspace"))
        .isInstanceOf(ReaperException.class);
  }

  @Test
  public void testGetPartitioner_RuntimeException() {
    // Given
    when(mockStorageService.getPartitionerName())
        .thenThrow(new RuntimeException("Failed to get partitioner"));

    // When/Then
    assertThatThrownBy(() -> proxy.getPartitioner()).isInstanceOf(ReaperException.class);
  }

  @Test
  public void testGetLiveNodes_RuntimeException() {
    // Given
    when(mockStorageService.getLiveNodes())
        .thenThrow(new RuntimeException("Failed to get live nodes"));

    // When/Then
    assertThatThrownBy(() -> proxy.getLiveNodes()).isInstanceOf(ReaperException.class);
  }

  @Test
  public void testStaticConnect_NullHost() {
    // When/Then
    assertThatThrownBy(
            () ->
                JmxCassandraManagementProxy.connect(
                    null, Optional.empty(), null, 30, mockMetricRegistry, null, "test-cluster"))
        .isInstanceOf(ReaperException.class)
        .hasMessageContaining("Null host given to JmxProxy.connect()");
  }

  @Test
  public void testGetRmiClientSocketFactory() throws Exception {
    // Given - test both SSL and non-SSL cases
    System.setProperty("ssl.enable", "false");

    // When
    java.lang.reflect.Method method =
        JmxCassandraManagementProxy.class.getDeclaredMethod("getRmiClientSocketFactory");
    method.setAccessible(true);
    Object result = method.invoke(null);

    // Then
    assertThat(result).isNotNull();

    // Test SSL case
    System.setProperty("ssl.enable", "true");
    Object sslResult = method.invoke(null);
    assertThat(sslResult).isNotNull();

    // Clean up
    System.clearProperty("ssl.enable");
  }
}
