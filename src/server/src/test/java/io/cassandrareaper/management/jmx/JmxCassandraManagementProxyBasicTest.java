package io.cassandrareaper.management.jmx;

import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.RepairType;
import io.cassandrareaper.management.RepairStatusHandler;
import io.cassandrareaper.service.RingRange;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
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

public class JmxCassandraManagementProxyBasicTest {

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
    // Use reflection to access private constructor
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
  public void testGetHost() {
    // When/Then
    assertThat(proxy.getHost()).isEqualTo("127.0.0.1");
  }

  @Test
  public void testGetClusterName() {
    // When/Then
    assertThat(proxy.getClusterName()).isEqualTo("test-cluster");
  }

  @Test
  public void testGetCassandraVersion() {
    // When/Then
    assertThat(proxy.getCassandraVersion()).isEqualTo("3.11.0");
  }

  @Test
  public void testGetTokens() {
    // Given
    Map<String, String> tokenMap = Maps.newHashMap();
    tokenMap.put("12345", "127.0.0.1");
    tokenMap.put("67890", "127.0.0.2");
    when(mockStorageService.getTokenToEndpointMap()).thenReturn(tokenMap);

    // When
    List<BigInteger> tokens = proxy.getTokens();

    // Then
    assertThat(tokens).hasSize(2);
    assertThat(tokens).contains(new BigInteger("12345"), new BigInteger("67890"));
  }

  @Test
  public void testGetRangeToEndpointMap() throws ReaperException {
    // Given
    String keyspace = "test_keyspace";
    Map<List<String>, List<String>> rangeMap = Maps.newHashMap();
    rangeMap.put(Lists.newArrayList("0", "100"), Lists.newArrayList("127.0.0.1"));
    when(mockStorageService.getRangeToEndpointMap(keyspace)).thenReturn(rangeMap);

    // When
    Map<List<String>, List<String>> result = proxy.getRangeToEndpointMap(keyspace);

    // Then
    assertThat(result).isEqualTo(rangeMap);
  }

  @Test
  public void testGetRangeToEndpointMap_RuntimeException() {
    // Given
    String keyspace = "test_keyspace";
    when(mockStorageService.getRangeToEndpointMap(keyspace))
        .thenThrow(new RuntimeException("Connection failed"));

    // When/Then
    assertThatThrownBy(() -> proxy.getRangeToEndpointMap(keyspace))
        .isInstanceOf(ReaperException.class)
        .hasMessageContaining("Connection failed");
  }

  @Test
  public void testGetLocalEndpoint() throws ReaperException {
    // Given
    String hostId = "host-123";
    Map<String, String> hostIdToEndpoint = Maps.newHashMap();
    hostIdToEndpoint.put(hostId, "127.0.0.1");
    when(mockStorageService.getLocalHostId()).thenReturn(hostId);
    when(mockStorageService.getHostIdToEndpoint()).thenReturn(hostIdToEndpoint);

    // When
    String endpoint = proxy.getLocalEndpoint();

    // Then
    assertThat(endpoint).isEqualTo("127.0.0.1");
  }

  @Test
  public void testGetEndpointToHostId() {
    // Given
    Map<String, String> endpointToHostId = Maps.newHashMap();
    endpointToHostId.put("127.0.0.1", "host-123");
    when(mockStorageService.getEndpointToHostId()).thenReturn(endpointToHostId);

    // When
    Map<String, String> result = proxy.getEndpointToHostId();

    // Then
    assertThat(result).isEqualTo(endpointToHostId);
  }

  @Test
  public void testGetPartitioner() throws ReaperException {
    // Given
    when(mockStorageService.getPartitionerName()).thenReturn("Murmur3Partitioner");

    // When
    String partitioner = proxy.getPartitioner();

    // Then
    assertThat(partitioner).isEqualTo("Murmur3Partitioner");
  }

  @Test
  public void testGetPartitioner_RuntimeException() {
    // Given
    when(mockStorageService.getPartitionerName()).thenThrow(new RuntimeException("JMX error"));

    // When/Then
    assertThatThrownBy(() -> proxy.getPartitioner()).isInstanceOf(ReaperException.class);
  }

  @Test
  public void testGetKeyspaces() {
    // Given
    List<String> keyspaces = Lists.newArrayList("system", "test_keyspace");
    when(mockStorageService.getKeyspaces()).thenReturn(keyspaces);

    // When
    List<String> result = proxy.getKeyspaces();

    // Then
    assertThat(result).isEqualTo(keyspaces);
  }

  @Test
  public void testGetPendingCompactions() throws Exception {
    // Given
    when(mockMBeanServer.getAttribute(any(ObjectName.class), eq("Value"))).thenReturn(5);

    // When
    int pendingCompactions = proxy.getPendingCompactions();

    // Then
    assertThat(pendingCompactions).isEqualTo(5);
  }

  @Test
  public void testGetPendingCompactions_IOException() throws Exception {
    // Given
    when(mockMBeanServer.getAttribute(any(ObjectName.class), eq("Value")))
        .thenThrow(new IOException("Connection failed"));

    // When
    int pendingCompactions = proxy.getPendingCompactions();

    // Then
    assertThat(pendingCompactions).isEqualTo(0); // Default when uncertain
  }

  @Test
  public void testGetPendingCompactions_InstanceNotFoundException() throws Exception {
    // Given
    when(mockMBeanServer.getAttribute(any(ObjectName.class), eq("Value")))
        .thenThrow(new InstanceNotFoundException("MBean not found"));

    // When
    int pendingCompactions = proxy.getPendingCompactions();

    // Then
    assertThat(pendingCompactions).isEqualTo(0); // Default when uncertain
  }

  @Test
  public void testIsRepairRunning_ValidationCompactionRunning() throws Exception {
    // Given
    when(mockMBeanServer.getAttribute(any(ObjectName.class), eq("Value")))
        .thenReturn(2) // Active validations
        .thenReturn(1); // Pending validations

    // When
    boolean isRunning = proxy.isRepairRunning();

    // Then
    assertThat(isRunning).isTrue();
  }

  @Test
  public void testIsRepairRunning_Post22RepairRunning() throws Exception {
    // Given
    when(mockMBeanServer.getAttribute(any(ObjectName.class), eq("Value")))
        .thenReturn(0) // No active validations
        .thenReturn(0); // No pending validations

    Set<ObjectName> repairMBeans =
        Sets.newHashSet(new ObjectName("org.apache.cassandra.internal:type=Repair#123"));
    when(mockMBeanServer.queryNames(any(ObjectName.class), any())).thenReturn(repairMBeans);

    // When
    boolean isRunning = proxy.isRepairRunning();

    // Then
    assertThat(isRunning).isTrue();
  }

  @Test
  public void testIsRepairRunning_NoRepairsRunning() throws Exception {
    // Given
    when(mockMBeanServer.getAttribute(any(ObjectName.class), eq("Value")))
        .thenReturn(0) // No active validations
        .thenReturn(0); // No pending validations

    when(mockMBeanServer.queryNames(any(ObjectName.class), any())).thenReturn(Sets.newHashSet());

    // When
    boolean isRunning = proxy.isRepairRunning();

    // Then
    assertThat(isRunning).isFalse();
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

    // Then
    verify(mockStorageService).forceTerminateAllRepairSessions();
  }

  @Test
  public void testListTablesByKeyspace() throws Exception {
    // Given
    Set<ObjectName> objectNames =
        Sets.newHashSet(
            new ObjectName(
                "org.apache.cassandra.db:type=ColumnFamilies,keyspace=test_ks,columnfamily=table1"),
            new ObjectName(
                "org.apache.cassandra.db:type=ColumnFamilies,keyspace=test_ks,columnfamily=table2"));

    when(mockMBeanServer.queryNames(any(ObjectName.class), any())).thenReturn(objectNames);

    // When
    Map<String, List<String>> result = proxy.listTablesByKeyspace();

    // Then
    assertThat(result).containsKey("test_ks");
    assertThat(result.get("test_ks")).containsExactlyInAnyOrder("table1", "table2");
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
  public void testTriggerRepair() throws Exception {
    // Given
    String keyspace = "test_keyspace";
    Collection<String> columnFamilies = Lists.newArrayList("table1");
    Collection<String> datacenters = Lists.newArrayList("dc1");
    List<RingRange> ranges =
        Lists.newArrayList(new RingRange(new BigInteger("0"), new BigInteger("100")));

    when(mockStorageService.repairAsync(eq(keyspace), any(Map.class))).thenReturn(123);

    // When
    int repairNo =
        proxy.triggerRepair(
            keyspace,
            RepairParallelism.SEQUENTIAL,
            columnFamilies,
            RepairType.SUBRANGE_FULL,
            datacenters,
            mockRepairStatusHandler,
            ranges,
            1);

    // Then
    assertThat(repairNo).isEqualTo(123);
    verify(mockStorageService).repairAsync(eq(keyspace), any(Map.class));
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
            Lists.newArrayList(),
            1);

    // Then
    assertThat(repairNo).isEqualTo(456);
  }

  @Test
  public void testTriggerRepair_RuntimeException() {
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
  public void testIsConnectionAlive() throws IOException {
    // When
    boolean isAlive = proxy.isConnectionAlive();

    // Then
    assertThat(isAlive).isTrue();
    verify(mockJmxConnector).getConnectionId();
  }

  @Test
  public void testIsConnectionAlive_IOException() throws IOException {
    // Given
    when(mockJmxConnector.getConnectionId()).thenThrow(new IOException("Connection lost"));

    // When
    boolean isAlive = proxy.isConnectionAlive();

    // Then
    assertThat(isAlive).isFalse();
  }

  @Test
  public void testRemoveRepairStatusHandler() {
    // Given
    int repairNo = 123;

    // When
    proxy.removeRepairStatusHandler(repairNo);

    // Then - should not throw
    assertThat(proxy).isNotNull();
  }

  @Test
  public void testClose() throws Exception {
    // When
    proxy.close();

    // Then - The close method calls removeNotificationListener twice (for STORAGE_SERVICE and
    // STREAM_MANAGER)
    verify(mockMBeanServer, atLeast(1))
        .removeNotificationListener(any(ObjectName.class), eq(proxy));
    verify(mockJmxConnector).close();
  }

  @Test
  public void testClose_IOException() throws Exception {
    // Given
    doThrow(new IOException("Close failed")).when(mockJmxConnector).close();

    // When - should not throw
    proxy.close();

    // Then
    verify(mockJmxConnector).close();
  }

  @Test
  public void testGetLiveNodes() throws ReaperException {
    // Given
    List<String> liveNodes = Lists.newArrayList("127.0.0.1", "127.0.0.2");
    when(mockStorageService.getLiveNodes()).thenReturn(liveNodes);

    // When
    List<String> result = proxy.getLiveNodes();

    // Then
    assertThat(result).isEqualTo(liveNodes);
  }

  @Test
  public void testGetLiveNodes_RuntimeException() {
    // Given
    when(mockStorageService.getLiveNodes()).thenThrow(new RuntimeException("JMX error"));

    // When/Then
    assertThatThrownBy(() -> proxy.getLiveNodes())
        .isInstanceOf(ReaperException.class)
        .hasMessageContaining("JMX error");
  }

  @Test
  public void testGetUntranslatedHost() throws ReaperException {
    // When
    String untranslatedHost = proxy.getUntranslatedHost();

    // Then
    assertThat(untranslatedHost).isEqualTo("127.0.0.1");
  }

  @Test
  public void testTakeSnapshot_WithKeyspaces() throws IOException {
    // Given
    String snapshotName = "test-snapshot";
    String[] keyspaces = {"ks1", "ks2"};

    // When
    proxy.takeSnapshot(snapshotName, keyspaces);

    // Then
    verify(mockStorageService).takeSnapshot(snapshotName, keyspaces);
  }

  @Test
  public void testTakeSnapshot_AllKeyspaces() throws IOException {
    // Given
    String snapshotName = "test-snapshot";
    when(mockStorageService.getKeyspaces()).thenReturn(Lists.newArrayList("ks1", "ks2"));

    // When
    proxy.takeSnapshot(snapshotName);

    // Then - takeSnapshot with no keyspaces calls getKeyspaces() and then takeSnapshot with varargs
    verify(mockStorageService).takeSnapshot(eq(snapshotName), eq("ks1"), eq("ks2"));
  }

  @Test
  public void testUserPasswordCallbackHandler() throws IOException, UnsupportedCallbackException {
    // Given
    JmxCassandraManagementProxy.UserPasswordCallbackHandler handler =
        new JmxCassandraManagementProxy.UserPasswordCallbackHandler("testuser", "testpass");

    NameCallback nameCallback = new NameCallback("Username:");
    PasswordCallback passwordCallback = new PasswordCallback("Password:", false);
    Callback[] callbacks = {nameCallback, passwordCallback};

    // When
    handler.handle(callbacks);

    // Then
    assertThat(nameCallback.getName()).isEqualTo("testuser");
    assertThat(passwordCallback.getPassword()).isEqualTo("testpass".toCharArray());
  }

  @Test
  public void testUserPasswordCallbackHandler_NullPassword()
      throws IOException, UnsupportedCallbackException {
    // Given
    JmxCassandraManagementProxy.UserPasswordCallbackHandler handler =
        new JmxCassandraManagementProxy.UserPasswordCallbackHandler("testuser", null);

    PasswordCallback passwordCallback = new PasswordCallback("Password:", false);
    Callback[] callbacks = {passwordCallback};

    // When
    handler.handle(callbacks);

    // Then
    assertThat(passwordCallback.getPassword()).isNull();
  }

  @Test
  public void testUserPasswordCallbackHandler_UnsupportedCallback() {
    // Given
    JmxCassandraManagementProxy.UserPasswordCallbackHandler handler =
        new JmxCassandraManagementProxy.UserPasswordCallbackHandler("testuser", "testpass");

    Callback unsupportedCallback = mock(Callback.class);
    Callback[] callbacks = {unsupportedCallback};

    // When/Then
    assertThatThrownBy(() -> handler.handle(callbacks))
        .isInstanceOf(UnsupportedCallbackException.class);
  }
}
