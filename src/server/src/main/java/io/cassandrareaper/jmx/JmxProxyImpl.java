/*
 * Copyright 2014-2017 Spotify AB
 * Copyright 2016-2018 The Last Pickle Ltd
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

package io.cassandrareaper.jmx;

import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.Segment;
import io.cassandrareaper.service.RingRange;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMISocketFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.JMX;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.rmi.ssl.SslRMIClientSocketFactory;
import javax.validation.constraints.NotNull;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.VersionNumber;
import com.datastax.driver.core.policies.EC2MultiRegionAddressTranslator;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.FailureDetectorMBean;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.streaming.StreamManagerMBean;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


final class JmxProxyImpl implements JmxProxy {

  private static final Logger LOG = LoggerFactory.getLogger(JmxProxy.class);

  private static final String VALUE_ATTRIBUTE = "Value";
  private static final String FAILED_TO_CONNECT_TO_USING_JMX = "Failed to connect to {} using JMX";
  private static final String ERROR_GETTING_ATTR_JMX = "Error getting attribute from JMX";


  private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();

  private final JMXConnector jmxConnector;
  private final MBeanServerConnection mbeanServer;
  private final CompactionManagerMBean cmProxy;
  private final EndpointSnitchInfoMBean endpointSnitchMbean;
  private final StorageServiceMBean ssProxy;
  private final FailureDetectorMBean fdProxy;
  private final String host;
  private final String hostBeforeTranslation;
  private final String clusterName;
  private final ConcurrentMap<Integer, ExecutorService> repairStatusExecutors = Maps.newConcurrentMap();
  private final ConcurrentMap<Integer, RepairStatusHandler> repairStatusHandlers = Maps.newConcurrentMap();
  private final MetricRegistry metricRegistry;
  private final Optional<StreamManagerMBean> smProxy;

  private JmxProxyImpl(
      String host,
      String hostBeforeTranslation,
      JMXConnector jmxConnector,
      StorageServiceMBean ssProxy,
      MBeanServerConnection mbeanServer,
      CompactionManagerMBean cmProxy,
      EndpointSnitchInfoMBean endpointSnitchMbean,
      FailureDetectorMBean fdProxy,
      MetricRegistry metricRegistry,
      Optional<StreamManagerMBean> smProxy) {

    this.host = host;
    this.hostBeforeTranslation = hostBeforeTranslation;
    this.jmxConnector = jmxConnector;
    this.mbeanServer = mbeanServer;
    this.ssProxy = ssProxy;
    this.cmProxy = cmProxy;
    this.endpointSnitchMbean = endpointSnitchMbean;
    this.clusterName = Cluster.toSymbolicName(ssProxy.getClusterName());
    this.fdProxy = fdProxy;
    this.metricRegistry = metricRegistry;
    this.smProxy = smProxy;
    registerConnectionsGauge();
  }

  /**
   * @see JmxProxy#connect(Optional, String, int, String, String, EC2MultiRegionAddressTranslator)
   */
  static JmxProxy connect(
      String host,
      String username,
      String password,
      final EC2MultiRegionAddressTranslator addressTranslator,
      int connectionTimeout,
      MetricRegistry metricRegistry)
      throws ReaperException, InterruptedException {

    if (host == null) {
      throw new ReaperException("Null host given to JmxProxy.connect()");
    }

    final HostAndPort hostAndPort = HostAndPort.fromString(host);

    return connect(
        hostAndPort.getHost(),
        hostAndPort.getPortOrDefault(Cluster.DEFAULT_JMX_PORT),
        username,
        password,
        addressTranslator,
        connectionTimeout,
        metricRegistry);
  }

  /**
   * Connect to JMX interface on the given host and port.
   *
   * @param handler Implementation of {@link RepairStatusHandler} to process incoming notifications
   *     of repair events.
   * @param originalHost hostname or ip address of Cassandra node
   * @param port port number to use for JMX connection
   * @param username username to use for JMX authentication
   * @param password password to use for JMX authentication
   * @param addressTranslator if EC2MultiRegionAddressTranslator isn't null it will be used to
   *     translate addresses
   */
  private static JmxProxy connect(
      String originalHost,
      int port,
      String username,
      String password,
      final EC2MultiRegionAddressTranslator addressTranslator,
      int connectionTimeout,
      MetricRegistry metricRegistry) throws ReaperException, InterruptedException {

    JMXServiceURL jmxUrl;
    String host = originalHost;

    if (addressTranslator != null) {
      host = addressTranslator.translate(new InetSocketAddress(host, port)).getAddress().getHostAddress();
      LOG.debug("translated {} to {}", originalHost, host);
    }

    try {
      LOG.debug("Connecting to {}...", host);
      jmxUrl = JmxAddresses.getJmxServiceUrl(host, port);
    } catch (MalformedURLException e) {
      LOG.error(String.format("Failed to prepare the JMX connection to %s:%s", host, port));
      throw new ReaperException("Failure during preparations for JMX connection", e);
    }
    try {
      final Map<String, Object> env = new HashMap<>();
      if (username != null && password != null) {
        String[] creds = {username, password};
        env.put(JMXConnector.CREDENTIALS, creds);
      }
      env.put("com.sun.jndi.rmi.factory.socket", getRmiClientSocketFactory());
      JMXConnector jmxConn = connectWithTimeout(jmxUrl, connectionTimeout, TimeUnit.SECONDS, env);
      MBeanServerConnection mbeanServerConn = jmxConn.getMBeanServerConnection();

      StorageServiceMBean ssProxy
          = JMX.newMBeanProxy(mbeanServerConn, ObjectNames.STORAGE_SERVICE, StorageServiceMBean.class);

      final String cassandraVersion = ssProxy.getReleaseVersion();
      if (cassandraVersion.startsWith("2.0") || cassandraVersion.startsWith("1.")) {
        ssProxy = JMX.newMBeanProxy(mbeanServerConn, ObjectNames.STORAGE_SERVICE, StorageServiceMBean20.class);
      }

      final Optional<StreamManagerMBean> smProxy;
      // StreamManagerMbean is only available since Cassandra 2.0
      if (cassandraVersion.startsWith("1.")) {
        smProxy = Optional.empty();
      } else {
        smProxy = Optional.of(JMX.newMBeanProxy(mbeanServerConn, ObjectNames.STREAM_MANAGER, StreamManagerMBean.class));
      }

      JmxProxy proxy
          = new JmxProxyImpl(
              host,
              originalHost,
              jmxConn,
              ssProxy,
              mbeanServerConn,
              JMX.newMBeanProxy(mbeanServerConn, ObjectNames.COMPACTION_MANAGER, CompactionManagerMBean.class),
              JMX.newMBeanProxy(mbeanServerConn, ObjectNames.ENDPOINT_SNITCH_INFO, EndpointSnitchInfoMBean.class),
              JMX.newMBeanProxy(mbeanServerConn, ObjectNames.FAILURE_DETECTOR, FailureDetectorMBean.class),
              metricRegistry,
              smProxy);

      // registering listeners throws bunch of exceptions, so do it here rather than in the constructor
      mbeanServerConn.addNotificationListener(ObjectNames.STORAGE_SERVICE, proxy, null, null);
      if (smProxy.isPresent()) {
        mbeanServerConn.addNotificationListener(ObjectNames.STREAM_MANAGER, proxy, null, null);
      }
      LOG.debug("JMX connection to {} properly connected: {}", host, jmxUrl.toString());

      return proxy;
    } catch (IOException | ExecutionException | TimeoutException | InstanceNotFoundException e) {
      throw new ReaperException("Failure when establishing JMX connection to " + host + ":" + port, e);
    } catch (InterruptedException expected) {
      LOG.debug(
          "JMX connection to {}:{} was interrupted by Reaper. "
              + "Another JMX connection must have succeeded before this one.",
          host,
          port);
      throw expected;
    }
  }

  private static JMXConnector connectWithTimeout(
      JMXServiceURL url,
      long timeout,
      TimeUnit unit,
      Map<String, Object> env) throws InterruptedException, ExecutionException, TimeoutException {

    Future<JMXConnector> future = EXECUTOR.submit(() -> JMXConnectorFactory.connect(url, env));
    return future.get(timeout, unit);
  }

  @Override
  public String getHost() {
    return host;
  }

  @Override
  public List<BigInteger> getTokens() {
    Preconditions.checkNotNull(ssProxy, "Looks like the proxy is not connected");

    return Lists.transform(Lists.newArrayList(ssProxy.getTokenToEndpointMap().keySet()), s -> new BigInteger(s));
  }

  @Override
  public Map<List<String>, List<String>> getRangeToEndpointMap(String keyspace) throws ReaperException {
    Preconditions.checkNotNull(ssProxy, "Looks like the proxy is not connected");
    try {
      return ssProxy.getRangeToEndpointMap(keyspace);
    } catch (RuntimeException e) {
      LOG.error(e.getMessage());
      throw new ReaperException(e.getMessage(), e);
    }
  }

  @Override
  public List<RingRange> getRangesForLocalEndpoint(String keyspace) throws ReaperException {
    Preconditions.checkNotNull(ssProxy, "Looks like the proxy is not connected");
    List<RingRange> localRanges = Lists.newArrayList();
    try {
      Map<List<String>, List<String>> ranges = ssProxy.getRangeToEndpointMap(keyspace);
      String localEndpoint = getLocalEndpoint();
      // Filtering ranges for which the local node is a replica
      // For local mode
      ranges
          .entrySet()
          .stream()
          .forEach(entry -> {
            if (entry.getValue().contains(localEndpoint)) {
              localRanges.add(
                  new RingRange(new BigInteger(entry.getKey().get(0)), new BigInteger(entry.getKey().get(1))));
            }
          });

      LOG.info("LOCAL RANGES {}", localRanges);
      return localRanges;
    } catch (RuntimeException e) {
      LOG.error(e.getMessage());
      throw new ReaperException(e.getMessage(), e);
    }
  }

  public String getLocalEndpoint() throws ReaperException {
    String cassandraVersion = getCassandraVersion();
    if (versionCompare(cassandraVersion, "2.1.10") >= 0) {
      return ssProxy.getHostIdToEndpoint().get(ssProxy.getLocalHostId());
    } else {
      // pre-2.1.10 compatibility
      BiMap<String, String> hostIdBiMap = ImmutableBiMap.copyOf(ssProxy.getHostIdMap());
      String localHostId = ssProxy.getLocalHostId();
      return hostIdBiMap.inverse().get(localHostId);
    }
  }

  @NotNull
  @Override
  public List<String> tokenRangeToEndpoint(String keyspace, Segment segment) {
    Preconditions.checkNotNull(ssProxy, "Looks like the proxy is not connected");

    Set<Map.Entry<List<String>, List<String>>> entries = ssProxy.getRangeToEndpointMap(keyspace).entrySet();

    for (Map.Entry<List<String>, List<String>> entry : entries) {
      BigInteger rangeStart = new BigInteger(entry.getKey().get(0));
      BigInteger rangeEnd = new BigInteger(entry.getKey().get(1));
      if (new RingRange(rangeStart, rangeEnd).encloses(segment.getTokenRanges().get(0))) {
        LOG.debug(
            "[tokenRangeToEndpoint] Found replicas for token range {} : {}",
            segment.getTokenRanges().get(0),
            entry.getValue());
        return entry.getValue();
      }
    }
    LOG.error("[tokenRangeToEndpoint] no replicas found for token range {}", segment);
    LOG.debug("[tokenRangeToEndpoint] checked token ranges were {}", entries);
    return Lists.newArrayList();
  }

  @NotNull
  @Override
  public Map<String, String> getEndpointToHostId() {
    Preconditions.checkNotNull(ssProxy, "Looks like the proxy is not connected");
    try {
      return ssProxy.getEndpointToHostId();
    } catch (UndeclaredThrowableException e) {
      return ssProxy.getHostIdMap();
    }
  }

  @Override
  public String getPartitioner() {
    Preconditions.checkNotNull(ssProxy, "Looks like the proxy is not connected");
    return ssProxy.getPartitionerName();
  }

  @Override
  public String getClusterName() {
    Preconditions.checkNotNull(ssProxy, "Looks like the proxy is not connected");
    return ssProxy.getClusterName();
  }

  @Override
  public List<String> getKeyspaces() {
    Preconditions.checkNotNull(ssProxy, "Looks like the proxy is not connected");
    return ssProxy.getKeyspaces();
  }

  @Override
  public Set<String> getTableNamesForKeyspace(String keyspace) throws ReaperException {
    Set<String> tableNames = new HashSet<>();
    Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> proxies;
    try {
      proxies = ColumnFamilyStoreMBeanIterator.getColumnFamilyStoreMBeanProxies(mbeanServer);
    } catch (IOException | MalformedObjectNameException e) {
      throw new ReaperException("failed to get ColumnFamilyStoreMBean instances from JMX", e);
    }
    while (proxies.hasNext()) {
      Map.Entry<String, ColumnFamilyStoreMBean> proxyEntry = proxies.next();
      String keyspaceName = proxyEntry.getKey();
      if (keyspace.equalsIgnoreCase(keyspaceName)) {
        ColumnFamilyStoreMBean columnFamilyMBean = proxyEntry.getValue();
        tableNames.add(columnFamilyMBean.getColumnFamilyName());
      }
    }
    return tableNames;
  }

  @Override
  public int getPendingCompactions() throws JMException {
    try {
      int pendingCount = (int) mbeanServer.getAttribute(ObjectNames.COMPACTIONS_PENDING, VALUE_ATTRIBUTE);
      return pendingCount;
    } catch (IOException ignored) {
      LOG.warn(FAILED_TO_CONNECT_TO_USING_JMX, host, ignored);
    } catch (InstanceNotFoundException e) {
      // This happens if no repair has yet been run on the node
      // The AntiEntropySessions object is created on the first repair
      LOG.error("Error getting pending compactions attribute from JMX", e);
      return 0;
    } catch (RuntimeException e) {
      LOG.error(ERROR_GETTING_ATTR_JMX, e);
    }
    // If uncertain, assume it's running
    return 0;
  }

  @Override
  public boolean isRepairRunning() throws JMException {
    return isRepairRunningPre22() || isRepairRunningPost22() || isValidationCompactionRunning();
  }

  /**
   * @return true if any repairs are running on the node.
   */
  private boolean isRepairRunningPre22() throws JMException {
    // Check if AntiEntropySession is actually running on the node
    try {
      int activeCount = (Integer) mbeanServer.getAttribute(ObjectNames.ANTI_ENTROPY_SESSIONS, "ActiveCount");
      long pendingCount = (Long) mbeanServer.getAttribute(ObjectNames.ANTI_ENTROPY_SESSIONS, "PendingTasks");
      return activeCount + pendingCount != 0;
    } catch (IOException ignored) {
      LOG.warn(FAILED_TO_CONNECT_TO_USING_JMX, host, ignored);
    } catch (InstanceNotFoundException e) {
      // This happens if no repair has yet been run on the node
      // The AntiEntropySessions object is created on the first repair
      LOG.debug("No repair has run yet on the node. Ignoring exception.", e);
      return false;
    } catch (RuntimeException e) {
      LOG.error(ERROR_GETTING_ATTR_JMX, e);
    }
    // If uncertain, assume it's running
    return true;
  }

  /**
   * @return true if any repairs are running on the node.
   */
  private boolean isValidationCompactionRunning() throws JMException {

    // Check if AntiEntropySession is actually running on the node
    try {
      int activeCount
          = ((Number) mbeanServer.getAttribute(ObjectNames.TP_VALIDATIONS_ACTIVE, VALUE_ATTRIBUTE)).intValue();

      int pendingCount
          = ((Number) mbeanServer.getAttribute(ObjectNames.TP_VALIDATIONS_PENDING, VALUE_ATTRIBUTE)).intValue();

      return activeCount + pendingCount != 0;
    } catch (IOException ignored) {
      LOG.warn(FAILED_TO_CONNECT_TO_USING_JMX, host, ignored);
    } catch (InstanceNotFoundException e) {
      LOG.error("Error getting pending/active validation compaction attributes from JMX", e);
      return false;
    } catch (RuntimeException e) {
      LOG.error(ERROR_GETTING_ATTR_JMX, e);
    }
    // If uncertain, assume it's not running
    return false;
  }

  /**
   * New way of determining if a repair is running after C* 2.2
   *
   * @return true if any repairs are running on the node.
   */
  private boolean isRepairRunningPost22() {
    try {
      // list all mbeans in search of one with the name Repair#??
      // This is the replacement for AntiEntropySessions since Cassandra 2.2
      Set beanSet = mbeanServer.queryNames(ObjectNames.INTERNALS, null);
      for (Object bean : beanSet) {
        ObjectName objName = (ObjectName) bean;
        if (objName.getCanonicalName().contains("Repair#")) {
          return true;
        }
      }
      return false;
    } catch (IOException ignored) {
      LOG.warn(FAILED_TO_CONNECT_TO_USING_JMX, host, ignored);
    } catch (RuntimeException e) {
      LOG.error(ERROR_GETTING_ATTR_JMX, e);
    }
    // If uncertain, assume it's running
    return true;
  }

  @Override
  public void cancelAllRepairs() {
    Preconditions.checkNotNull(ssProxy, "Looks like the proxy is not connected");
    try {
      ssProxy.forceTerminateAllRepairSessions();
    } catch (RuntimeException e) {
      // This can happen if the node is down (UndeclaredThrowableException),
      // in which case repairs will be cancelled anyway...
      LOG.warn("Failed to terminate all repair sessions; node down?", e);
    }
  }

  @Override
  public Map<String, List<String>> listTablesByKeyspace() {
    Map<String, List<String>> tablesByKeyspace = Maps.newHashMap();
    try {
      Set<ObjectName> beanSet = mbeanServer.queryNames(ObjectNames.COLUMN_FAMILIES, null);

      tablesByKeyspace = beanSet.stream()
              .map(bean ->
                      new JmxColumnFamily(bean.getKeyProperty("keyspace"), bean.getKeyProperty("columnfamily")))
              .collect(
                  Collectors.groupingBy(
                      JmxColumnFamily::getKeyspace,
                      Collectors.mapping(JmxColumnFamily::getColumnFamily, Collectors.toList())));

    } catch (IOException e) {
      LOG.warn("Couldn't get a list of tables through JMX", e);
    }

    return Collections.unmodifiableMap(tablesByKeyspace);
  }

  @Override
  public String getCassandraVersion() {
    return ssProxy.getReleaseVersion();
  }


  @Override
  public int triggerRepair(
      BigInteger beginToken,
      BigInteger endToken,
      String keyspace,
      RepairParallelism repairParallelism,
      Collection<String> columnFamilies,
      boolean fullRepair,
      Collection<String> datacenters,
      RepairStatusHandler repairStatusHandler,
      List<RingRange> associatedTokens,
      int repairThreadCount)
      throws ReaperException {

    Preconditions.checkNotNull(ssProxy, "Looks like the proxy is not connected");
    String cassandraVersion = getCassandraVersion();
    boolean canUseDatacenterAware = false;
    canUseDatacenterAware = versionCompare(cassandraVersion, "2.0.12") >= 0;

    String msg = String.format(
        "Triggering repair of range (%s,%s] for keyspace \"%s\" on "
        + "host %s, with repair parallelism %s, in cluster with Cassandra "
        + "version '%s' (can use DATACENTER_AWARE '%s'), "
        + "for column families: %s",
        beginToken.toString(),
        endToken.toString(),
        keyspace,
        this.host,
        repairParallelism,
        cassandraVersion,
        canUseDatacenterAware,
        columnFamilies);
    LOG.info(msg);
    if (repairParallelism.equals(RepairParallelism.DATACENTER_AWARE) && !canUseDatacenterAware) {
      LOG.info(
          "Cannot use DATACENTER_AWARE repair policy for Cassandra cluster with version {},"
          + " falling back to SEQUENTIAL repair.",
          cassandraVersion);
      repairParallelism = RepairParallelism.SEQUENTIAL;
    }
    try {
      int repairNo;
      if (cassandraVersion.startsWith("2.0") || cassandraVersion.startsWith("1.")) {
        repairNo = triggerRepairPre2dot1(
            repairParallelism,
            keyspace,
            columnFamilies,
            beginToken,
            endToken,
            datacenters.size() > 0 ? datacenters : null);
      } else if (cassandraVersion.startsWith("2.1")) {
        repairNo = triggerRepair2dot1(
            fullRepair,
            repairParallelism,
            keyspace,
            columnFamilies,
            beginToken,
            endToken,
            cassandraVersion,
            datacenters.size() > 0 ? datacenters : null);
      } else {
        repairNo = triggerRepairPost2dot2(
            fullRepair,
            repairParallelism,
            keyspace,
            columnFamilies,
            datacenters,
            associatedTokens,
            repairThreadCount);
      }
      repairStatusExecutors.putIfAbsent(repairNo, Executors.newSingleThreadExecutor());
      repairStatusHandlers.putIfAbsent(repairNo, repairStatusHandler);
      return repairNo;
    } catch (RuntimeException e) {
      LOG.error("Segment repair failed", e);
      throw new ReaperException(e);
    }
  }

  private int triggerRepairPost2dot2(
      boolean fullRepair,
      RepairParallelism repairParallelism,
      String keyspace,
      Collection<String> columnFamilies,
      Collection<String> datacenters,
      List<RingRange> associatedTokens,
      int repairThreadCount) {

    Map<String, String> options = new HashMap<>();

    options.put(RepairOption.PARALLELISM_KEY, repairParallelism.getName());
    options.put(RepairOption.INCREMENTAL_KEY, Boolean.toString(!fullRepair));
    options.put(
        RepairOption.JOB_THREADS_KEY,
        Integer.toString(repairThreadCount == 0 ? 1 : repairThreadCount));
    options.put(RepairOption.TRACE_KEY, Boolean.toString(Boolean.FALSE));
    options.put(RepairOption.COLUMNFAMILIES_KEY, StringUtils.join(columnFamilies, ","));
    // options.put(RepairOption.PULL_REPAIR_KEY, Boolean.FALSE);
    if (fullRepair) {
      options.put(
          RepairOption.RANGES_KEY,
          StringUtils.join(
              associatedTokens
                  .stream()
                  .map(token -> token.getStart() + ":" + token.getEnd())
                  .collect(Collectors.toList()),
              ","));
    }

    LOG.info("Triggering repair for ranges {}", options.get(RepairOption.RANGES_KEY));

    options.put(RepairOption.DATACENTERS_KEY, StringUtils.join(datacenters, ","));
    // options.put(RepairOption.HOSTS_KEY, StringUtils.join(specificHosts, ","));
    return ssProxy.repairAsync(keyspace, options);
  }

  private int triggerRepair2dot1(
      boolean fullRepair,
      RepairParallelism repairParallelism,
      String keyspace,
      Collection<String> columnFamilies,
      BigInteger beginToken,
      BigInteger endToken,
      String cassandraVersion,
      Collection<String> datacenters) {

    if (fullRepair) {
      // full repair
      if (repairParallelism.equals(RepairParallelism.DATACENTER_AWARE)) {
        return ssProxy
                .forceRepairRangeAsync(
                    beginToken.toString(),
                    endToken.toString(),
                    keyspace,
                    repairParallelism.ordinal(),
                    datacenters,
                    cassandraVersion.startsWith("2.2") ? new HashSet<>() : null,
                    fullRepair,
                    columnFamilies.toArray(new String[columnFamilies.size()]));
      }
      boolean snapshotRepair = repairParallelism.equals(RepairParallelism.SEQUENTIAL);

      return ssProxy
              .forceRepairRangeAsync(
                  beginToken.toString(),
                  endToken.toString(),
                  keyspace,
                  snapshotRepair
                      ? RepairParallelism.SEQUENTIAL.ordinal()
                      : RepairParallelism.PARALLEL.ordinal(),
                  datacenters,
                  cassandraVersion.startsWith("2.2") ? new HashSet<>() : null,
                  fullRepair,
                  columnFamilies.toArray(new String[columnFamilies.size()]));
    }

    // incremental repair
    return ssProxy
            .forceRepairAsync(
                keyspace,
                Boolean.FALSE,
                Boolean.FALSE,
                Boolean.FALSE,
                fullRepair,
                columnFamilies.toArray(new String[columnFamilies.size()]));
  }

  private int triggerRepairPre2dot1(
      RepairParallelism repairParallelism,
      String keyspace,
      Collection<String> columnFamilies,
      BigInteger beginToken,
      BigInteger endToken,
      Collection<String> datacenters) {

    // Cassandra 1.2 and 2.0 compatibility
    if (repairParallelism.equals(RepairParallelism.DATACENTER_AWARE)) {
      return ((StorageServiceMBean20) ssProxy)
              .forceRepairRangeAsync(
                  beginToken.toString(),
                  endToken.toString(),
                  keyspace,
                  repairParallelism.ordinal(),
                  datacenters,
                  null,
                  columnFamilies.toArray(new String[columnFamilies.size()]));
    }
    boolean snapshotRepair = repairParallelism.equals(RepairParallelism.SEQUENTIAL);

    return ((StorageServiceMBean20) ssProxy)
            .forceRepairRangeAsync(
                beginToken.toString(),
                endToken.toString(),
                keyspace,
                snapshotRepair,
                false,
                columnFamilies.toArray(new String[columnFamilies.size()]));
  }

  /**
   * Invoked when the MBean this class listens to publishes an event.
   *
   * <p>We're interested in repair-related events. Their format is explained at
   * {@link org.apache.cassandra.service.StorageServiceMBean#forceRepairAsync}. The format is:
   *    notification type: "repair"
   *    notification userData: int array of length 2 where
   *      [0] = command number
   *      [1] = ordinal of AntiEntropyService.Status
   *
   */
  @Override
  public void handleNotification(final Notification notification, Object handback) {
    // pass off the work immediately to a separate thread
    final int repairNo = "repair".equals(notification.getType())
        ? ((int[]) notification.getUserData())[0]
        : Integer.parseInt(((String) notification.getSource()).split(":")[1]);

    repairStatusExecutors.get(repairNo).submit(() -> {
      String threadName = Thread.currentThread().getName();
      try {
        String type = notification.getType();
        Thread.currentThread().setName(clusterName + "–" + type + "–" + repairNo);
        LOG.debug("Received notification: {} with type {}", notification, type);

        if (("repair").equals(type)) {
          processOldApiNotification(notification);
        } else if (("progress").equals(type)) {
          processNewApiNotification(notification);
        }
      } finally {
        Thread.currentThread().setName(threadName);
      }
    });
  }

  /**
   * Handles notifications from the old repair API (forceRepairAsync)
   */
  private void processOldApiNotification(Notification notification) {
    try {
      int[] data = (int[]) notification.getUserData();
      // get the repair sequence number
      int repairNo = data[0];
      // get the repair status
      ActiveRepairService.Status status = ActiveRepairService.Status.values()[data[1]];
      // this is some text message like "Starting repair...", "Finished repair...", etc.
      String message = notification.getMessage();
      // let the handler process the even
      if (repairStatusHandlers.containsKey(repairNo)) {
        LOG.debug("Handling notification {} with repair handler {}", notification, repairStatusHandlers.get(repairNo));

        repairStatusHandlers
            .get(repairNo)
            .handle(repairNo, Optional.of(status), Optional.empty(), message, this);
      }
    } catch (RuntimeException e) {
      LOG.error("Error while processing JMX notification", e);
    }
  }

  /**
   * Handles notifications from the new repair API (repairAsync)
   */
  private void processNewApiNotification(Notification notification) {
    Map<String, Integer> data = (Map<String, Integer>) notification.getUserData();
    try {
      // get the repair sequence number
      int repairNo = Integer.parseInt(((String) notification.getSource()).split(":")[1]);
      // get the progress status
      ProgressEventType progress = ProgressEventType.values()[data.get("type")];
      // this is some text message like "Starting repair...", "Finished repair...", etc.
      String message = notification.getMessage();
      // let the handler process the even
      if (repairStatusHandlers.containsKey(repairNo)) {
        LOG.debug("Handling notification {} with repair handler {}", notification, repairStatusHandlers.get(repairNo));

        repairStatusHandlers
            .get(repairNo)
            .handle(repairNo, Optional.empty(), Optional.of(progress), message, this);
      }
    } catch (RuntimeException e) {
      LOG.error("Error while processing JMX notification", e);
    }
  }

  private String getConnectionId() throws IOException {
    return jmxConnector.getConnectionId();
  }

  @Override
  public boolean isConnectionAlive() {
    try {
      String connectionId = getConnectionId();
      return null != connectionId && connectionId.length() > 0;
    } catch (IOException e) {
      LOG.debug("Couldn't get Connection Id", e);
      return false;
    }
  }

  @Override
  public void removeRepairStatusHandler(int repairNo) {
    repairStatusHandlers.remove(repairNo);
    ExecutorService repairStatusExecutor = repairStatusExecutors.remove(repairNo);
    if (null != repairStatusExecutor) {
      repairStatusExecutor.shutdown();
    }
  }

  /** Cleanly shut down by un-registering the listener and closing the JMX connection. */
  @Override
  public void close() {
    try {
      mbeanServer.removeNotificationListener(ObjectNames.STORAGE_SERVICE, this);
      mbeanServer.removeNotificationListener(ObjectNames.STREAM_MANAGER, this);
      LOG.debug("Successfully removed notification listeners for '{}'", host);
    } catch (InstanceNotFoundException | ListenerNotFoundException | IOException e) {
      LOG.debug("failed on removing notification listener", e);
    }
    try {
      jmxConnector.close();
    } catch (IOException e) {
      LOG.warn("failed closing a JMX connection", e);
    }
  }

  /**
   * Compares two Cassandra versions using classes provided by the Datastax Java Driver.
   *
   * @param str1 a string of ordinal numbers separated by decimal points.
   * @param str2 a string of ordinal numbers separated by decimal points.
   * @return The result is a negative integer if str1 is _numerically_ less than str2. The result is
   *     a positive integer if str1 is _numerically_ greater than str2. The result is zero if the
   *     strings are _numerically_ equal. It does not work if "1.10" is supposed to be equal to
   *     "1.10.0".
   */
  static Integer versionCompare(String str1, String str2) {
    VersionNumber version1 = VersionNumber.parse(str1);
    VersionNumber version2 = VersionNumber.parse(str2);

    return version1.compareTo(version2);
  }

  @Override
  public List<String> getLiveNodes() throws ReaperException {
    Preconditions.checkNotNull(ssProxy, "Looks like the proxy is not connected");
    try {
      return ssProxy.getLiveNodes();
    } catch (RuntimeException e) {
      LOG.error(e.getMessage());
      throw new ReaperException(e.getMessage(), e);
    }
  }

  private static RMIClientSocketFactory getRmiClientSocketFactory() {
    return Boolean.parseBoolean(System.getProperty("ssl.enable"))
        ? new SslRMIClientSocketFactory()
        : RMISocketFactory.getDefaultSocketFactory();
  }

  private static final class JmxColumnFamily {
    private final String keyspace;
    private final String columnFamily;

    JmxColumnFamily(String keyspace, String columnFamily) {
      super();
      this.keyspace = keyspace;
      this.columnFamily = columnFamily;
    }

    public String getKeyspace() {
      return keyspace;
    }

    public String getColumnFamily() {
      return columnFamily;
    }
  }

  private void registerConnectionsGauge() {
    try {
      if (!metricRegistry
          .getGauges()
          .containsKey(
              MetricRegistry.name(
                  JmxProxyImpl.class,
                  clusterName.replaceAll("[^A-Za-z0-9]", ""),
                  host.replace('.', 'x').replaceAll("[^A-Za-z0-9]", ""),
                  "repairStatusHandlers"))) {

        metricRegistry.register(
            MetricRegistry.name(
                JmxProxyImpl.class,
                clusterName.replaceAll("[^A-Za-z0-9]", ""),
                host.replace('.', 'x').replaceAll("[^A-Za-z0-9]", ""),
                "repairStatusHandlers"),
            (Gauge<Integer>) () -> repairStatusHandlers.size());
      }
    } catch (IllegalArgumentException e) {
      LOG.warn("Cannot create connection gauge for node {}", host, e);
    }
  }

  StorageServiceMBean getStorageServiceMBean() {
    return ssProxy;
  }

  MBeanServerConnection getMBeanServerConnection() {
    return mbeanServer;
  }

  CompactionManagerMBean getCompactionManagerMBean() {
    return cmProxy;
  }

  Optional<StreamManagerMBean> getStreamManagerMBean() {
    return smProxy;
  }

  FailureDetectorMBean getFailureDetectorMBean() {
    return fdProxy;
  }

  EndpointSnitchInfoMBean getEndpointSnitchInfoMBean() {
    return endpointSnitchMbean;
  }

  String getUntranslatedHost() {
    return hostBeforeTranslation;
  }

  // Initialization-on-demand holder for jmx ObjectNames
  private static final class ObjectNames {

    static final ObjectName STORAGE_SERVICE;
    static final ObjectName COMPACTION_MANAGER;
    static final ObjectName FAILURE_DETECTOR;
    static final ObjectName STREAM_MANAGER;
    static final ObjectName ENDPOINT_SNITCH_INFO;
    static final ObjectName ANTI_ENTROPY_SESSIONS;
    static final ObjectName COMPACTIONS_PENDING;
    static final ObjectName COLUMN_FAMILIES;
    static final ObjectName TP_VALIDATIONS_ACTIVE;
    static final ObjectName TP_VALIDATIONS_PENDING;
    static final ObjectName INTERNALS;

    static {
      try {
        STORAGE_SERVICE = new ObjectName("org.apache.cassandra.db:type=StorageService");
        COMPACTION_MANAGER = new ObjectName(CompactionManager.MBEAN_OBJECT_NAME);
        FAILURE_DETECTOR = new ObjectName(FailureDetector.MBEAN_NAME);
        STREAM_MANAGER = new ObjectName(StreamManagerMBean.OBJECT_NAME);
        ENDPOINT_SNITCH_INFO = new ObjectName("org.apache.cassandra.db:type=EndpointSnitchInfo");
        ANTI_ENTROPY_SESSIONS = new ObjectName("org.apache.cassandra.internal:type=AntiEntropySessions");
        COMPACTIONS_PENDING = new ObjectName("org.apache.cassandra.metrics:type=Compaction,name=PendingTasks");
        COLUMN_FAMILIES = new ObjectName("org.apache.cassandra.db:type=ColumnFamilies,keyspace=*,columnfamily=*");
        INTERNALS = new ObjectName("org.apache.cassandra.internal:*");

        TP_VALIDATIONS_ACTIVE = new ObjectName(
            "org.apache.cassandra.metrics:type=ThreadPools,path=internal,scope=ValidationExecutor,name=ActiveTasks");

        TP_VALIDATIONS_PENDING = new ObjectName(
            "org.apache.cassandra.metrics:type=ThreadPools,path=internal,scope=ValidationExecutor,name=PendingTasks");

      } catch (MalformedObjectNameException e) {
        throw new IllegalStateException("Failure during preparations for JMX connection", e);
      }
    }
  }
}
