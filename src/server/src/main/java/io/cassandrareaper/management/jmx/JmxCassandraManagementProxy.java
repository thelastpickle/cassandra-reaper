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

package io.cassandrareaper.management.jmx;

import io.cassandrareaper.ReaperApplicationConfiguration.Jmxmp;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.JmxCredentials;
import io.cassandrareaper.core.Table;
import io.cassandrareaper.crypto.Cryptograph;
import io.cassandrareaper.management.ICassandraManagementProxy;
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
import javax.management.AttributeList;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.JMException;
import javax.management.JMX;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.QueryExp;
import javax.management.ReflectionException;
import javax.management.openmbean.TabularData;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.rmi.ssl.SslRMIClientSocketFactory;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.validation.constraints.NotNull;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.VersionNumber;
import com.datastax.driver.core.policies.AddressTranslator;
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


final class JmxCassandraManagementProxy implements ICassandraManagementProxy {

  private static final Logger LOG = LoggerFactory.getLogger(ICassandraManagementProxy.class);

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
  private final DiagnosticEventPersistenceMBean diagEventProxy;
  private final LastEventIdBroadcasterMBean lastEventIdProxy;
  private final Jmxmp jmxmp;

  private JmxCassandraManagementProxy(
      String host,
      String hostBeforeTranslation,
      JMXConnector jmxConnector,
      StorageServiceMBean ssProxy,
      MBeanServerConnection mbeanServer,
      CompactionManagerMBean cmProxy,
      EndpointSnitchInfoMBean endpointSnitchMbean,
      FailureDetectorMBean fdProxy,
      MetricRegistry metricRegistry,
      Optional<StreamManagerMBean> smProxy,
      DiagnosticEventPersistenceMBean diagEventProxy,
      LastEventIdBroadcasterMBean lastEventIdProxy,
      Jmxmp jmxmp) {

    this.host = host;
    this.hostBeforeTranslation = hostBeforeTranslation;
    this.jmxConnector = jmxConnector;
    this.mbeanServer = mbeanServer;
    this.ssProxy = ssProxy;
    this.cmProxy = cmProxy;
    this.endpointSnitchMbean = endpointSnitchMbean;
    this.clusterName = Cluster.toSymbolicName(ssProxy.getClusterName());
    this.fdProxy = fdProxy;
    this.diagEventProxy = diagEventProxy;
    this.lastEventIdProxy = lastEventIdProxy;
    this.metricRegistry = metricRegistry;
    this.smProxy = smProxy;
    this.jmxmp = jmxmp;
    registerConnectionsGauge();
  }

  static ICassandraManagementProxy connect(
      String host,
      Optional<JmxCredentials> jmxCredentials,
      final AddressTranslator addressTranslator,
      int connectionTimeout,
      MetricRegistry metricRegistry,
      Cryptograph cryptograph,
      Jmxmp jmxmp)
      throws ReaperException, InterruptedException {

    if (host == null) {
      throw new ReaperException("Null host given to JmxProxy.connect()");
    }

    final HostAndPort hostAndPort = HostAndPort.fromString(host);

    return connect(
        hostAndPort.getHost(),
        hostAndPort.getPortOrDefault(Cluster.DEFAULT_JMX_PORT),
        jmxCredentials,
        addressTranslator,
        connectionTimeout,
        metricRegistry,
        cryptograph,
        jmxmp);
  }

  /**
   * Connect to JMX interface on the given host and port.
   *
   * @param originalHost      hostname or ip address of Cassandra node
   * @param port              port number to use for JMX connection
   * @param jmxCredentials    credentials to use for JMX authentication
   * @param addressTranslator if EC2MultiRegionAddressTranslator isn't null it will be used to
   *                          translate addresses
   */
  private static ICassandraManagementProxy connect(
      String originalHost,
      int port,
      Optional<JmxCredentials> jmxCredentials,
      final AddressTranslator addressTranslator,
      int connectionTimeout,
      MetricRegistry metricRegistry,
      Cryptograph cryptograph,
      Jmxmp jmxmp) throws ReaperException, InterruptedException {

    JMXServiceURL jmxUrl;
    String host = originalHost;

    if (addressTranslator != null) {
      host = addressTranslator.translate(new InetSocketAddress(host, port)).getAddress().getHostAddress();
      LOG.debug("translated {} to {}", originalHost, host);
    }

    try {
      LOG.debug("Connecting to {}...", host);
      jmxUrl = JmxAddresses.getJmxServiceUrl(host, port, jmxmp.isEnabled());
    } catch (MalformedURLException e) {
      LOG.error(String.format("Failed to prepare the JMX connection to %s:%s", host, port));
      throw new ReaperException("Failure during preparations for JMX connection", e);
    }
    try {
      final Map<String, Object> env = new HashMap<>();
      if (jmxmp.useSsl() && jmxCredentials.isPresent()) {
        String[] creds = {jmxCredentials.get().getUsername(), jmxCredentials.get().getPassword()};
        env.put(JMXConnector.CREDENTIALS, creds);
        LOG.debug("Use SSL with profile 'TLS SASL/PLAIN' with JMXMP");
        env.put("jmx.remote.profiles", "TLS SASL/PLAIN");
        env.put("jmx.remote.sasl.callback.handler",
            new UserPasswordCallbackHandler(jmxCredentials.get().getUsername(), jmxCredentials.get().getPassword()));
      } else {
        if (jmxCredentials.isPresent()) {
          String jmxPassword = cryptograph.decrypt(jmxCredentials.get().getPassword());
          String[] creds = {jmxCredentials.get().getUsername(), jmxPassword};
          env.put(JMXConnector.CREDENTIALS, creds);
        }
        env.put("com.sun.jndi.rmi.factory.socket", getRmiClientSocketFactory());
      }
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

      ICassandraManagementProxy proxy
          = new JmxCassandraManagementProxy(
          host,
          originalHost,
          jmxConn,
          ssProxy,
          mbeanServerConn,
          JMX.newMBeanProxy(mbeanServerConn, ObjectNames.COMPACTION_MANAGER, CompactionManagerMBean.class),
          JMX.newMBeanProxy(mbeanServerConn, ObjectNames.ENDPOINT_SNITCH_INFO, EndpointSnitchInfoMBean.class),
          JMX.newMBeanProxy(mbeanServerConn, ObjectNames.FAILURE_DETECTOR, FailureDetectorMBean.class),
          metricRegistry,
          smProxy,
          JMX.newMBeanProxy(mbeanServerConn, ObjectNames.DIAGNOSTICS_EVENTS, DiagnosticEventPersistenceMBean.class),
          JMX.newMBeanProxy(mbeanServerConn, ObjectNames.LAST_EVENT_ID, LastEventIdBroadcasterMBean.class),
          jmxmp);

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

  private static RMIClientSocketFactory getRmiClientSocketFactory() {
    return Boolean.parseBoolean(System.getProperty("ssl.enable"))
        ? new SslRMIClientSocketFactory()
        : RMISocketFactory.getDefaultSocketFactory();
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

  @NotNull
  @Override
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
  public Set<Table> getTablesForKeyspace(String keyspace) throws ReaperException {
    final boolean canUseCompactionStrategy = versionCompare(getCassandraVersion(), "2.1") >= 0;

    final Set<Table> tables = new HashSet<>();
    final Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> proxies;
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

        Table.Builder tableBuilder = Table.builder()
            .withName(columnFamilyMBean.getColumnFamilyName());

        if (canUseCompactionStrategy) {
          tableBuilder.withCompactionStrategy(columnFamilyMBean.getCompactionParameters().get("class"));
        }

        tables.add(tableBuilder.build());
      }
    }
    return tables;
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
      return getRunningRepairMetricsPost22().isEmpty()
          ? false
          : true;
    } catch (RuntimeException e) {
      LOG.error(ERROR_GETTING_ATTR_JMX, e);
    }
    // If uncertain, assume it's running
    return true;
  }

  @Override
  public List<String> getRunningRepairMetricsPost22() {
    List<String> repairMbeans = Lists.newArrayList();
    try {
      // list all mbeans in search of one with the name Repair#??
      // This is the replacement for AntiEntropySessions since Cassandra 2.2
      Set beanSet = mbeanServer.queryNames(ObjectNames.INTERNALS, null);
      for (Object bean : beanSet) {
        ObjectName objName = (ObjectName) bean;
        if (objName.getCanonicalName().contains("Repair#")) {
          repairMbeans.add(objName.getCanonicalName());
        }
      }
    } catch (IOException ignored) {
      LOG.warn(FAILED_TO_CONNECT_TO_USING_JMX, host, ignored);
    } catch (RuntimeException e) {
      LOG.error(ERROR_GETTING_ATTR_JMX, e);
    }
    // If uncertain, assume it's running
    return repairMbeans;
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
    final boolean canUseDatacenterAware = versionCompare(cassandraVersion, "2.0.12") >= 0;

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
   * notification type: "repair"
   * notification userData: int array of length 2 where
   * [0] = command number
   * [1] = ordinal of AntiEntropyService.Status
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
    } catch (NumberFormatException e) {
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

  /**
   * Cleanly shut down by un-registering the listener and closing the JMX connection.
   */
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

  private void registerConnectionsGauge() {
    try {
      if (!metricRegistry
          .getGauges()
          .containsKey(
              MetricRegistry.name(
                  JmxCassandraManagementProxy.class,
                  clusterName.replaceAll("[^A-Za-z0-9]", ""),
                  host.replace('.', 'x').replaceAll("[^A-Za-z0-9]", ""),
                  "repairStatusHandlers"))) {

        metricRegistry.register(
            MetricRegistry.name(
                JmxCassandraManagementProxy.class,
                clusterName.replaceAll("[^A-Za-z0-9]", ""),
                host.replace('.', 'x').replaceAll("[^A-Za-z0-9]", ""),
                "repairStatusHandlers"),
            (Gauge<Integer>) () -> repairStatusHandlers.size());
      }
    } catch (IllegalArgumentException e) {
      LOG.warn("Cannot create connection gauge for node {}", host, e);
    }
  }

  private StorageServiceMBean getStorageServiceMBean() {
    return ssProxy;
  }

  private MBeanServerConnection getMBeanServerConnection() {
    return mbeanServer;
  }

  private CompactionManagerMBean getCompactionManagerMBean() {
    return cmProxy;
  }

  Optional<StreamManagerMBean> getStreamManagerMBean() {
    return smProxy;
  }

  private FailureDetectorMBean getFailureDetectorMBean() {
    return fdProxy;
  }

  EndpointSnitchInfoMBean getEndpointSnitchInfoMBean() {
    return endpointSnitchMbean;
  }

  DiagnosticEventPersistenceMBean getDiagnosticEventPersistenceMBean() {
    return diagEventProxy;
  }

  LastEventIdBroadcasterMBean getLastEventIdBroadcasterMBean() {
    return lastEventIdProxy;
  }

  String getUntranslatedHost() {
    return hostBeforeTranslation;
  }

  void addConnectionNotificationListener(NotificationListener listener) {
    jmxConnector.addConnectionNotificationListener(listener, null, null);
  }

  void removeConnectionNotificationListener(NotificationListener listener) throws ListenerNotFoundException {
    jmxConnector.removeConnectionNotificationListener(listener);
  }

  void addNotificationListener(NotificationListener listener, NotificationFilter filter)
      throws IOException, JMException {

    jmxConnector.getMBeanServerConnection()
        .addNotificationListener(ObjectNames.LAST_EVENT_ID, listener, filter, null);
  }

  void removeNotificationListener(NotificationListener listener) throws IOException, JMException {
    jmxConnector.getMBeanServerConnection().removeNotificationListener(ObjectNames.LAST_EVENT_ID, listener);
  }

  // From storageServiceMbean
  public void clearSnapshot(String var1, String... var2) throws IOException {
    this.getStorageServiceMBean().clearSnapshot(var1, var2);
  }

  public Map<String, TabularData> getSnapshotDetails() {
    return this.getStorageServiceMBean().getSnapshotDetails();
  }

  public void takeSnapshot(String var1, String... var2) throws IOException {
    this.getStorageServiceMBean().takeSnapshot(var1, var2);
  }

  public void takeColumnFamilySnapshot(String var1, String var2, String var3) throws IOException {
    this.getStorageServiceMBean().takeColumnFamilySnapshot(var1, var2, var3);
  }

  public Map<String, String> getTokenToEndpointMap() {
    return this.getStorageServiceMBean().getTokenToEndpointMap();
  }

  public void forceKeyspaceCompaction(boolean var1, String var2, String... var3) throws IOException, ExecutionException,
      InterruptedException {
    this.getStorageServiceMBean().forceKeyspaceCompaction(var1, var2, var3);
  }

  // From MBeanServerConnection
  public Set<ObjectName> queryNames(ObjectName name, QueryExp query)
      throws IOException {
    return getMBeanServerConnection().queryNames(name, query);
  }

  public MBeanInfo getMBeanInfo(ObjectName name)
      throws InstanceNotFoundException, IntrospectionException,
      ReflectionException, IOException {
    return getMBeanServerConnection().getMBeanInfo(name);
  }

  public AttributeList getAttributes(ObjectName name, String[] attributes)
      throws InstanceNotFoundException, ReflectionException,
      IOException {
    return getMBeanServerConnection().getAttributes(name, attributes);
  }

  // From CompactionManagerMBean
  public List<Map<String, String>> getCompactions() {
    return getCompactionManagerMBean().getCompactions();
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

  // From FailureDetectorMBean
  public String getAllEndpointStates() {
    return getFailureDetectorMBean().getAllEndpointStates();
  }

  public Map<String, String> getSimpleStates() {
    return getFailureDetectorMBean().getSimpleStates();
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
    static final ObjectName DIAGNOSTICS_EVENTS;
    static final ObjectName LAST_EVENT_ID;

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
        DIAGNOSTICS_EVENTS = new ObjectName("org.apache.cassandra.diag:type=DiagnosticEventService");
        LAST_EVENT_ID = new ObjectName("org.apache.cassandra.diag:type=LastEventIdBroadcaster");

        TP_VALIDATIONS_ACTIVE = new ObjectName(
            "org.apache.cassandra.metrics:type=ThreadPools,path=internal,scope=ValidationExecutor,name=ActiveTasks");

        TP_VALIDATIONS_PENDING = new ObjectName(
            "org.apache.cassandra.metrics:type=ThreadPools,path=internal,scope=ValidationExecutor,name=PendingTasks");

      } catch (MalformedObjectNameException e) {
        throw new IllegalStateException("Failure during preparations for JMX connection", e);
      }
    }
  }

  static class UserPasswordCallbackHandler implements javax.security.auth.callback.CallbackHandler {
    private String username;
    private char[] password;

    UserPasswordCallbackHandler(String user, String password) {
      this.username = user;
      this.password = password == null ? null : password.toCharArray();
    }

    public void handle(javax.security.auth.callback.Callback[] callbacks)
        throws IOException, javax.security.auth.callback.UnsupportedCallbackException {
      for (int i = 0; i < callbacks.length; i++) {
        if (callbacks[i] instanceof PasswordCallback) {
          PasswordCallback pcb = (PasswordCallback) callbacks[i];
          pcb.setPassword(password);
        } else if (callbacks[i] instanceof javax.security.auth.callback.NameCallback) {
          NameCallback ncb = (NameCallback) callbacks[i];
          ncb.setName(username);
        } else {
          throw new UnsupportedCallbackException(callbacks[i]);
        }
      }
    }
  }


}