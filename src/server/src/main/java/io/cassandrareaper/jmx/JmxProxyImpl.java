/*
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
import io.cassandrareaper.core.Snapshot;
import io.cassandrareaper.core.Snapshot.Builder;
import io.cassandrareaper.service.RingRange;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMISocketFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.JMX;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.openmbean.TabularData;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.rmi.ssl.SslRMIClientSocketFactory;
import javax.validation.constraints.NotNull;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.VersionNumber;
import com.datastax.driver.core.policies.EC2MultiRegionAddressTranslator;
import com.google.common.base.Optional;
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
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

final class JmxProxyImpl implements JmxProxy {

  private static final Logger LOG = LoggerFactory.getLogger(JmxProxy.class);

  private static final int JMX_PORT = 7199;
  private static final String SS_OBJECT_NAME = "org.apache.cassandra.db:type=StorageService";
  private static final String AES_OBJECT_NAME = "org.apache.cassandra.internal:type=AntiEntropySessions";
  private static final String VALIDATION_ACTIVE_OBJECT_NAME
      = "org.apache.cassandra.metrics:type=ThreadPools,path=internal,scope=ValidationExecutor,name=ActiveTasks";
  private static final String VALIDATION_PENDING_OBJECT_NAME
      = "org.apache.cassandra.metrics:type=ThreadPools,path=internal,scope=ValidationExecutor,name=PendingTasks";
  private static final String COMP_OBJECT_NAME = "org.apache.cassandra.metrics:type=Compaction,name=PendingTasks";
  private static final String VALUE_ATTRIBUTE = "Value";
  private static final String FAILED_TO_CONNECT_TO_USING_JMX = "Failed to connect to {} using JMX";
  private static final String ERROR_GETTING_ATTR_JMX = "Error getting attribute from JMX";
  private static final long KB_FACTOR = 1000;
  private static final long KIB_FACTOR = 1024;
  private static final long MB_FACTOR = 1000 * KB_FACTOR;
  private static final long MIB_FACTOR = 1024 * KIB_FACTOR;
  private static final long GB_FACTOR = 1000 * MB_FACTOR;
  private static final long GIB_FACTOR = 1024 * MIB_FACTOR;


  private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();

  private final JMXConnector jmxConnector;
  private final ObjectName ssMbeanName;
  private final MBeanServerConnection mbeanServer;
  private final CompactionManagerMBean cmProxy;
  private final EndpointSnitchInfoMBean endpointSnitchMbean;
  private final Object ssProxy;
  private final Object fdProxy;
  private final String host;
  private final String hostBeforeTranslation;
  private final JMXServiceURL jmxUrl;
  private final String clusterName;
  private final ConcurrentMap<Integer, RepairStatusHandler> repairStatusHandlers =
      Maps.newConcurrentMap();
  private final MetricRegistry metricRegistry;

  private JmxProxyImpl(
      String host,
      String hostBeforeTranslation,
      JMXServiceURL jmxUrl,
      JMXConnector jmxConnector,
      Object ssProxy,
      ObjectName ssMbeanName,
      MBeanServerConnection mbeanServer,
      CompactionManagerMBean cmProxy,
      EndpointSnitchInfoMBean endpointSnitchMbean,
      FailureDetectorMBean fdProxy,
      MetricRegistry metricRegistry) {

    this.host = host;
    this.hostBeforeTranslation = hostBeforeTranslation;
    this.jmxUrl = jmxUrl;
    this.jmxConnector = jmxConnector;
    this.ssMbeanName = ssMbeanName;
    this.mbeanServer = mbeanServer;
    this.ssProxy = ssProxy;
    this.cmProxy = cmProxy;
    this.endpointSnitchMbean = endpointSnitchMbean;
    this.clusterName = Cluster.toSymbolicName(((StorageServiceMBean) ssProxy).getClusterName());
    this.fdProxy = fdProxy;
    this.metricRegistry = metricRegistry;
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
          hostAndPort.getHostText(),
          hostAndPort.getPortOrDefault(JMX_PORT),
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
   * @param host hostname or ip address of Cassandra node
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
      MetricRegistry metricRegistry)
      throws ReaperException, InterruptedException {

    ObjectName ssMbeanName;
    ObjectName cmMbeanName;
    ObjectName fdMbeanName;
    ObjectName endpointSnitchMbeanName;
    JMXServiceURL jmxUrl;
    String host = originalHost;

    if (addressTranslator != null) {
      host = addressTranslator.translate(new InetSocketAddress(host, port)).getAddress().getHostAddress();
      LOG.debug("translated {} to {}", originalHost, host);
    }

    try {
      LOG.debug("Connecting to {}...", host);
      jmxUrl = JmxAddresses.getJmxServiceUrl(host, port);
      ssMbeanName = new ObjectName(SS_OBJECT_NAME);
      cmMbeanName = new ObjectName(CompactionManager.MBEAN_OBJECT_NAME);
      fdMbeanName = new ObjectName(FailureDetector.MBEAN_NAME);
      endpointSnitchMbeanName = new ObjectName("org.apache.cassandra.db:type=EndpointSnitchInfo");
    } catch (MalformedURLException | MalformedObjectNameException e) {
      LOG.error(String.format("Failed to prepare the JMX connection to %s:%s", host, port));
      throw new ReaperException("Failure during preparations for JMX connection", e);
    }
    try {
      Map<String, Object> env = new HashMap<>();
      if (username != null && password != null) {
        String[] creds = {username, password};
        env.put(JMXConnector.CREDENTIALS, creds);
      }
      env.put("com.sun.jndi.rmi.factory.socket", getRmiClientSocketFactory());
      JMXConnector jmxConn = connectWithTimeout(jmxUrl, connectionTimeout, TimeUnit.SECONDS, env);
      MBeanServerConnection mbeanServerConn = jmxConn.getMBeanServerConnection();
      Object ssProxy = JMX.newMBeanProxy(mbeanServerConn, ssMbeanName, StorageServiceMBean.class);
      String cassandraVersion = ((StorageServiceMBean) ssProxy).getReleaseVersion();
      if (cassandraVersion.startsWith("2.0") || cassandraVersion.startsWith("1.")) {
        ssProxy = JMX.newMBeanProxy(mbeanServerConn, ssMbeanName, StorageServiceMBean20.class);
      }

      CompactionManagerMBean cmProxy = JMX.newMBeanProxy(mbeanServerConn, cmMbeanName, CompactionManagerMBean.class);
      FailureDetectorMBean fdProxy = JMX.newMBeanProxy(mbeanServerConn, fdMbeanName, FailureDetectorMBean.class);

      EndpointSnitchInfoMBean endpointSnitchProxy
          = JMX.newMBeanProxy(mbeanServerConn, endpointSnitchMbeanName, EndpointSnitchInfoMBean.class);

      JmxProxy proxy =
          new JmxProxyImpl(
              host,
              originalHost,
              jmxUrl,
              jmxConn,
              ssProxy,
              ssMbeanName,
              mbeanServerConn,
              cmProxy,
              endpointSnitchProxy,
              fdProxy,
              metricRegistry);

      // registering a listener throws bunch of exceptions, so we do it here rather than in the
      // constructor
      mbeanServerConn.addNotificationListener(ssMbeanName, proxy, null, null);
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
  public String getDataCenter() {
    return getDataCenter(hostBeforeTranslation);
  }

  @Override
  public String getDataCenter(String host) {
    try {
      return endpointSnitchMbean.getDatacenter(host);
    } catch (UnknownHostException ex) {
      throw new IllegalArgumentException(ex);
    }
  }

  @Override
  public List<BigInteger> getTokens() {
    checkNotNull(ssProxy, "Looks like the proxy is not connected");

    return Lists.transform(
        Lists.newArrayList(((StorageServiceMBean) ssProxy).getTokenToEndpointMap().keySet()), s -> new BigInteger(s));
  }

  @Override
  public Map<List<String>, List<String>> getRangeToEndpointMap(String keyspace) throws ReaperException {
    checkNotNull(ssProxy, "Looks like the proxy is not connected");
    try {
      return ((StorageServiceMBean) ssProxy).getRangeToEndpointMap(keyspace);
    } catch (RuntimeException e) {
      LOG.error(e.getMessage());
      throw new ReaperException(e.getMessage(), e);
    }
  }

  @Override
  public List<RingRange> getRangesForLocalEndpoint(String keyspace) throws ReaperException {
    checkNotNull(ssProxy, "Looks like the proxy is not connected");
    List<RingRange> localRanges = Lists.newArrayList();
    try {
      Map<List<String>, List<String>> ranges = ((StorageServiceMBean) ssProxy).getRangeToEndpointMap(keyspace);
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
      return ((StorageServiceMBean) ssProxy)
          .getHostIdToEndpoint()
          .get(((StorageServiceMBean) ssProxy).getLocalHostId());
    } else {
      // pre-2.1.10 compatibility
      BiMap<String, String> hostIdBiMap =
          ImmutableBiMap.copyOf(((StorageServiceMBean) ssProxy).getHostIdMap());
      String localHostId = ((StorageServiceMBean) ssProxy).getLocalHostId();
      return hostIdBiMap.inverse().get(localHostId);
    }
  }

  @NotNull
  @Override
  public List<String> tokenRangeToEndpoint(String keyspace, Segment segment) {
    checkNotNull(ssProxy, "Looks like the proxy is not connected");

    Set<Map.Entry<List<String>, List<String>>> entries
        = ((StorageServiceMBean) ssProxy).getRangeToEndpointMap(keyspace).entrySet();

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
    checkNotNull(ssProxy, "Looks like the proxy is not connected");
    Map<String, String> hosts;
    try {
      hosts = ((StorageServiceMBean) ssProxy).getEndpointToHostId();
    } catch (UndeclaredThrowableException e) {
      hosts = ((StorageServiceMBean) ssProxy).getHostIdMap();
    }
    return hosts;
  }

  @Override
  public String getPartitioner() {
    checkNotNull(ssProxy, "Looks like the proxy is not connected");
    return ((StorageServiceMBean) ssProxy).getPartitionerName();
  }

  @Override
  public String getClusterName() {
    checkNotNull(ssProxy, "Looks like the proxy is not connected");
    return ((StorageServiceMBean) ssProxy).getClusterName();
  }

  @Override
  public List<String> getKeyspaces() {
    checkNotNull(ssProxy, "Looks like the proxy is not connected");
    return ((StorageServiceMBean) ssProxy).getKeyspaces();
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
  public int getPendingCompactions() throws MBeanException, AttributeNotFoundException, ReflectionException {
    checkNotNull(cmProxy, "Looks like the proxy is not connected");
    try {
      ObjectName name = new ObjectName(COMP_OBJECT_NAME);
      int pendingCount = (int) mbeanServer.getAttribute(name, VALUE_ATTRIBUTE);
      return pendingCount;
    } catch (IOException ignored) {
      LOG.warn(FAILED_TO_CONNECT_TO_USING_JMX, host, ignored);
    } catch (MalformedObjectNameException ignored) {
      LOG.error("Internal error, malformed name", ignored);
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
  public boolean isRepairRunning() throws MBeanException, AttributeNotFoundException, ReflectionException {
    return isRepairRunningPre22() || isRepairRunningPost22() || isValidationCompactionRunning();
  }

  /**
   * @return true if any repairs are running on the node.
   */
  private boolean isRepairRunningPre22() throws MBeanException, AttributeNotFoundException, ReflectionException {
    // Check if AntiEntropySession is actually running on the node
    try {
      ObjectName name = new ObjectName(AES_OBJECT_NAME);
      int activeCount = (Integer) mbeanServer.getAttribute(name, "ActiveCount");
      long pendingCount = (Long) mbeanServer.getAttribute(name, "PendingTasks");
      return activeCount + pendingCount != 0;
    } catch (IOException ignored) {
      LOG.warn(FAILED_TO_CONNECT_TO_USING_JMX, host, ignored);
    } catch (MalformedObjectNameException ignored) {
      LOG.error("Internal error, malformed name", ignored);
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
  private boolean isValidationCompactionRunning()
      throws MBeanException, AttributeNotFoundException, ReflectionException {

    // Check if AntiEntropySession is actually running on the node
    try {
      int activeCount
          = (Integer) mbeanServer.getAttribute(new ObjectName(VALIDATION_ACTIVE_OBJECT_NAME), VALUE_ATTRIBUTE);

      long pendingCount
          = (Long) mbeanServer.getAttribute(new ObjectName(VALIDATION_PENDING_OBJECT_NAME), VALUE_ATTRIBUTE);

      return activeCount + pendingCount != 0;
    } catch (IOException ignored) {
      LOG.warn(FAILED_TO_CONNECT_TO_USING_JMX, host, ignored);
    } catch (MalformedObjectNameException ignored) {
      LOG.error("Internal error, malformed name", ignored);
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
      Set beanSet = mbeanServer.queryNames(new ObjectName("org.apache.cassandra.internal:*"), null);
      for (Object bean : beanSet) {
        ObjectName objName = (ObjectName) bean;
        if (objName.getCanonicalName().contains("Repair#")) {
          return true;
        }
      }
      return false;
    } catch (IOException ignored) {
      LOG.warn(FAILED_TO_CONNECT_TO_USING_JMX, host, ignored);
    } catch (MalformedObjectNameException ignored) {
      LOG.error("Internal error, malformed name", ignored);
    } catch (RuntimeException e) {
      LOG.error(ERROR_GETTING_ATTR_JMX, e);
    }
    // If uncertain, assume it's running
    return true;
  }

  @Override
  public void cancelAllRepairs() {
    checkNotNull(ssProxy, "Looks like the proxy is not connected");
    try {
      ((StorageServiceMBean) ssProxy).forceTerminateAllRepairSessions();
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
      Set<ObjectName> beanSet =
          mbeanServer.queryNames(
              new ObjectName(
                  "org.apache.cassandra.db:type=ColumnFamilies,keyspace=*,columnfamily=*"),
              null);

      tablesByKeyspace =
          beanSet
              .stream()
              .map(
                  bean ->
                      new JmxColumnFamily(
                          bean.getKeyProperty("keyspace"), bean.getKeyProperty("columnfamily")))
              .collect(
                  Collectors.groupingBy(
                      JmxColumnFamily::getKeyspace,
                      Collectors.mapping(JmxColumnFamily::getColumnFamily, Collectors.toList())));

    } catch (MalformedObjectNameException | IOException e) {
      LOG.warn("Couldn't get a list of tables through JMX", e);
    }

    return Collections.unmodifiableMap(tablesByKeyspace);
  }

  @Override
  public String getCassandraVersion() {
    return ((StorageServiceMBean) ssProxy).getReleaseVersion();
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
      List<RingRange> associatedTokens)
      throws ReaperException {

    checkNotNull(ssProxy, "Looks like the proxy is not connected");
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
      if (cassandraVersion.startsWith("2.0") || cassandraVersion.startsWith("1.")) {
        return triggerRepairPre2dot1(
            repairParallelism,
            keyspace,
            columnFamilies,
            beginToken,
            endToken,
            datacenters.size() > 0 ? datacenters : null,
            repairStatusHandler);
      } else if (cassandraVersion.startsWith("2.1")) {
        return triggerRepair2dot1(
            fullRepair,
            repairParallelism,
            keyspace,
            columnFamilies,
            beginToken,
            endToken,
            cassandraVersion,
            datacenters.size() > 0 ? datacenters : null,
            repairStatusHandler);
      } else {
        return triggerRepairPost2dot2(
            fullRepair,
            repairParallelism,
            keyspace,
            columnFamilies,
            beginToken,
            endToken,
            cassandraVersion,
            datacenters,
            repairStatusHandler,
            associatedTokens);
      }
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
      BigInteger beginToken,
      BigInteger endToken,
      String cassandraVersion,
      Collection<String> datacenters,
      RepairStatusHandler repairStatusHandler,
      List<RingRange> associatedTokens) {

    Map<String, String> options = new HashMap<>();

    options.put(RepairOption.PARALLELISM_KEY, repairParallelism.getName());
    options.put(RepairOption.INCREMENTAL_KEY, Boolean.toString(!fullRepair));
    options.put(RepairOption.JOB_THREADS_KEY, Integer.toString(1));
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
    int commandId = ((StorageServiceMBean) ssProxy).repairAsync(keyspace, options);
    repairStatusHandlers.putIfAbsent(commandId, repairStatusHandler);

    return commandId;
  }

  private int triggerRepair2dot1(
      boolean fullRepair,
      RepairParallelism repairParallelism,
      String keyspace,
      Collection<String> columnFamilies,
      BigInteger beginToken,
      BigInteger endToken,
      String cassandraVersion,
      Collection<String> datacenters,
      RepairStatusHandler repairStatusHandler) {

    if (fullRepair) {
      // full repair
      if (repairParallelism.equals(RepairParallelism.DATACENTER_AWARE)) {
        int commandId =
            ((StorageServiceMBean) ssProxy)
                .forceRepairRangeAsync(
                    beginToken.toString(),
                    endToken.toString(),
                    keyspace,
                    repairParallelism.ordinal(),
                    datacenters,
                    cassandraVersion.startsWith("2.2") ? new HashSet<String>() : null,
                    fullRepair,
                    columnFamilies.toArray(new String[columnFamilies.size()]));

        repairStatusHandlers.putIfAbsent(commandId, repairStatusHandler);
        return commandId;
      }

      boolean snapshotRepair = repairParallelism.equals(RepairParallelism.SEQUENTIAL);

      int commandId =
          ((StorageServiceMBean) ssProxy)
              .forceRepairRangeAsync(
                  beginToken.toString(),
                  endToken.toString(),
                  keyspace,
                  snapshotRepair
                      ? RepairParallelism.SEQUENTIAL.ordinal()
                      : RepairParallelism.PARALLEL.ordinal(),
                  datacenters,
                  cassandraVersion.startsWith("2.2") ? new HashSet<String>() : null,
                  fullRepair,
                  columnFamilies.toArray(new String[columnFamilies.size()]));

      repairStatusHandlers.putIfAbsent(commandId, repairStatusHandler);
      return commandId;
    }

    // incremental repair
    int commandId =
        ((StorageServiceMBean) ssProxy)
            .forceRepairAsync(
                keyspace,
                Boolean.FALSE,
                Boolean.FALSE,
                Boolean.FALSE,
                fullRepair,
                columnFamilies.toArray(new String[columnFamilies.size()]));

    repairStatusHandlers.putIfAbsent(commandId, repairStatusHandler);
    return commandId;
  }

  private int triggerRepairPre2dot1(
      RepairParallelism repairParallelism,
      String keyspace,
      Collection<String> columnFamilies,
      BigInteger beginToken,
      BigInteger endToken,
      Collection<String> datacenters,
      RepairStatusHandler repairStatusHandler) {

    // Cassandra 1.2 and 2.0 compatibility
    if (repairParallelism.equals(RepairParallelism.DATACENTER_AWARE)) {
      int commandId =
          ((StorageServiceMBean20) ssProxy)
              .forceRepairRangeAsync(
                  beginToken.toString(),
                  endToken.toString(),
                  keyspace,
                  repairParallelism.ordinal(),
                  datacenters,
                  null,
                  columnFamilies.toArray(new String[columnFamilies.size()]));

      repairStatusHandlers.putIfAbsent(commandId, repairStatusHandler);
      return commandId;
    }
    boolean snapshotRepair = repairParallelism.equals(RepairParallelism.SEQUENTIAL);
    int commandId =
        ((StorageServiceMBean20) ssProxy)
            .forceRepairRangeAsync(
                beginToken.toString(),
                endToken.toString(),
                keyspace,
                snapshotRepair,
                false,
                columnFamilies.toArray(new String[columnFamilies.size()]));

    repairStatusHandlers.putIfAbsent(commandId, repairStatusHandler);
    return commandId;
  }

  @Override
  public String getAllEndpointsState() {
    return ((FailureDetectorMBean) fdProxy).getAllEndpointStates();
  }

  @Override
  public Map<String, String> getSimpleStates() {
    return ((FailureDetectorMBean) fdProxy).getSimpleStates();
  }

  /**
   * Invoked when the MBean this class listens to publishes an event. We're only interested in repair-related events.
   * Their format is explained at {@link org.apache.cassandra.service.StorageServiceMBean#forceRepairAsync} The forma
   * is: notification type: "repair" notification userData: int array of length 2 where [0] = command number [1] =
   * ordinal of AntiEntropyService.Status
   */
  @Override
  public void handleNotification(final Notification notification, Object handback) {
    // pass off the work immediately to a separate thread
    EXECUTOR.submit(() -> {
      String threadName = Thread.currentThread().getName();
      try {
        Thread.currentThread().setName(clusterName);
        // we're interested in "repair"
        String type = notification.getType();
        LOG.debug("Received notification: {} with type {}", notification, type);
        if (("repair").equals(type)) {
          processOldApiNotification(notification);
        }

        if (("progress").equals(type)) {
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
        LOG.debug(
            "Handling notification: {} with repair handler {}",
            notification,
            repairStatusHandlers.containsKey(repairNo));
        repairStatusHandlers
            .get(repairNo)
            .handle(repairNo, Optional.of(status), Optional.absent(), message, this);
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
        LOG.debug(
            "Handling notification: {} with repair handler {}",
            notification,
            repairStatusHandlers.containsKey(repairNo));
        repairStatusHandlers
            .get(repairNo)
            .handle(repairNo, Optional.absent(), Optional.of(progress), message, this);
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
  public void removeRepairStatusHandler(int commandId) {
    repairStatusHandlers.remove(commandId);
  }

  /** Cleanly shut down by un-registering the listener and closing the JMX connection. */
  @Override
  public void close() {
    try {
      mbeanServer.removeNotificationListener(ssMbeanName, this);
      LOG.debug("Successfully removed notification listener for '{}': {}", host, jmxUrl);
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
  public void clearSnapshot(String repairId, String keyspaceName) {
    if (null == repairId || repairId.isEmpty()) {
      // Passing in null or empty string will clear all snapshots on the hos
      throw new IllegalArgumentException("repairId cannot be null or empty string");
    }
    try {
      ((StorageServiceMBean) ssProxy).clearSnapshot(repairId, keyspaceName);
    } catch (AssertionError | IOException e) {
      LOG.error("failed to clear snapshot " + repairId + " in keyspace " + keyspaceName, e);
    }
  }

  @Override
  public void clearSnapshot(String snapshotName) {
    if (null == snapshotName || snapshotName.isEmpty()) {
      // Passing in null or empty string will clear all snapshots on the hos
      throw new IllegalArgumentException("snapshotName cannot be null or empty string");
    }
    try {
      ((StorageServiceMBean) ssProxy).clearSnapshot(snapshotName);
    } catch (AssertionError | IOException e) {
      LOG.error("failed to clear snapshot " + snapshotName, e);
    }
  }

  @Override
  public List<String> getLiveNodes() throws ReaperException {
    checkNotNull(ssProxy, "Looks like the proxy is not connected");
    try {
      return ((StorageServiceMBean) ssProxy).getLiveNodes();
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
                  clusterName.replace('.', '-'),
                  host.replace('.', '-'),
                  "repairStatusHandlers"))) {

        metricRegistry.register(
            MetricRegistry.name(
                JmxProxyImpl.class,
                clusterName.replace('.', '-'),
                host.replace('.', '-'),
                "repairStatusHandlers"),
            (Gauge<Integer>) () -> repairStatusHandlers.size());
      }
    } catch (IllegalArgumentException e) {
      LOG.warn("Cannot create connection gauge for node {}", host, e);
    }
  }

  @Override
  public List<Snapshot> listSnapshots() throws UnsupportedOperationException {
    List<Snapshot> snapshots = Lists.newArrayList();

    String cassandraVersion = getCassandraVersion();
    if (versionCompare(cassandraVersion, "2.1.0") < 0) {
      // 2.0 and prior do not allow to list snapshots
      throw new UnsupportedOperationException(
          "Snapshot listing is not supported in Cassandra 2.0 and prior.");
    }

    final Map<String, TabularData> snapshotDetails =
        ((StorageServiceMBean) ssProxy).getSnapshotDetails();
    if (snapshotDetails.isEmpty()) {
      LOG.debug("There are no snapshots on host {}", this.host);
      return snapshots;
    }

    final String clusterName = ((StorageServiceMBean) ssProxy).getClusterName();
    final long trueSnapshotsSize = ((StorageServiceMBean) ssProxy).trueSnapshotsSize();

    // display column names only once
    final List<String> indexNames =
        snapshotDetails.entrySet().iterator().next().getValue().getTabularType().getIndexNames();

    for (final Map.Entry<String, TabularData> snapshotDetail : snapshotDetails.entrySet()) {
      Set<?> values = snapshotDetail.getValue().keySet();
      for (Object eachValue : values) {
        int index = 0;
        Builder snapshotBuilder = Snapshot.builder().withHost(this.getHost());
        final List<?> valueList = (List<?>) eachValue;
        for (Object value : valueList) {
          switch (indexNames.get(index)) {
            case "Snapshot name":
              snapshotBuilder.withName((String) value);
              break;
            case "Keyspace name":
              snapshotBuilder.withKeyspace((String) value);
              break;
            case "Column family name":
              snapshotBuilder.withTable((String) value);
              break;
            case "True size":
              snapshotBuilder.withTrueSize(parseHumanReadableSize((String) value));
              break;
            case "Size on disk":
              snapshotBuilder.withSizeOnDisk(parseHumanReadableSize((String) value));
              break;
            default:
              break;
          }
          index++;
        }
        snapshots.add(snapshotBuilder.withClusterName(clusterName).build());
      }
    }

    return snapshots;
  }

  public static double parseHumanReadableSize(String readableSize) {
    int spaceNdx = readableSize.indexOf(" ");
    double ret =
        readableSize.contains(".")
            ? Double.parseDouble(readableSize.substring(0, spaceNdx))
            : Double.parseDouble(readableSize.substring(0, spaceNdx).replace(",", "."));
    switch (readableSize.substring(spaceNdx + 1)) {
      case "GB":
        return ret * GB_FACTOR;
      case "GiB":
        return ret * GIB_FACTOR;
      case "MB":
        return ret * MB_FACTOR;
      case "MiB":
        return ret * MIB_FACTOR;
      case "KB":
        return ret * KB_FACTOR;
      case "KiB":
        return ret * KIB_FACTOR;
      default:
        return 0;
    }
  }

  @Override
  public String takeSnapshot(String snapshotName, String... keyspaceNames) throws ReaperException {
    try {
      ((StorageServiceMBean) ssProxy).takeSnapshot(snapshotName, keyspaceNames);
      return snapshotName;
    } catch (IOException e) {
      throw new ReaperException(e);
    }
  }

  @Override
  public void takeColumnFamilySnapshot(
      String keyspaceName, String columnFamilyName, String snapshotName) throws ReaperException {
    try {
      ((StorageServiceMBean) ssProxy)
          .takeColumnFamilySnapshot(keyspaceName, columnFamilyName, snapshotName);
    } catch (IOException e) {
      throw new ReaperException(e);
    }
  }
}
