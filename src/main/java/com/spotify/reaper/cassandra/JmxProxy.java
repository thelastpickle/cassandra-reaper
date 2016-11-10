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
package com.spotify.reaper.cassandra;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.JMX;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.validation.constraints.NotNull;

import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageServiceMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.service.RingRange;

public class JmxProxy implements NotificationListener, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(JmxProxy.class);

  private static final int JMX_PORT = 7199;
  private static final String JMX_URL = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";
  private static final String SS_OBJECT_NAME = "org.apache.cassandra.db:type=StorageService";
  private static final String AES_OBJECT_NAME =
      "org.apache.cassandra.internal:type=AntiEntropySessions";
  private static final String COMP_OBJECT_NAME =
	      "org.apache.cassandra.metrics:type=Compaction";

  private final JMXConnector jmxConnector;
  private final ObjectName ssMbeanName;
  private final MBeanServerConnection mbeanServer;
  private final CompactionManagerMBean cmProxy;
  private final Object ssProxy;
  private final Optional<RepairStatusHandler> repairStatusHandler;
  private final String host;
  private final JMXServiceURL jmxUrl;
  private final String clusterName;

  private JmxProxy(Optional<RepairStatusHandler> handler, String host, JMXServiceURL jmxUrl,
      JMXConnector jmxConnector, Object ssProxy, ObjectName ssMbeanName,
      MBeanServerConnection mbeanServer, CompactionManagerMBean cmProxy) {
    this.host = host;
    this.jmxUrl = jmxUrl;
    this.jmxConnector = jmxConnector;
    this.ssMbeanName = ssMbeanName;
    this.mbeanServer = mbeanServer;
    this.ssProxy = ssProxy;
    this.repairStatusHandler = handler;
    this.cmProxy = cmProxy;
    this.clusterName = Cluster.toSymbolicName(((StorageServiceMBean) ssProxy).getClusterName());
  }


  /**
   * @see JmxProxy#connect(Optional, String, int, String, String)
   */
  static JmxProxy connect(Optional<RepairStatusHandler> handler, String host, String username,
      String password)
      throws ReaperException {
    assert null != host : "null host given to JmxProxy.connect()";
    String[] parts = host.split(":");
    if (parts.length == 2) {
      return connect(handler, parts[0], Integer.valueOf(parts[1]), username, password);
    } else {
      return connect(handler, host, JMX_PORT, username, password);
    }
  }


  /**
   * Connect to JMX interface on the given host and port.
   *
   * @param handler  Implementation of {@link RepairStatusHandler} to process incoming
   *                 notifications
   *                 of repair events.
   * @param host     hostname or ip address of Cassandra node
   * @param port     port number to use for JMX connection
   * @param username username to use for JMX authentication
   * @param password password to use for JMX authentication
   */
  static JmxProxy connect(Optional<RepairStatusHandler> handler, String host, int port,
      String username, String password)
      throws ReaperException {
    ObjectName ssMbeanName;
    ObjectName cmMbeanName;
    JMXServiceURL jmxUrl;
    try {
      jmxUrl = new JMXServiceURL(String.format(JMX_URL, host, port));
      ssMbeanName = new ObjectName(SS_OBJECT_NAME);
      cmMbeanName = new ObjectName(CompactionManager.MBEAN_OBJECT_NAME);
    } catch (MalformedURLException | MalformedObjectNameException e) {
      LOG.error(String.format("Failed to prepare the JMX connection to %s:%s", host, port));
      throw new ReaperException("Failure during preparations for JMX connection", e);
    }
    try {
      Map<String, Object> env = new HashMap<String, Object>();
      if (username != null && password != null) {
        String[] creds = {username, password};
        env.put(JMXConnector.CREDENTIALS, creds);
      }
      JMXConnector jmxConn = JMXConnectorFactory.connect(jmxUrl, env);
      MBeanServerConnection mbeanServerConn = jmxConn.getMBeanServerConnection();
      Object ssProxy =
          JMX.newMBeanProxy(mbeanServerConn, ssMbeanName, StorageServiceMBean.class);
      String cassandraVersion = ((StorageServiceMBean) ssProxy).getReleaseVersion();
      if(cassandraVersion.startsWith("2.0")){
    	  ssProxy = JMX.newMBeanProxy(mbeanServerConn, ssMbeanName, StorageServiceMBean20.class);
      }

      CompactionManagerMBean cmProxy =
          JMX.newMBeanProxy(mbeanServerConn, cmMbeanName, CompactionManagerMBean.class);
      JmxProxy proxy = new JmxProxy(handler, host + ':' + port, jmxUrl, jmxConn, ssProxy, ssMbeanName,
          mbeanServerConn, cmProxy);
      // registering a listener throws bunch of exceptions, so we do it here rather than in the
      // constructor
      mbeanServerConn.addNotificationListener(ssMbeanName, proxy, null, null);
      LOG.debug(String.format("JMX connection to %s properly connected: %s",
          host, jmxUrl.toString()));
      return proxy;
    } catch (IOException | InstanceNotFoundException e) {
      LOG.error(String.format("Failed to establish JMX connection to %s:%s", host, port));
      throw new ReaperException("Failure when establishing JMX connection", e);
    }
  }

  public String getHost() {
    return host;
  }

  /**
   * @return list of tokens in the cluster
   */
  public List<BigInteger> getTokens() {
    checkNotNull(ssProxy, "Looks like the proxy is not connected");
    return Lists.transform(
        Lists.newArrayList(((StorageServiceMBean) ssProxy).getTokenToEndpointMap().keySet()),
        new Function<String, BigInteger>() {
          @Override
          public BigInteger apply(String s) {
            return new BigInteger(s);
          }
        });
  }

  public Map<List<String>, List<String>> getRangeToEndpointMap(String keyspace)
      throws ReaperException {
    checkNotNull(ssProxy, "Looks like the proxy is not connected");
    try {
      return ((StorageServiceMBean) ssProxy).getRangeToEndpointMap(keyspace);
    } catch (AssertionError e) {
      LOG.error(e.getMessage());
      throw new ReaperException(e.getMessage());
    }
  }

  /**
   * @return all hosts owning a range of tokens
   */
  @NotNull
  public List<String> tokenRangeToEndpoint(String keyspace, RingRange tokenRange) {
    checkNotNull(ssProxy, "Looks like the proxy is not connected");
    Set<Map.Entry<List<String>, List<String>>> entries =
    		((StorageServiceMBean) ssProxy).getRangeToEndpointMap(keyspace).entrySet();
    for (Map.Entry<List<String>, List<String>> entry : entries) {
      BigInteger rangeStart = new BigInteger(entry.getKey().get(0));
      BigInteger rangeEnd = new BigInteger(entry.getKey().get(1));
      if (new RingRange(rangeStart, rangeEnd).encloses(tokenRange)) {
        List<String> endpoints = entry.getValue();
        if (2 == host.split(":").length) {
            String port = host.split(":")[1];
            for (int i = 0 ; i < endpoints.size(); ++i) {
                endpoints.set(i, endpoints.get(i) + ':' + port);
            }
        }
        return endpoints;
      }
    }
    return Lists.newArrayList();
  }

  /**
   * @return full class name of Cassandra's partitioner.
   */
  public String getPartitioner() {
    checkNotNull(ssProxy, "Looks like the proxy is not connected");
    return ((StorageServiceMBean) ssProxy).getPartitionerName();
  }

  /**
   * @return Cassandra cluster name.
   */
  public String getClusterName() {
    checkNotNull(ssProxy, "Looks like the proxy is not connected");
    return ((StorageServiceMBean) ssProxy).getClusterName();
  }

  /**
   * @return list of available keyspaces
   */
  public List<String> getKeyspaces() {
    checkNotNull(ssProxy, "Looks like the proxy is not connected");
    return ((StorageServiceMBean) ssProxy).getKeyspaces();
  }

  public Set<String> getTableNamesForKeyspace(String keyspace) throws ReaperException {
    Set<String> tableNames = new HashSet<>();
    Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> proxies;
    try {
      proxies = ColumnFamilyStoreMBeanIterator.getColumnFamilyStoreMBeanProxies(mbeanServer);
    } catch (IOException | MalformedObjectNameException e) {
      e.printStackTrace();
      throw new ReaperException("failed to get ColumnFamilyStoreMBean instances from JMX");
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

  /**
   * @return number of pending compactions on the node this proxy is connected to
   */
  public int getPendingCompactions() {
    checkNotNull(cmProxy, "Looks like the proxy is not connected");
    try {
        ObjectName name = new ObjectName(COMP_OBJECT_NAME);
        int pendingCount = (int) mbeanServer.getAttribute(name, "PendingTasks");
        return pendingCount;
      } catch (IOException ignored) {
        LOG.warn("Failed to connect to " + host + " using JMX");
      } catch (MalformedObjectNameException ignored) {
        LOG.error("Internal error, malformed name");
      } catch (InstanceNotFoundException e) {
        // This happens if no repair has yet been run on the node
        // The AntiEntropySessions object is created on the first repair
        return 0;
      } catch (Exception e) {
        LOG.error("Error getting attribute from JMX", e);
      }
      // If uncertain, assume it's running
      return 0;
  }

  /**
   * @return true if any repairs are running on the node.
   */
  public boolean isRepairRunning() {
    // Check if AntiEntropySession is actually running on the node
    try {
      ObjectName name = new ObjectName(AES_OBJECT_NAME);
      int activeCount = (Integer) mbeanServer.getAttribute(name, "ActiveCount");
      long pendingCount = (Long) mbeanServer.getAttribute(name, "PendingTasks");
      return activeCount + pendingCount != 0;
    } catch (IOException ignored) {
      LOG.warn("Failed to connect to " + host + " using JMX");
    } catch (MalformedObjectNameException ignored) {
      LOG.error("Internal error, malformed name");
    } catch (InstanceNotFoundException e) {
      // This happens if no repair has yet been run on the node
      // The AntiEntropySessions object is created on the first repair
      return false;
    } catch (Exception e) {
      LOG.error("Error getting attribute from JMX", e);
    }
    // If uncertain, assume it's running
    return true;
  }

  /**
   * Terminates all ongoing repairs on the node this proxy is connected to
   */
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

  /**
   * Checks if table exists in the cluster by instantiating a MBean for that table.
   */
  public boolean tableExists(String ks, String cf) {
    try {
      String type = cf.contains(".") ? "IndexColumnFamilies" : "ColumnFamilies";
      String nameStr = String.format("org.apache.cassandra.db:type=*%s,keyspace=%s,columnfamily=%s",
          type, ks, cf);
      Set<ObjectName> beans = mbeanServer.queryNames(new ObjectName(nameStr), null);
      if (beans.isEmpty() || beans.size() != 1) {
        return false;
      }
      ObjectName bean = beans.iterator().next();
      JMX.newMBeanProxy(mbeanServer, bean, ColumnFamilyStoreMBean.class);
    } catch (MalformedObjectNameException | IOException e) {
      String errMsg = String.format("ColumnFamilyStore for %s/%s not found: %s", ks, cf,
          e.getMessage());
      LOG.warn(errMsg);
      return false;
    }
    return true;
  }

  public String getCassandraVersion(){
	  return ((StorageServiceMBean) ssProxy).getReleaseVersion();
  }

  /**
   * Triggers a repair of range (beginToken, endToken] for given keyspace and column family.
   * The repair is triggered by {@link org.apache.cassandra.service.StorageServiceMBean#forceRepairRangeAsync}
   * For time being, we don't allow local nor snapshot repairs.
   *
   * @return Repair command number, or 0 if nothing to repair
   */
  public int triggerRepair(BigInteger beginToken, BigInteger endToken, String keyspace,
    	RepairParallelism repairParallelism, Collection<String> columnFamilies, boolean fullRepair) {
    checkNotNull(ssProxy, "Looks like the proxy is not connected");
    String cassandraVersion = getCassandraVersion();
    boolean canUseDatacenterAware = false;
    try {
      canUseDatacenterAware = versionCompare(cassandraVersion, "2.0.12") >= 0;
    } catch (ReaperException e) {
      LOG.warn("failed on version comparison, not using dc aware repairs by default");
    }
    String msg = String.format("Triggering repair of range (%s,%s] for keyspace \"%s\" on "
                               + "host %s, with repair parallelism %s, in cluster with Cassandra "
                               + "version '%s' (can use DATACENTER_AWARE '%s'), "
                               + "for column families: %s",
        beginToken.toString(), endToken.toString(), keyspace, this.host,
        repairParallelism, cassandraVersion, canUseDatacenterAware,
        columnFamilies);
    LOG.info(msg);

    if(!cassandraVersion.startsWith("2.0")){
	    if(fullRepair) {
		    if (repairParallelism.equals(RepairParallelism.DATACENTER_AWARE)) {
		      if (canUseDatacenterAware) {
		        return ((StorageServiceMBean) ssProxy).forceRepairRangeAsync(beginToken.toString(), endToken.toString(), keyspace,
		            repairParallelism.ordinal(), null, null, fullRepair,
		            columnFamilies
		                .toArray(new String[columnFamilies.size()]));
		      } else {
		        LOG.info("Cannot use DATACENTER_AWARE repair policy for Cassandra cluster with version {},"
		                 + " falling back to SEQUENTIAL repair.",
		            cassandraVersion);
		        repairParallelism = RepairParallelism.SEQUENTIAL;
		      }
		    }
		    boolean snapshotRepair = repairParallelism.equals(RepairParallelism.SEQUENTIAL);
		    return ((StorageServiceMBean) ssProxy).forceRepairRangeAsync(beginToken.toString(), endToken.toString(), keyspace,
		        snapshotRepair, false, fullRepair,
		        columnFamilies.toArray(new String[columnFamilies.size()]));
	    }
	    else {
	    	return ((StorageServiceMBean) ssProxy).forceRepairAsync(keyspace, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, fullRepair, columnFamilies.toArray(new String[columnFamilies.size()]));
	    }
    }
    else {
    	// Cassandra 2.0 compatibility
    	if (repairParallelism.equals(RepairParallelism.DATACENTER_AWARE)) {
		      if (canUseDatacenterAware) {
		        return ((StorageServiceMBean20) ssProxy).forceRepairRangeAsync(beginToken.toString(), endToken.toString(), keyspace,
		            repairParallelism.ordinal(), null, null,
		            columnFamilies
		                .toArray(new String[columnFamilies.size()]));
		      } else {
		        LOG.info("Cannot use DATACENTER_AWARE repair policy for Cassandra cluster with version {},"
		                 + " falling back to SEQUENTIAL repair.",
		            cassandraVersion);
		        repairParallelism = RepairParallelism.SEQUENTIAL;
		      }
		    }
		    boolean snapshotRepair = repairParallelism.equals(RepairParallelism.SEQUENTIAL);
		    return ((StorageServiceMBean20) ssProxy).forceRepairRangeAsync(beginToken.toString(), endToken.toString(), keyspace,
		        snapshotRepair, false,
		        columnFamilies.toArray(new String[columnFamilies.size()]));
    }

  }


  /**
   * Invoked when the MBean this class listens to publishes an event.
   * We're only interested in repair-related events.
   * Their format is explained at {@link org.apache.cassandra.service.StorageServiceMBean#forceRepairAsync}
   * The format is: notification type: "repair" notification userData: int array of length 2 where
   * [0] = command number [1] = ordinal of AntiEntropyService.Status
   */
  @Override
  public void handleNotification(Notification notification, Object handback) {
    Thread.currentThread().setName(clusterName);
    // we're interested in "repair"
    String type = notification.getType();
    LOG.debug("Received notification: {}", notification.toString());
    if (repairStatusHandler.isPresent() && type.equals("repair")) {
      int[] data = (int[]) notification.getUserData();
      // get the repair sequence number
      int repairNo = data[0];
      // get the repair status
      ActiveRepairService.Status status = ActiveRepairService.Status.values()[data[1]];
      // this is some text message like "Starting repair...", "Finished repair...", etc.
      String message = notification.getMessage();
      // let the handler process the event
      repairStatusHandler.get().handle(repairNo, status, message);
    }
  }

  public String getConnectionId() throws IOException {
    return jmxConnector.getConnectionId();
  }

  public boolean isConnectionAlive() {
    try {
      String connectionId = getConnectionId();
      return null != connectionId && connectionId.length() > 0;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  /**
   * Cleanly shut down by un-registering the listener and closing the JMX connection.
   */
  @Override
  public void close() throws ReaperException {
    LOG.debug(String.format("close JMX connection to '%s': %s", host, jmxUrl));
    try {
      mbeanServer.removeNotificationListener(ssMbeanName, this);
    } catch (InstanceNotFoundException | ListenerNotFoundException | IOException e) {
      LOG.warn("failed on removing notification listener");
      e.printStackTrace();
    }
    try {
      jmxConnector.close();
    } catch (IOException e) {
      LOG.warn("failed closing a JMX connection");
      e.printStackTrace();
    }
  }

  /**
   * NOTICE: This code is loosely based on StackOverflow answer:
   * http://stackoverflow.com/questions/6701948/efficient-way-to-compare-version-strings-in-java
   *
   * Compares two version strings.
   *
   * Use this instead of String.compareTo() for a non-lexicographical
   * comparison that works for version strings. e.g. "1.10".compareTo("1.6").
   *
   * @param str1 a string of ordinal numbers separated by decimal points.
   * @param str2 a string of ordinal numbers separated by decimal points.
   * @return The result is a negative integer if str1 is _numerically_ less than str2.
   * The result is a positive integer if str1 is _numerically_ greater than str2.
   * The result is zero if the strings are _numerically_ equal.
   * It does not work if "1.10" is supposed to be equal to "1.10.0".
   */
  public static Integer versionCompare(String str1, String str2) throws ReaperException {
    try {
      str1 = str1.split(" ")[0].replaceAll("[-_~]", ".");
      str2 = str2.split(" ")[0].replaceAll("[-_~]", ".");
      String[] parts1 = str1.split("\\.");
      String[] parts2 = str2.split("\\.");
      int i = 0;
      // set index to first non-equal ordinal or length of shortest version string
      while (i < parts1.length && i < parts2.length) {
        try {
          Integer.parseInt(parts1[i]);
          Integer.parseInt(parts2[i]);
        } catch (NumberFormatException ex) {
          if (i == 0) {
            throw ex; // just comparing two non-version strings should fail
          }
          // first non integer part, so let's just stop comparison here and ignore the rest
          i--;
          break;
        }
        if (parts1[i].equals(parts2[i])) {
          i++;
          continue;
        }
        break;
      }
      // compare first non-equal ordinal number
      if (i < parts1.length && i < parts2.length) {
        int diff = Integer.valueOf(parts1[i]).compareTo(Integer.valueOf(parts2[i]));
        return Integer.signum(diff);
      }
      // the strings are equal or one string is a substring of the other
      // e.g. "1.2.3" = "1.2.3" or "1.2.3" < "1.2.3.4"
      else {
        return Integer.signum(parts1.length - parts2.length);
      }
    } catch (Exception ex) {
      LOG.error("failed comparing strings for versions: '{}' '{}'", str1, str2);
      throw new ReaperException(ex);
    }
  }

  public void clearSnapshot(String repairId, String keyspaceName) throws ReaperException {
    if (repairId == null || repairId.equals("")) {
      // Passing in null or empty string will clear all snapshots on the host
      throw new IllegalArgumentException("repairId cannot be null or empty string");
    }
    try {
    	((StorageServiceMBean) ssProxy).clearSnapshot(repairId, keyspaceName);
    } catch (IOException e) {
      throw new ReaperException(e);
    }
  }
}

/**
 * This code is copied and adjusted from from NodeProbe.java from Cassandra source.
 */
class ColumnFamilyStoreMBeanIterator
    implements Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> {

  private Iterator<ObjectName> resIter;
  private MBeanServerConnection mbeanServerConn;

  public ColumnFamilyStoreMBeanIterator(MBeanServerConnection mbeanServerConn)
      throws MalformedObjectNameException, NullPointerException, IOException {
    ObjectName query = new ObjectName("org.apache.cassandra.db:type=ColumnFamilies,*");
    resIter = mbeanServerConn.queryNames(query, null).iterator();
    this.mbeanServerConn = mbeanServerConn;
  }

  static Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> getColumnFamilyStoreMBeanProxies(
      MBeanServerConnection mbeanServerConn)
      throws IOException, MalformedObjectNameException {
    return new ColumnFamilyStoreMBeanIterator(mbeanServerConn);
  }

  @Override
public boolean hasNext() {
    return resIter.hasNext();
  }

  @Override
public Map.Entry<String, ColumnFamilyStoreMBean> next() {
    ObjectName objectName = resIter.next();
    String keyspaceName = objectName.getKeyProperty("keyspace");
    ColumnFamilyStoreMBean cfsProxy =
        JMX.newMBeanProxy(mbeanServerConn, objectName, ColumnFamilyStoreMBean.class);
    return new AbstractMap.SimpleImmutableEntry<>(keyspaceName, cfsProxy);
  }

  @Override
public void remove() {
    throw new UnsupportedOperationException();
  }
}
