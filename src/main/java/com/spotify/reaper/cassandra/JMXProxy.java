package com.spotify.reaper.cassandra;

import com.google.common.collect.Lists;
import com.spotify.reaper.ReaperException;
import org.apache.cassandra.service.StorageServiceMBean;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.List;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class JMXProxy {

  private static final int DEFAULT_JMX_PORT = 7199;

  private JMXConnector jmxc = null;
  private StorageServiceMBean ssProxy;

  public JMXProxy(JMXConnector jmxc, StorageServiceMBean ssProxy) {
    this.jmxc = jmxc;
    this.ssProxy = ssProxy;
  }

  public static JMXProxy connect(String host) throws ReaperException {
    return connect(host, DEFAULT_JMX_PORT);
  }

  public static JMXProxy connect(String host, int port) throws ReaperException {
    JMXServiceURL jmxUrl;
    ObjectName name;
    try {
      jmxUrl = new JMXServiceURL(String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi",
          host, port));
      name = new ObjectName("org.apache.cassandra.db:type=StorageService");
    } catch (MalformedURLException | MalformedObjectNameException e) {
      throw new ReaperException("Failure during preparations for JMX connection", e);
    }
    try {
      JMXConnector jmxc = JMXConnectorFactory.connect(jmxUrl);
      MBeanServerConnection mbeanServerConn = jmxc.getMBeanServerConnection();
      StorageServiceMBean
          ssProxy = JMX.newMBeanProxy(mbeanServerConn, name, StorageServiceMBean.class);
      return new JMXProxy(jmxc, ssProxy);
    } catch (IOException e) {
      throw new ReaperException("Failure when establishing JMX connection", e);
    }
  }

  public List<String> getTokens() {
    return Lists.newArrayList(ssProxy.getTokenToEndpointMap().keySet());
  }

  public void close() throws ReaperException {
    try {
      jmxc.close();
    } catch (IOException e) {
      throw new ReaperException(e);
    }
  }
}
