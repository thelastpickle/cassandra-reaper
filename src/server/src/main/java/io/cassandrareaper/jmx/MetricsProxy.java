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


import io.cassandrareaper.core.JmxStat;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.management.JMException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.ObjectName;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public final class MetricsProxy {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsProxy.class);

  private final JmxProxyImpl proxy;

  private MetricsProxy(JmxProxyImpl proxy) {
    this.proxy = proxy;
  }

  public static MetricsProxy create(JmxProxy proxy) {
    Preconditions.checkArgument(proxy instanceof JmxProxyImpl, "only JmxProxyImpl is supported");
    return new MetricsProxy((JmxProxyImpl)proxy);
  }

  public Map<String, List<JmxStat>> collectTpStats() throws JMException, IOException {
    return collectMetrics(
        "org.apache.cassandra.metrics:type=ThreadPools,path=request,*",
        "org.apache.cassandra.metrics:type=ThreadPools,path=internal,*");
  }

  public Map<String, List<JmxStat>> collectDroppedMessages() throws JMException, IOException {
    return collectMetrics("org.apache.cassandra.metrics:type=DroppedMessage,*");
  }

  public Map<String, List<JmxStat>> collectLatencyMetrics() throws JMException, IOException {
    return collectMetrics("org.apache.cassandra.metrics:type=ClientRequest,*");
  }


  /**
   * Collects all attributes for a given set of JMX beans.
   *
   * @param beans the list of beans to collect through JMX
   * @return a map with a key for each bean and a list of jmx stat in generic format.
   */
  private Map<String, List<JmxStat>> collectMetrics(String... beans) throws JMException, IOException {
    List<List<JmxStat>> allStats = Lists.newArrayList();
    Set<ObjectName> beanSet = Sets.newLinkedHashSet();
    for (String bean : beans) {
      beanSet.addAll(proxy.getMBeanServerConnection().queryNames(new ObjectName(bean), null));
    }

    beanSet.stream()
        .map((objName) -> scrapeBean(objName))
        .forEachOrdered(attributes -> allStats.add(attributes));

    List<JmxStat> flatStatList = allStats.stream()
        .flatMap(attr -> attr.stream()).collect(Collectors.toList());

    // Group the stats by scope to ease displaying/manipulating the data
    Map<String, List<JmxStat>> groupedStatList = flatStatList.stream()
        .collect(Collectors.groupingBy(JmxStat::getScope));

    return groupedStatList;
  }

  private List<JmxStat> scrapeBean(ObjectName mbeanName) {
    List<JmxStat> attributeList = Lists.newArrayList();
    try {
      Map<String, MBeanAttributeInfo> name2AttrInfo = new LinkedHashMap<>();
      MBeanInfo info = proxy.getMBeanServerConnection().getMBeanInfo(mbeanName);
      for (MBeanAttributeInfo attrInfo : info.getAttributes()) {
        MBeanAttributeInfo attr = attrInfo;
        if (!attr.isReadable()) {
          LOG.warn("{}.{} not readable", mbeanName, attr);
        } else {
          name2AttrInfo.put(attr.getName(), attr);
        }
      }
      proxy.getMBeanServerConnection().getAttributes(mbeanName, name2AttrInfo.keySet().toArray(new String[0]))
          .asList()
          .forEach((attribute) -> {
            Object value = attribute.getValue();

            JmxStat.Builder jmxStatBuilder = JmxStat.builder()
                .withAttribute(attribute.getName())
                .withName(mbeanName.getKeyProperty("name"))
                .withScope(mbeanName.getKeyProperty("scope"));

            if (null == value) {
              attributeList.add(jmxStatBuilder.withValue(0.0).build());
            } else if (value instanceof Number) {
              attributeList.add(jmxStatBuilder.withValue(((Number) value).doubleValue()).build());
            }
          });
    } catch (JMException | IOException e) {
      LOG.error("Fail getting mbeanInfo or grabbing attributes for mbean {} ", mbeanName, e);
    }
    return attributeList;
  }

}
