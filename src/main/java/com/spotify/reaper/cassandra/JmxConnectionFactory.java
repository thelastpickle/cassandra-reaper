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

import com.google.common.base.Optional;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.core.Cluster;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class JmxConnectionFactory {

  private Map<String, Integer> jmxPorts;

  public JmxProxy connect(Optional<RepairStatusHandler> handler, String host)
      throws ReaperException {
    // use configured jmx port for host if provided
    if(jmxPorts != null && jmxPorts.containsKey(host) && !host.contains(":"))
        host = host + ":" + jmxPorts.get(host);
    return JmxProxy.connect(handler, host);
  }

  public final JmxProxy connect(String host) throws ReaperException {
    return connect(Optional.<RepairStatusHandler>absent(), host);
  }

  public final JmxProxy connectAny(Optional<RepairStatusHandler> handler, Collection<String> hosts)
      throws ReaperException {
    if (hosts == null || hosts.isEmpty()) {
      throw new ReaperException("no hosts given for connectAny");
    }
    return connect(handler, hosts.iterator().next());
  }

  public final JmxProxy connectAny(Cluster cluster)
      throws ReaperException {
    Set<String> hosts = cluster.getSeedHosts();
    if (hosts == null || hosts.isEmpty()) {
      throw new ReaperException("no seeds in cluster with name: " + cluster.getName());
    }
    return connectAny(Optional.<RepairStatusHandler>absent(), hosts);
  }

  public void setJmxPorts(Map<String, Integer> jmxPorts) {
    this.jmxPorts = jmxPorts;
  }
}
