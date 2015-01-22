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

public class JmxConnectionFactory {

  public JmxProxy create(Optional<RepairStatusHandler> handler, String host)
      throws ReaperException {
    return JmxProxy.connect(handler, host);
  }

  public final JmxProxy create(String host) throws ReaperException {
    return create(Optional.<RepairStatusHandler>absent(), host);
  }

  public final JmxProxy connectAny(Optional<RepairStatusHandler> handler, Collection<String> hosts)
      throws ReaperException {
    return create(handler, hosts.iterator().next());
  }

  public final JmxProxy connectAny(Cluster cluster)
      throws ReaperException {
    return connectAny(Optional.<RepairStatusHandler>absent(), cluster.getSeedHosts());
  }
}
