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

package io.cassandrareaper.storage.cluster;

import io.cassandrareaper.core.Cluster;
import java.util.Collection;

public interface IClusterDao {
  Collection<Cluster> getClusters();

  boolean addCluster(Cluster cluster);

  boolean updateCluster(Cluster newCluster);

  Cluster getCluster(String clusterName);

  /**
   * Delete the Cluster instance identified by the given cluster name. Delete succeeds only if there
   * are no repair runs for the targeted cluster.
   *
   * @param clusterName The name of the Cluster instance to delete.
   * @return The deleted Cluster instance if delete succeeds, with state set to DELETED.
   */
  Cluster deleteCluster(String clusterName);
}
