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

package io.cassandrareaper.service;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.jmx.JmxProxy;

import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class RepairUnitService {

  private static final Logger LOG = LoggerFactory.getLogger(RepairUnitService.class);

  private final AppContext context;

  private RepairUnitService(AppContext context) {
    this.context = context;
  }

  public static RepairUnitService create(AppContext context) {
    return new RepairUnitService(context);
  }

  public RepairUnit getNewOrExistingRepairUnit(Cluster cluster, RepairUnit.Builder params) {
    if (params.incrementalRepair) {
      try {
        JmxProxy jmxProxy
            = context.jmxConnectionFactory.connectAny(cluster, context.config.getJmxConnectionTimeoutInSeconds());

        String version = jmxProxy.getCassandraVersion();
        if (null != version && version.startsWith("2.0")) {
          throw new IllegalArgumentException("Incremental repair does not work with Cassandra versions before 2.1");
        }
      } catch (ReaperException e) {
        LOG.warn("unknown version to cluster {}, maybe enabling incremental on 2.0...", cluster.getName(), e);
      }
    }
    Optional<RepairUnit> repairUnit = context.storage.getRepairUnit(params);
    return repairUnit.isPresent() ? repairUnit.get() : context.storage.addRepairUnit(params);
  }
}
