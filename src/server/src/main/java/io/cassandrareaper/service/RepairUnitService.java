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
import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.jmx.JmxProxy;

import java.util.Set;
import java.util.stream.Collectors;

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

  public RepairUnit getNewOrExistingRepairUnit(
      Cluster cluster,
      String keyspace,
      Set<String> tableNames,
      Boolean incrementalRepair,
      Set<String> nodesToRepair,
      Set<String> datacenters,
      Set<String> blacklistedTables) throws ReaperException {

    Optional<RepairUnit> storedRepairUnit = context.storage.getRepairUnit(
        cluster.getName(),
        keyspace,
        tableNames,
        nodesToRepair,
        datacenters,
        blacklistedTables);

    Optional<String> cassandraVersion = Optional.absent();

    try {
      JmxProxy jmxProxy =
          context.jmxConnectionFactory.connectAny(
              Optional.absent(),
              cluster
                  .getSeedHosts()
                  .stream()
                  .map(host -> Node.builder().withCluster(cluster).withHostname(host).build())
                  .collect(Collectors.toList()),
              context.config.getJmxConnectionTimeoutInSeconds());

      cassandraVersion = Optional.fromNullable(jmxProxy.getCassandraVersion());
    } catch (ReaperException e) {
      LOG.warn("couldn't connect to hosts: {}, life sucks...", cluster.getSeedHosts(), e);
    }

    if (cassandraVersion.isPresent() && cassandraVersion.get().startsWith("2.0") && incrementalRepair) {
      String errMsg = "Incremental repair does not work with Cassandra versions before 2.1";
      LOG.error(errMsg);
      throw new ReaperException(errMsg);
    }

    RepairUnit theRepairUnit;

    if (storedRepairUnit.isPresent()
        && storedRepairUnit.get().getIncrementalRepair().equals(incrementalRepair)
        && storedRepairUnit.get().getNodes().equals(nodesToRepair)
        && storedRepairUnit.get().getDatacenters().equals(datacenters)
        && storedRepairUnit.get().getBlacklistedTables().equals(blacklistedTables)
        && storedRepairUnit.get().getColumnFamilies().equals(tableNames)) {
      LOG.info(
          "use existing repair unit for cluster '{}', keyspace '{}', "
              + "column families: {}, nodes: {} and datacenters: {}",
          cluster.getName(),
          keyspace,
          tableNames,
          nodesToRepair,
          datacenters);

      theRepairUnit = storedRepairUnit.get();
    } else {
      LOG.info(
          "create new repair unit for cluster '{}', keyspace '{}', column families: {}, nodes: {} and datacenters: {}",
          cluster.getName(),
          keyspace,
          tableNames,
          nodesToRepair,
          datacenters);

      theRepairUnit = context.storage.addRepairUnit(
              new RepairUnit.Builder(
                  cluster.getName(),
                  keyspace,
                  tableNames,
                  incrementalRepair,
                  nodesToRepair,
                  datacenters,
                  blacklistedTables));
    }
    return theRepairUnit;
  }
}
