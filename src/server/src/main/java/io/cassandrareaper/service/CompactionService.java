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
import io.cassandrareaper.core.Compaction;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.jmx.ColumnFamilyStoreProxy;
import io.cassandrareaper.jmx.CompactionProxy;
import io.cassandrareaper.jmx.JmxProxy;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import javax.management.JMException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.cassandra.db.compaction.LeveledCompactionStrategy;
import org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class CompactionService {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionService.class);

  private static final Set SUPPORTED_COMPACTION_STRATEGIES
      = ImmutableSet.of(SizeTieredCompactionStrategy.class.getName(), LeveledCompactionStrategy.class.getName());

  private static final ColumnFamilyStoreProxyProvider DEFAULT_CFS_PROXY_PROVIDER
      = new ColumnFamilyStoreProxyProvider() {
          ColumnFamilyStoreProxy create(String keyspace, String table, JmxProxy proxy) {
            return ColumnFamilyStoreProxy.create(keyspace, table, proxy);
          }
      };

  private final AppContext context;
  private final ColumnFamilyStoreProxyProvider cfsProxyProvider;


  private CompactionService(AppContext context, ColumnFamilyStoreProxyProvider cfsProxyProvider) {
    this.context = context;
    this.cfsProxyProvider = cfsProxyProvider;
  }

  public static CompactionService create(AppContext context) {
    return new CompactionService(context, DEFAULT_CFS_PROXY_PROVIDER);
  }

  @VisibleForTesting
  static CompactionService create(AppContext context, ColumnFamilyStoreProxyProvider cfsProxyProvider) {
    return new CompactionService(context, cfsProxyProvider);
  }

  public List<Compaction> listActiveCompactions(Node host) throws ReaperException {
    try {
      JmxProxy jmxProxy = context.jmxConnectionFactory.connect(host, context.config.getJmxConnectionTimeoutInSeconds());
      return CompactionProxy.create(jmxProxy, context.metricRegistry).listActiveCompactions();
    } catch (JMException | RuntimeException | InterruptedException e) {
      LOG.error("Failed listing compactions for host {}", host, e);
      throw new ReaperException(e);
    }
  }

  /** Calls compact on the unit's keyspace and tables on all live nodes. */
  public void compact(UUID repairUnitId) throws ReaperException {
    RepairUnit unit = context.storage.getRepairUnit(repairUnitId);
    Cluster cluster = context.storage.getCluster(unit.getClusterName()).get();
    compact(cluster, unit.getKeyspaceName(), listTables(cluster, unit));
  }

  private void compact(Cluster cluster, String keyspaceName, String... tableNames) throws ReaperException {
    context.jmxConnectionFactory
        .connectAny(cluster, context.config.getJmxConnectionTimeoutInSeconds())
        .getLiveNodes()
        .stream()
        .map(host -> Node.builder().withCluster(cluster).withHostname(host).build())
        .map(node -> connect(node))
        .filter(node -> node.isPresent())
        .map(proxy -> CompactionProxy.create(proxy.get(), context.metricRegistry))
        .forEach(compactionProxy -> compactionProxy.forceCompaction(keyspaceName, tableNames));
  }

  private Optional<JmxProxy> connect(Node nde) {
    try {
      return Optional.of(context.jmxConnectionFactory.connect(nde, context.config.getJmxConnectionTimeoutInSeconds()));
    } catch (ReaperException | InterruptedException ex) {
      LOG.warn("skipping compactions on " + nde, ex);
      return Optional.empty();
    }
  }

  private String[] listTables(Cluster cluster, RepairUnit unit) {
    String keyspace = unit.getKeyspaceName();
    try {
      JmxProxy proxy
          = context.jmxConnectionFactory.connectAny(cluster, context.config.getJmxConnectionTimeoutInSeconds());

      Set<String> tables = Sets.newHashSet(
          unit.getColumnFamilies().isEmpty()
              ? proxy.getTableNamesForKeyspace(keyspace)
              : unit.getColumnFamilies());

      tables.removeAll(unit.getBlacklistedTables());
      tables.removeAll(tablesWithUnsupportedCompactionStrategies(tables, keyspace, proxy));
      return tables.toArray(new String[tables.size()]);
    } catch (ReaperException e) {
      LOG.warn(String.format("listTables fail: cluster %s keyspace %s", cluster.getName(), keyspace), e);
      return new String[0];
    }
  }

  private Set<String> tablesWithUnsupportedCompactionStrategies(Set<String> tables, String keyspace, JmxProxy proxy) {
    Set<String> unsupported = Sets.newHashSet();
    tables.forEach((table) -> {
      ColumnFamilyStoreProxy cfsProxy = cfsProxyProvider.create(keyspace, table, proxy);
      String compactionStrategy = cfsProxy.getCompactionStrategyClass();
      if (!SUPPORTED_COMPACTION_STRATEGIES.contains(compactionStrategy)) {
        unsupported.add(table);
        LOG.warn("Reaper will not compact table {}.{} with unsupported {}", keyspace, table, compactionStrategy);
      }
    });
    return unsupported;
  }

  @VisibleForTesting
  abstract static class ColumnFamilyStoreProxyProvider {
    abstract ColumnFamilyStoreProxy create(String keyspace, String table, JmxProxy proxy);
  }
}
