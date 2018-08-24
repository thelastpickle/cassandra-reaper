/*
 * Copyright 2018-2018 The Last Pickle Ltd
 *
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

package io.cassandrareaper.jmx;

import io.cassandrareaper.core.Compaction;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;

import com.codahale.metrics.InstrumentedExecutorService;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CompactionProxy {

  private static final AtomicReference<ExecutorService> EXECUTOR = new AtomicReference();
  private static final Logger LOG = LoggerFactory.getLogger(CompactionProxy.class);

  private final JmxProxyImpl proxy;

  private CompactionProxy(JmxProxyImpl proxy, MetricRegistry metrics) {
    this.proxy = proxy;
    if (null == EXECUTOR.get()) {
      EXECUTOR.set(new InstrumentedExecutorService(Executors.newCachedThreadPool(), metrics, "CompactionProxy"));
    }
  }

  public static CompactionProxy create(JmxProxy proxy, MetricRegistry metrics) {
    Preconditions.checkArgument(proxy instanceof JmxProxyImpl, "only JmxProxyImpl is supported");
    return new CompactionProxy((JmxProxyImpl)proxy, metrics);
  }

  public void forceCompaction(String keyspaceName, String... tableNames) {
    EXECUTOR.get().submit(() -> {
      try {
        // major compactions abort all currently running compactions on the specified table,
        // parallel major compactions are therefore not possile,
        // calling this multiple times on the same table is ok,
        // but comes at the cost of time spent unnecessary compactions
        // reference: ColumnFamilyStore.runWithCompactionsDisabled(..)
        proxy.getStorageServiceMBean().forceKeyspaceCompaction(false, keyspaceName, tableNames);
      } catch (IOException | ExecutionException | InterruptedException ex) {
        LOG.warn(String.format("failed compaction on %s (%s)", keyspaceName, StringUtils.join(tableNames)), ex);
      }
    });
  }

  public List<Compaction> listActiveCompactions() throws ReflectionException, MalformedObjectNameException {
    Preconditions.checkNotNull(proxy.getCompactionManagerMBean(), "Looks like the proxy is not connected");
    List<Compaction> activeCompactions = Lists.newArrayList();
    List<Map<String, String>> compactions = proxy.getCompactionManagerMBean().getCompactions();
    if (!compactions.isEmpty()) {
      for (Map<String, String> c : compactions) {
        Compaction compaction =
            Compaction.builder()
                .withId(c.get("compactionId"))
                .withKeyspace(c.get("keyspace"))
                .withTable(c.get("columnfamily"))
                .withProgress(Long.parseLong(c.get("completed")))
                .withTotal(Long.parseLong(c.get("total")))
                .withUnit(c.get("unit"))
                .withType(c.get("taskType"))
                .build();

        activeCompactions.add(compaction);
      }
    }
    return activeCompactions;
  }
}
