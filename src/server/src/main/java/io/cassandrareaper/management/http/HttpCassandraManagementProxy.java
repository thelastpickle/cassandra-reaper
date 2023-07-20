/*
 * Copyright 2020-2020 The Last Pickle Ltd
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

package io.cassandrareaper.management.http;

import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Table;
import io.cassandrareaper.management.ICassandraManagementProxy;
import io.cassandrareaper.management.jmx.RepairStatusHandler;
import io.cassandrareaper.service.RingRange;

import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.management.JMException;
import javax.management.Notification;
import javax.validation.constraints.NotNull;

import com.codahale.metrics.MetricRegistry;
import org.apache.cassandra.repair.RepairParallelism;

public class HttpCassandraManagementProxy implements ICassandraManagementProxy {
  String host;
  MetricRegistry metricRegistry;
  String rootPath;
  InetSocketAddress endpoint;

  public HttpCassandraManagementProxy(MetricRegistry metricRegistry,
                                      String rootPath,
                                      InetSocketAddress endpoint
  ) {
    this.metricRegistry = metricRegistry;
    this.rootPath = rootPath;
    this.endpoint = endpoint;
  }

  @Override
  public String getHost() {
    return host;
  }

  @Override
  public List<BigInteger> getTokens() {
    return null; // TODO: implement me.
  }

  @Override
  public Map<List<String>, List<String>> getRangeToEndpointMap(String keyspace) throws ReaperException {
    return null; // TODO: implement me.
  }

  @NotNull
  @Override
  public String getLocalEndpoint() throws ReaperException {
    return null; // TODO: implement me.
  }

  @NotNull
  @Override
  public Map<String, String> getEndpointToHostId() {
    return null; // TODO: implement me.
  }

  @Override
  public String getPartitioner() {
    return null; // TODO: implement me.
  }

  @Override
  public String getClusterName() {
    return null; // TODO: implement me.
  }

  @Override
  public List<String> getKeyspaces() {
    return null; // TODO: implement me.
  }

  @Override
  public Set<Table> getTablesForKeyspace(String keyspace) throws ReaperException {
    return null; // TODO: implement me.
  }

  @Override
  public int getPendingCompactions() throws JMException {
    return 1; // TODO: implement me.
  }

  @Override
  public boolean isRepairRunning() throws JMException {
    return true; // TODO: implement me.
  }


  @Override
  public List<String> getRunningRepairMetricsPost22() {
    return null; // TODO: implement me.
  }

  @Override
  public void cancelAllRepairs() {
    // TODO: implement me.
  }

  @Override
  public Map<String, List<String>> listTablesByKeyspace() {
    return null; // TODO: implement me.
  }

  @Override
  public String getCassandraVersion() {
    return null; // TODO: implement me.
  }

  @Override
  public int triggerRepair(
      BigInteger beginToken,
      BigInteger endToken,
      String keyspace,
      RepairParallelism repairParallelism,
      Collection<String> columnFamilies,
      boolean fullRepair,
      Collection<String> datacenters,
      RepairStatusHandler repairStatusHandler,
      List<RingRange> associatedTokens,
      int repairThreadCount)
      throws ReaperException {
    return 1; //TODO: implement me

  }

  @Override
  public void handleNotification(final Notification notification, Object handback) {
    // TODO: implement me.
  }

  @Override
  public boolean isConnectionAlive() {
    return true; // TODO: implement me.
  }

  @Override
  public void removeRepairStatusHandler(int repairNo) {
    // TODO: implement me.
  }

  @Override
  public void close() {
    // TODO: implement me.
  }

  @Override
  public List<String> getLiveNodes() throws ReaperException {
    return null; // TODO: implement me.
  }

}