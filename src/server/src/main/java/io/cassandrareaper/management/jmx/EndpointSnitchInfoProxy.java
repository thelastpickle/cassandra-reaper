/*
 * Copyright 2018-2019 The Last Pickle Ltd
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

package io.cassandrareaper.management.jmx;

import io.cassandrareaper.management.ICassandraManagementProxy;

import java.util.concurrent.ExecutionException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;


public final class EndpointSnitchInfoProxy {

  // `EndpointSnitchInfoMBean().getDatacenter(host)` is static info regardless of what node is answering
  private static final Cache<String, String> DATACENTERS_BY_HOST = CacheBuilder.newBuilder().build();

  private final JmxCassandraManagementProxy proxy;

  private EndpointSnitchInfoProxy(JmxCassandraManagementProxy proxy) {
    this.proxy = proxy;
  }

  public static EndpointSnitchInfoProxy create(ICassandraManagementProxy proxy) {

    return new EndpointSnitchInfoProxy((JmxCassandraManagementProxy) proxy);
  }


  public String getDataCenter() {
    return getDataCenter(proxy.getUntranslatedHost());
  }

  public String getDataCenter(String host) {
    try {
      return DATACENTERS_BY_HOST.get(host, () -> proxy.getEndpointSnitchInfoMBean().getDatacenter(host));
    } catch (ExecutionException ex) {
      throw new IllegalArgumentException(ex);
    }
  }

}