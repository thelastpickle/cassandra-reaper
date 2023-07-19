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

package io.cassandrareaper.management.jmx;

import io.cassandrareaper.management.ICassandraManagementProxy;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import org.apache.cassandra.service.StorageServiceMBean;


public final class StorageServiceProxy {

  private final JmxCassandraManagementProxy proxy;
  private final StorageServiceMBean ssProxy;

  private StorageServiceProxy(JmxCassandraManagementProxy proxy) {
    this.proxy = proxy;
    this.ssProxy = proxy.getStorageServiceMBean();
  }

  public static StorageServiceProxy create(ICassandraManagementProxy proxy) {
    Preconditions.checkArgument(proxy instanceof JmxCassandraManagementProxy, "only JmxProxyImpl is supported");
    return new StorageServiceProxy((JmxCassandraManagementProxy) proxy);
  }

  public Map<String, List<String>> getTokensByNode() {
    Preconditions.checkNotNull(ssProxy, "Looks like the proxy is not connected");

    Map<String, String> tokenToEndpointMap = ssProxy.getTokenToEndpointMap();
    return tokenToEndpointMap
        .entrySet()
        .stream()
        .collect(
            Collectors.groupingBy(
                Entry::getValue, Collectors.mapping(Entry::getKey, Collectors.toList())));

  }


}