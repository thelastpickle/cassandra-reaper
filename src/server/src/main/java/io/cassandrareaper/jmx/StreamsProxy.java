/*
 * Copyright 2014-2017 Spotify AB
 * Copyright 2016-2018 The Last Pickle Ltd
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


import java.util.Set;

import javax.management.openmbean.CompositeData;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;


public final class StreamsProxy {

  private final JmxProxyImpl proxy;

  private StreamsProxy(JmxProxyImpl proxy) {
    this.proxy = proxy;
  }

  public static StreamsProxy create(JmxProxy proxy) {
    Preconditions.checkArgument(proxy instanceof JmxProxyImpl, "only JmxProxyImpl is supported");
    return new StreamsProxy((JmxProxyImpl)proxy);
  }

  public Set<CompositeData> listStreams() {
    if (proxy.getStreamManagerMBean().isPresent()) {
      return proxy.getStreamManagerMBean().get().getCurrentStreams();
    } else {
      return ImmutableSet.of();
    }
  }
}
