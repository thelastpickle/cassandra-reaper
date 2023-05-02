/*
 * Copyright 2019-2019 The Last Pickle Ltd
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

package io.cassandrareaper.storage.cassandra.network;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.policies.AddressTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps broadcast addresses (as advertised by the cassandra cluster) to effective addresses using a hard-coded mapping
 */
public final class MultiIpPerNodeAddressTranslator implements AddressTranslator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiIpPerNodeAddressTranslator.class);
  private Map<String, String> addressTranslation = new HashMap<>();

  public MultiIpPerNodeAddressTranslator(
      final List<MultiIpPerNodeAddressTranslatorFactory.AddressTranslation> translations) {
    if (!translations.isEmpty()) {
      addressTranslation = new HashMap<>(translations.size());
      translations.forEach(i -> addressTranslation.put(i.getFrom(), i.getTo()));
      if (translations.size() != addressTranslation.size()) {
        throw new IllegalArgumentException("Invalid mapping specified - some mappings are defined multiple times");
      }
      LOGGER.info("Initialised cassandra address translator {}", addressTranslation);
    }
  }

  @Override
  public void init(Cluster cluster) {
    // nothing to do
  }

  @Override
  public InetSocketAddress translate(final InetSocketAddress broadcastAddress) {
    final String from = broadcastAddress.getAddress().getHostAddress();
    final String to = addressTranslation.get(from);
    if (to != null) {
      final InetSocketAddress result = new InetSocketAddress(to, broadcastAddress.getPort());
      LOGGER.debug("Performed cassandra address translation from {} to {}", from, result);
      return result;
    } else {
      return broadcastAddress;
    }
  }

  @Override
  public void close() {
    //do nothing
  }
}
