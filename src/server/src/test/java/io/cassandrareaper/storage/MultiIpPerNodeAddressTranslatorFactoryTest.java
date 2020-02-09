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

package io.cassandrareaper.storage;

import io.cassandrareaper.storage.cassandra.MultiIpPerNodeAddressTranslator;
import io.cassandrareaper.storage.cassandra.MultiIpPerNodeAddressTranslatorFactory;
import io.cassandrareaper.storage.cassandra.MultiIpPerNodeAddressTranslatorFactory.AddressTranslation;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import io.dropwizard.jackson.DiscoverableSubtypeResolver;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

public class MultiIpPerNodeAddressTranslatorFactoryTest {

  @Test
  public void isDiscoverable() {
    assertTrue("problem with discovering custom factory",
        new DiscoverableSubtypeResolver().getDiscoveredSubtypes()
        .contains(MultiIpPerNodeAddressTranslatorFactory.class));
  }

  @Test
  public void shouldReturnSameAddressWhenNoEntryFound() {
    MultiIpPerNodeAddressTranslatorFactory factory = new MultiIpPerNodeAddressTranslatorFactory();
    List<AddressTranslation> addressTranslations = new ArrayList<>();
    factory.setAddressTranslations(addressTranslations);
    MultiIpPerNodeAddressTranslator translator = (MultiIpPerNodeAddressTranslator) factory.build();

    InetSocketAddress address = new InetSocketAddress("123.2.23.109", 9042);
    assertThat(translator.translate(address)).isEqualTo(address);
  }

  @Test
  public void shouldReturnNewAddressWhenMatchFound() {
    List<AddressTranslation> addressTranslations = new ArrayList<>();
    AddressTranslation addressTranslation = new AddressTranslation();
    addressTranslation.setFrom("1.1.1.1");
    addressTranslation.setTo("2.2.2.2");
    addressTranslations.add(addressTranslation);
    MultiIpPerNodeAddressTranslatorFactory factory = new MultiIpPerNodeAddressTranslatorFactory();
    factory.setAddressTranslations(addressTranslations);
    MultiIpPerNodeAddressTranslator translator = (MultiIpPerNodeAddressTranslator) factory.build();

    InetSocketAddress expectedAddress = new InetSocketAddress("2.2.2.2", 9042);
    InetSocketAddress address = new InetSocketAddress("1.1.1.1", 9042);
    assertThat(translator.translate(address)).isEqualTo(expectedAddress);
  }
}
