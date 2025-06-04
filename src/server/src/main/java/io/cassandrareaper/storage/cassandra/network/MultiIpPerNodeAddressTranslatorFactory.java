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

import java.util.ArrayList;
import java.util.List;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.cassandra.DropwizardProgrammaticDriverConfigLoaderBuilder;
import io.dropwizard.cassandra.network.AddressTranslatorFactory;

/**
 * A factory for configuring and building custom {@link
 * com.datastax.driver.core.policies.AddressTranslator} instance.
 */
@JsonTypeName("multiIpPerNode")
public class MultiIpPerNodeAddressTranslatorFactory implements AddressTranslatorFactory {
  @JsonProperty("ipTranslations")
  @VisibleForTesting
  public static List<AddressTranslation> addressTranslations = new ArrayList<>();

  public static void setAddressTranslations(List<AddressTranslation> addressTranslations) {
    MultiIpPerNodeAddressTranslatorFactory.addressTranslations = addressTranslations;
  }

  @Override
  public void accept(DropwizardProgrammaticDriverConfigLoaderBuilder builder) {
    builder.withClass(
        DefaultDriverOption.ADDRESS_TRANSLATOR_CLASS, MultiIpPerNodeAddressTranslator.class);
  }

  public static class AddressTranslation {
    /**
     * An IP address as returned by {@link java.net.InetAddress#getHostAddress()}. This IP address
     * will be tranlated to the "to" hostname.
     */
    @JsonProperty private String from;

    /** An IP address or hostname to translate to. */
    @JsonProperty private String to;

    public String getFrom() {
      return from;
    }

    public AddressTranslation setFrom(final String from) {
      this.from = from;
      return this;
    }

    public String getTo() {
      return to;
    }

    public AddressTranslation setTo(final String to) {
      this.to = to;
      return this;
    }
  }
}
