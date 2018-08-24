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

import java.net.MalformedURLException;
import javax.management.remote.JMXServiceURL;

import com.google.common.base.Preconditions;

public final class JmxAddresses {
  private static final String JMX_URL = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";

  private JmxAddresses() {

  }

  public static boolean isNumericIPv6Address(String address) {
    Preconditions.checkNotNull(address);
    // address contains colon if and only if it's a numeric IPv6 address
    return (address.indexOf(':') >= 0);
  }

  public static JMXServiceURL getJmxServiceUrl(String host, int port)
        throws MalformedURLException {
    Preconditions.checkNotNull(host);
    Preconditions.checkArgument(port > 0);
    String effectiveHost = isNumericIPv6Address(host) ? wrapIPv6BracesIfNeed(host) : host;
    return new JMXServiceURL(String.format(JMX_URL, effectiveHost, port));
  }

  private static String wrapIPv6BracesIfNeed(String address) {
    if (address.startsWith("[") && address.endsWith("]")) {
      return address;
    }
    if (address.startsWith("[") || address.endsWith("]")) {
      throw new IllegalArgumentException("Invalid IPv6 address to be enclosed with braces:" + address);
    }
    return "[" + address + "]";
  }
}
