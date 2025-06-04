/*
 *
 * Copyright 2022-2022 The Last Pickle Ltd
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

package io.cassandrareaper.resources;

import java.time.Duration;

import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.HttpMethod;

public final class RequestUtils {
  private static boolean isCorsEnabled = false;
  private static Duration sessionTimeout = Duration.ofMinutes(-1);

  private RequestUtils() {}

  public static void setCorsEnabled(boolean enabled) {
    isCorsEnabled = enabled;
  }

  public static boolean isCorsEnabled() {
    return isCorsEnabled;
  }

  public static void setSessionTimeout(Duration configSessionTimeout) {
    RequestUtils.sessionTimeout = configSessionTimeout;
  }

  public static Duration getSessionTimeout() {
    return sessionTimeout;
  }

  public static boolean isOptionsRequest(ServletRequest request) {
    if (request != null && request instanceof HttpServletRequest) {
      if (((HttpServletRequest) request).getMethod().equalsIgnoreCase(HttpMethod.OPTIONS)) {
        return true;
      }
    }
    return false;
  }
}
