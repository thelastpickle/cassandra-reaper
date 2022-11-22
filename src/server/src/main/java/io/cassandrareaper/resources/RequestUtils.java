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

import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.HttpMethod;

import com.google.common.annotations.VisibleForTesting;

public final class RequestUtils {
  public static final String ENABLE_CORS_ENV_VAR_NAME = "REAPER_ENABLE_CROSS_ORIGIN";

  private RequestUtils() {}

  public static boolean isOptionsRequest(ServletRequest request) {
    if (request != null && request instanceof HttpServletRequest) {
      if (((HttpServletRequest) request).getMethod().equalsIgnoreCase(HttpMethod.OPTIONS)) {
        return true;
      }
    }
    return false;
  }

  @VisibleForTesting
  static boolean isCorsEnabled(String corsEnabledEnvVarValue) {
    if (corsEnabledEnvVarValue != null) {
      return Boolean.parseBoolean(corsEnabledEnvVarValue.trim().toLowerCase());
    }
    return false;
  }

  public static boolean isCorsEnabled() {
    String corsEnabledEnvVarValue = System.getenv(ENABLE_CORS_ENV_VAR_NAME);
    return isCorsEnabled(corsEnabledEnvVarValue);
  }
}
