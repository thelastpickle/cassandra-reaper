/*
 * Copyright 2014-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
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

package io.cassandrareaper.auth;

import io.dropwizard.auth.Authorizer;
import jakarta.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RoleAuthorizer implements Authorizer<User> {

  private static final Logger LOG = LoggerFactory.getLogger(RoleAuthorizer.class);

  @Override
  public boolean authorize(
      User user,
      String role,
      @Nullable jakarta.ws.rs.container.ContainerRequestContext requestContext) {

    String method = requestContext != null ? requestContext.getMethod() : "unknown";
    String path = "unknown";
    if (requestContext != null && requestContext.getUriInfo() != null) {
      path = requestContext.getUriInfo().getPath();
    }

    LOG.info(
        "Authorization check: user={}, roles={}, required_role={}, method={}, path={}",
        user.getName(),
        user.getRoles(),
        role,
        method,
        path);

    // Operator role has all permissions
    if (user.hasRole("operator")) {
      LOG.info("User {} has operator role - access granted", user.getName());
      return true;
    }

    // Users with "user" role can only perform read operations (GET), regardless of required role
    if (user.hasRole("user")) {
      if (requestContext != null) {
        // Only allow GET and OPTIONS methods for users with "user" role
        boolean allowed = "GET".equals(method) || "OPTIONS".equals(method);
        LOG.info("User {} has user role - method {} allowed: {}", user.getName(), method, allowed);
        return allowed;
      }
      // If no request context, deny access for safety
      LOG.info("User {} has user role but no request context - access denied", user.getName());
      return false;
    }

    // For other roles, check if user has the exact required role
    boolean hasRole = user.hasRole(role);
    LOG.info("User {} role check for '{}': {}", user.getName(), role, hasRole);
    return hasRole;
  }
}
