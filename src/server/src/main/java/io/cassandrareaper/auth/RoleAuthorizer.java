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

import javax.annotation.Nullable;

import io.dropwizard.auth.Authorizer;

public class RoleAuthorizer implements Authorizer<User> {

  @Override
  public boolean authorize(
      User user,
      String role,
      @Nullable javax.ws.rs.container.ContainerRequestContext requestContext) {
    // Check if user has the required role
    if (user.hasRole(role)) {
      return true;
    }

    // Operator role has all permissions
    if (user.hasRole("operator")) {
      return true;
    }

    // For REST endpoints, users with "user" role can perform read operations
    // only if the required role is also "user"
    if (requestContext != null && "user".equals(role)) {
      String method = requestContext.getMethod();
      // Users with "user" role can perform read operations on "user" endpoints
      if (user.hasRole("user") && "GET".equals(method)) {
        return true;
      }
    }

    return false;
  }
}
