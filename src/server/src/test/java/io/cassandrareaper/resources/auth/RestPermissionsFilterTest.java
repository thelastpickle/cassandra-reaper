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

package io.cassandrareaper.resources.auth;

import io.cassandrareaper.auth.RoleAuthorizer;
import io.cassandrareaper.auth.User;

import java.util.Collections;
import java.util.Set;

import javax.ws.rs.container.ContainerRequestContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class RestPermissionsFilterTest {

  @Mock private ContainerRequestContext mockRequestContext;

  private RoleAuthorizer roleAuthorizer;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    roleAuthorizer = new RoleAuthorizer();
  }

  @Test
  public void testOptionsRequestAccess() {
    // Given
    Set<String> userRoles = Collections.singleton("user");
    User regularUser = new User("regular-user", userRoles);
    when(mockRequestContext.getMethod()).thenReturn("OPTIONS");

    // When & Then - OPTIONS requests typically don't require special authorization
    // This would be handled at a higher level in the Dropwizard auth framework
    assertThat(roleAuthorizer.authorize(regularUser, "user", mockRequestContext)).isTrue();
  }

  @Test
  public void testUserReadAccess() {
    // Given
    Set<String> userRoles = Collections.singleton("user");
    User regularUser = new User("regular-user", userRoles);
    when(mockRequestContext.getMethod()).thenReturn("GET");

    // When & Then
    assertThat(roleAuthorizer.authorize(regularUser, "user", mockRequestContext)).isTrue();
  }

  @Test
  public void testUserWriteAccessDenied() {
    // Given
    Set<String> userRoles = Collections.singleton("user");
    User regularUser = new User("regular-user", userRoles);
    when(mockRequestContext.getMethod()).thenReturn("POST");

    // When & Then - Users with "user" role should be denied POST operations
    assertThat(roleAuthorizer.authorize(regularUser, "operator", mockRequestContext)).isFalse();
    assertThat(roleAuthorizer.authorize(regularUser, "user", mockRequestContext)).isFalse();
  }

  @Test
  public void testUserCannotDoWriteOperationsRegardlessOfRequiredRole() {
    // Given
    Set<String> userRoles = Collections.singleton("user");
    User regularUser = new User("regular-user", userRoles);

    // When & Then - Users with "user" role should only be allowed GET operations
    when(mockRequestContext.getMethod()).thenReturn("GET");
    assertThat(roleAuthorizer.authorize(regularUser, "user", mockRequestContext)).isTrue();
    assertThat(roleAuthorizer.authorize(regularUser, "operator", mockRequestContext))
        .isTrue(); // user can access operator endpoints with GET

    when(mockRequestContext.getMethod()).thenReturn("POST");
    assertThat(roleAuthorizer.authorize(regularUser, "user", mockRequestContext)).isFalse();
    assertThat(roleAuthorizer.authorize(regularUser, "operator", mockRequestContext)).isFalse();

    when(mockRequestContext.getMethod()).thenReturn("PUT");
    assertThat(roleAuthorizer.authorize(regularUser, "user", mockRequestContext)).isFalse();
    assertThat(roleAuthorizer.authorize(regularUser, "operator", mockRequestContext)).isFalse();

    when(mockRequestContext.getMethod()).thenReturn("DELETE");
    assertThat(roleAuthorizer.authorize(regularUser, "user", mockRequestContext)).isFalse();
    assertThat(roleAuthorizer.authorize(regularUser, "operator", mockRequestContext)).isFalse();
  }

  @Test
  public void testOperatorHasFullAccess() {
    // Given
    Set<String> operatorRoles = Collections.singleton("operator");
    User operatorUser = new User("operator-user", operatorRoles);

    // When & Then - Operators should have access to all methods
    when(mockRequestContext.getMethod()).thenReturn("GET");
    assertThat(roleAuthorizer.authorize(operatorUser, "user", mockRequestContext)).isTrue();
    assertThat(roleAuthorizer.authorize(operatorUser, "operator", mockRequestContext)).isTrue();

    when(mockRequestContext.getMethod()).thenReturn("POST");
    assertThat(roleAuthorizer.authorize(operatorUser, "user", mockRequestContext)).isTrue();
    assertThat(roleAuthorizer.authorize(operatorUser, "operator", mockRequestContext)).isTrue();

    when(mockRequestContext.getMethod()).thenReturn("DELETE");
    assertThat(roleAuthorizer.authorize(operatorUser, "operator", mockRequestContext)).isTrue();
  }
}
