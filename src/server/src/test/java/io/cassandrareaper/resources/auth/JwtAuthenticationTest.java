/*
 *
 * Copyright 2019-2019 The Last Pickle Ltd
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

import io.cassandrareaper.auth.JwtAuthenticator;
import io.cassandrareaper.auth.RoleAuthorizer;
import io.cassandrareaper.auth.User;
import io.cassandrareaper.auth.UserStore;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import javax.crypto.SecretKey;
import javax.ws.rs.container.ContainerRequestContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public final class JwtAuthenticationTest {

  private static final String JWT_SECRET =
      "MySecretKeyForJWTWhichMustBeLongEnoughForHS256Algorithm";

  @Mock private UserStore mockUserStore;

  @Mock private ContainerRequestContext mockRequestContext;

  private JwtAuthenticator jwtAuthenticator;
  private RoleAuthorizer roleAuthorizer;
  private SecretKey jwtKey;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    jwtKey = Keys.hmacShaKeyFor(JWT_SECRET.getBytes());
    jwtAuthenticator = new JwtAuthenticator(JWT_SECRET, mockUserStore);
    roleAuthorizer = new RoleAuthorizer();
  }

  @Test
  public void testValidJwtAuthentication() {
    // Given
    String username = "test-user";
    Set<String> userRoles = Collections.singleton("user");
    User mockUser = new User(username, userRoles);
    when(mockUserStore.findUser(username)).thenReturn(mockUser);

    String token = Jwts.builder().subject(username).signWith(jwtKey).compact();

    // When
    Optional<User> result = jwtAuthenticator.authenticate(token);

    // Then
    assertThat(result).isPresent();
    assertThat(result.get().getName()).isEqualTo(username);
    assertThat(result.get().hasRole("user")).isTrue();
  }

  @Test
  public void testInvalidJwtAuthentication() {
    // Given
    String invalidToken = "invalid.jwt.token";

    // When
    Optional<User> result = jwtAuthenticator.authenticate(invalidToken);

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  public void testOperatorRoleAuthorization() {
    // Given
    Set<String> operatorRoles = Collections.singleton("operator");
    User operatorUser = new User("operator-user", operatorRoles);
    when(mockRequestContext.getMethod()).thenReturn("POST");

    // When & Then
    assertThat(roleAuthorizer.authorize(operatorUser, "operator", mockRequestContext)).isTrue();
    assertThat(roleAuthorizer.authorize(operatorUser, "user", mockRequestContext))
        .isTrue(); // operator has all permissions
    assertThat(roleAuthorizer.authorize(operatorUser, "any-role", mockRequestContext)).isTrue();
  }

  @Test
  public void testUserRoleAuthorization() {
    // Given
    Set<String> userRoles = Collections.singleton("user");
    User regularUser = new User("regular-user", userRoles);
    when(mockRequestContext.getMethod()).thenReturn("GET");

    // When & Then
    assertThat(roleAuthorizer.authorize(regularUser, "user", mockRequestContext)).isTrue();
    assertThat(roleAuthorizer.authorize(regularUser, "operator", mockRequestContext)).isFalse();
  }

  @Test
  public void testUserRoleCannotWriteOperations() {
    // Given
    Set<String> userRoles = Collections.singleton("user");
    User regularUser = new User("regular-user", userRoles);
    when(mockRequestContext.getMethod()).thenReturn("POST");

    // When & Then
    assertThat(roleAuthorizer.authorize(regularUser, "operator", mockRequestContext)).isFalse();
  }

  @Test
  public void testUserWithoutRequiredRole() {
    // Given
    Set<String> userRoles = Collections.singleton("user");
    User regularUser = new User("regular-user", userRoles);
    when(mockRequestContext.getMethod()).thenReturn("GET");

    // When & Then
    assertThat(roleAuthorizer.authorize(regularUser, "admin", mockRequestContext)).isFalse();
  }
}
