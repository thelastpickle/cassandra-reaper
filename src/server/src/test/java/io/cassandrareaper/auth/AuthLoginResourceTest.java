/*
 * Copyright 2025-2025 DataStax, Inc.
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

import java.time.Duration;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.WebApplicationException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class AuthLoginResourceTest {

  private static final String JWT_SECRET = "test-secret-key-that-is-long-enough-for-hmac-256";
  private static final String USERNAME = "testuser";
  private static final String PASSWORD = "testpass";

  @Mock private UserStore userStore;

  private AuthLoginResource resource;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    resource = new AuthLoginResource(userStore, JWT_SECRET);
  }

  @Test
  public void testLogin_successful() {
    // Given
    User user = new User(USERNAME, Set.of("user"));
    when(userStore.authenticate(USERNAME, PASSWORD)).thenReturn(true);
    when(userStore.findUser(USERNAME)).thenReturn(user);

    // When
    AuthLoginResource.LoginResponse response = resource.login(USERNAME, PASSWORD, false);

    // Then
    assertThat(response).isNotNull();
    assertThat(response.getToken()).isNotNull();
    assertThat(response.getUsername()).isEqualTo(USERNAME);
    assertThat(response.getRoles()).containsExactly("user");
  }

  @Test
  public void testLogin_successfulWithRememberMe() {
    // Given
    User user = new User(USERNAME, Set.of("admin", "operator"));
    when(userStore.authenticate(USERNAME, PASSWORD)).thenReturn(true);
    when(userStore.findUser(USERNAME)).thenReturn(user);

    // When
    AuthLoginResource.LoginResponse response = resource.login(USERNAME, PASSWORD, true);

    // Then
    assertThat(response).isNotNull();
    assertThat(response.getToken()).isNotNull();
    assertThat(response.getUsername()).isEqualTo(USERNAME);
    assertThat(response.getRoles()).containsExactlyInAnyOrder("admin", "operator");
  }

  @Test
  public void testLogin_failsWithBlankUsername() {
    // Given/When/Then
    assertThatThrownBy(() -> resource.login("", PASSWORD, false))
        .isInstanceOf(WebApplicationException.class)
        .hasMessageContaining("missing username");
  }

  @Test
  public void testLogin_failsWithNullUsername() {
    // Given/When/Then
    assertThatThrownBy(() -> resource.login(null, PASSWORD, false))
        .isInstanceOf(WebApplicationException.class)
        .hasMessageContaining("missing username");
  }

  @Test
  public void testLogin_failsWithBlankPassword() {
    // Given/When/Then
    assertThatThrownBy(() -> resource.login(USERNAME, "", false))
        .isInstanceOf(WebApplicationException.class)
        .hasMessageContaining("missing password");
  }

  @Test
  public void testLogin_failsWithNullPassword() {
    // Given/When/Then
    assertThatThrownBy(() -> resource.login(USERNAME, null, false))
        .isInstanceOf(WebApplicationException.class)
        .hasMessageContaining("missing password");
  }

  @Test
  public void testLogin_failsWithInvalidCredentials() {
    // Given
    when(userStore.authenticate(USERNAME, PASSWORD)).thenReturn(false);

    // When/Then
    assertThatThrownBy(() -> resource.login(USERNAME, PASSWORD, false))
        .isInstanceOf(WebApplicationException.class)
        .hasMessageContaining("Invalid credentials");
  }

  @Test
  public void testLogin_failsWhenUserNotFound() {
    // Given
    when(userStore.authenticate(USERNAME, PASSWORD)).thenReturn(true);
    when(userStore.findUser(USERNAME)).thenReturn(null);

    // When/Then
    assertThatThrownBy(() -> resource.login(USERNAME, PASSWORD, false))
        .isInstanceOf(WebApplicationException.class)
        .hasMessageContaining("User not found");
  }

  @Test
  public void testConstructorWithSessionTimeout() {
    // Given
    Duration sessionTimeout = Duration.ofMinutes(30);

    // When
    AuthLoginResource resourceWithTimeout =
        new AuthLoginResource(userStore, JWT_SECRET, null, sessionTimeout);

    // Then
    assertThat(resourceWithTimeout).isNotNull();
  }

  @Test
  public void testLoginResponse_getters() {
    // Given
    String token = "test.jwt.token";
    Set<String> roles = Set.of("admin", "user");

    // When
    AuthLoginResource.LoginResponse response =
        new AuthLoginResource.LoginResponse(token, USERNAME, roles);

    // Then
    assertThat(response.getToken()).isEqualTo(token);
    assertThat(response.getUsername()).isEqualTo(USERNAME);
    assertThat(response.getRoles()).isEqualTo(roles);
  }

  @Test
  public void testLogin_handlesWhitespaceUsername() {
    // Given/When/Then
    assertThatThrownBy(() -> resource.login("   ", PASSWORD, false))
        .isInstanceOf(WebApplicationException.class)
        .hasMessageContaining("missing username");
  }

  @Test
  public void testLogin_handlesWhitespacePassword() {
    // Given/When/Then
    assertThatThrownBy(() -> resource.login(USERNAME, "   ", false))
        .isInstanceOf(WebApplicationException.class)
        .hasMessageContaining("missing password");
  }

  @Test
  public void testLogin_multipleRoles() {
    // Given
    Set<String> multipleRoles = Set.of("user", "admin", "operator", "viewer");
    User user = new User(USERNAME, multipleRoles);
    when(userStore.authenticate(USERNAME, PASSWORD)).thenReturn(true);
    when(userStore.findUser(USERNAME)).thenReturn(user);

    // When
    AuthLoginResource.LoginResponse response = resource.login(USERNAME, PASSWORD, false);

    // Then
    assertThat(response.getRoles()).hasSize(4);
    assertThat(response.getRoles()).containsExactlyInAnyOrderElementsOf(multipleRoles);
  }

  @Test
  public void testLogin_emptyRoles() {
    // Given
    User user = new User(USERNAME, Set.of());
    when(userStore.authenticate(USERNAME, PASSWORD)).thenReturn(true);
    when(userStore.findUser(USERNAME)).thenReturn(user);

    // When
    AuthLoginResource.LoginResponse response = resource.login(USERNAME, PASSWORD, false);

    // Then
    assertThat(response.getRoles()).isEmpty();
  }
}
