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

import io.cassandrareaper.auth.AuthLoginResource;
import io.cassandrareaper.auth.User;
import io.cassandrareaper.auth.UserStore;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.junit.Before;
import org.junit.Test;

public final class LoginResourceTest {

  private UserStore userStore;
  private AuthLoginResource loginResource;
  private static final String JWT_SECRET =
      "MySecretKeyForJWTWhichMustBeLongEnoughForHS256Algorithm";

  @Before
  public void setUp() {
    userStore = new UserStore();
    // Add test users explicitly since we removed default users for security
    userStore.addUser("admin", "admin", java.util.Set.of("operator"));
    userStore.addUser("user", "user", java.util.Set.of("user"));
    loginResource = new AuthLoginResource(userStore, JWT_SECRET);
  }

  @Test
  public void testUserStoreAddedUsers() {
    // Test that explicitly added users are available
    User adminUser = userStore.findUser("admin");
    assertThat(adminUser).isNotNull();
    assertThat(adminUser.getName()).isEqualTo("admin");
    assertThat(adminUser.hasRole("operator")).isTrue();

    User regularUser = userStore.findUser("user");
    assertThat(regularUser).isNotNull();
    assertThat(regularUser.getName()).isEqualTo("user");
    assertThat(regularUser.hasRole("user")).isTrue();
  }

  @Test
  public void testAuthentication() {
    // Test admin authentication
    assertThat(userStore.authenticate("admin", "admin")).isTrue();
    assertThat(userStore.authenticate("admin", "wrong")).isFalse();

    // Test user authentication
    assertThat(userStore.authenticate("user", "user")).isTrue();
    assertThat(userStore.authenticate("user", "wrong")).isFalse();

    // Test non-existent user
    assertThat(userStore.authenticate("nonexistent", "password")).isFalse();
  }

  @Test
  public void testLogin() {
    // Test successful login
    AuthLoginResource.LoginResponse response = loginResource.login("admin", "admin", false);
    assertThat(response).isNotNull();
    assertThat(response.getToken()).isNotNull();
    assertThat(response.getUsername()).isEqualTo("admin");
    assertThat(response.getRoles()).contains("operator");
  }

  @Test(expected = jakarta.ws.rs.WebApplicationException.class)
  public void testLoginFailure() {
    // Test failed login
    loginResource.login("admin", "wrong_password", false);
  }

  @Test
  public void testJwtTokenWithConfiguredExpirationTime() {
    // Use sessionTimeout directly - no JWT config needed
    Duration sessionTimeout = Duration.ofMinutes(5);

    // Create login resource with custom session timeout
    AuthLoginResource customLoginResource =
        new AuthLoginResource(userStore, JWT_SECRET, null, sessionTimeout);

    // Perform login
    Instant beforeLogin = Instant.now();
    AuthLoginResource.LoginResponse response = customLoginResource.login("admin", "admin", false);
    Instant afterLogin = Instant.now();

    // Verify token was created
    assertThat(response).isNotNull();
    assertThat(response.getToken()).isNotNull();

    // Parse the JWT token to verify expiration time
    Claims claims =
        Jwts.parser()
            .verifyWith(Keys.hmacShaKeyFor(JWT_SECRET.getBytes()))
            .build()
            .parseSignedClaims(response.getToken())
            .getPayload();

    Date expirationDate = claims.getExpiration();
    Instant expiration = expirationDate.toInstant();

    // The token should expire approximately 5 minutes after login (with some tolerance)
    Duration expectedExpiration = Duration.ofMinutes(5);
    Instant expectedExpiry = beforeLogin.plus(expectedExpiration).minusSeconds(1);
    Instant expectedExpiryEnd = afterLogin.plus(expectedExpiration).plusSeconds(1);

    assertThat(expiration).isBetween(expectedExpiry, expectedExpiryEnd);
  }

  @Test
  public void testJwtTokenWithRememberMeIgnoresConfiguration() {
    // Use sessionTimeout directly - no JWT config needed
    Duration sessionTimeout = Duration.ofMinutes(5);

    // Create login resource with custom session timeout
    AuthLoginResource customLoginResource =
        new AuthLoginResource(userStore, JWT_SECRET, null, sessionTimeout);

    // Perform login with rememberMe=true
    Instant beforeLogin = Instant.now();
    AuthLoginResource.LoginResponse response = customLoginResource.login("admin", "admin", true);
    Instant afterLogin = Instant.now();

    // Verify token was created
    assertThat(response).isNotNull();
    assertThat(response.getToken()).isNotNull();

    // Parse the JWT token to verify expiration time
    Claims claims =
        Jwts.parser()
            .verifyWith(Keys.hmacShaKeyFor(JWT_SECRET.getBytes()))
            .build()
            .parseSignedClaims(response.getToken())
            .getPayload();

    Date expirationDate = claims.getExpiration();
    Instant expiration = expirationDate.toInstant();

    // With rememberMe=true, token should expire in 30 days regardless of configuration
    Duration expectedExpiration = Duration.ofDays(30);
    Instant expectedExpiry = beforeLogin.plus(expectedExpiration).minusSeconds(1);
    Instant expectedExpiryEnd = afterLogin.plus(expectedExpiration).plusSeconds(1);

    assertThat(expiration).isBetween(expectedExpiry, expectedExpiryEnd);
  }
}
