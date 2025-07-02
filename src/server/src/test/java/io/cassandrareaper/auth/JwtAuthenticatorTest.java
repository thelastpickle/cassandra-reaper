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

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import io.dropwizard.auth.AuthenticationException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class JwtAuthenticatorTest {

  private static final String JWT_SECRET = "test-secret-key-that-is-long-enough-for-hmac-256";
  private static final String USERNAME = "testuser";

  @Mock private UserStore userStore;

  private JwtAuthenticator authenticator;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    authenticator = new JwtAuthenticator(JWT_SECRET, userStore);
  }

  @Test
  public void testAuthenticate_validToken() throws AuthenticationException {
    // Given
    String validToken = createValidToken();
    User user = new User(USERNAME, Set.of("user"));
    when(userStore.getUser(USERNAME)).thenReturn(Optional.of(user));

    // When
    Optional<User> result = authenticator.authenticate(validToken);

    // Then
    assertThat(result).isPresent();
    assertThat(result.get().getName()).isEqualTo(USERNAME);
    assertThat(result.get().getRoles()).containsExactly("user");
  }

  @Test
  public void testAuthenticate_nullToken() throws AuthenticationException {
    // Given/When
    Optional<User> result = authenticator.authenticate(null);

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  public void testAuthenticate_emptyToken() throws AuthenticationException {
    // Given/When
    Optional<User> result = authenticator.authenticate("");

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  public void testAuthenticate_whitespaceToken() throws AuthenticationException {
    // Given/When
    Optional<User> result = authenticator.authenticate("   ");

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  public void testAuthenticate_invalidToken() throws AuthenticationException {
    // Given
    String invalidToken = "invalid.jwt.token";

    // When
    Optional<User> result = authenticator.authenticate(invalidToken);

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  public void testAuthenticate_expiredToken() throws AuthenticationException {
    // Given
    String expiredToken = createExpiredToken();

    // When
    Optional<User> result = authenticator.authenticate(expiredToken);

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  public void testAuthenticate_tokenWithoutSubject() throws AuthenticationException {
    // Given
    String tokenWithoutSubject = createTokenWithoutSubject();

    // When
    Optional<User> result = authenticator.authenticate(tokenWithoutSubject);

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  public void testAuthenticate_userNotFoundInStore() throws AuthenticationException {
    // Given
    String validToken = createValidToken();
    when(userStore.getUser(USERNAME)).thenReturn(Optional.empty());

    // When
    Optional<User> result = authenticator.authenticate(validToken);

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  public void testAuthenticate_multipleRoles() throws AuthenticationException {
    // Given
    String validToken = createValidToken();
    Set<String> roles = Set.of("user", "admin", "operator");
    User user = new User(USERNAME, roles);
    when(userStore.getUser(USERNAME)).thenReturn(Optional.of(user));

    // When
    Optional<User> result = authenticator.authenticate(validToken);

    // Then
    assertThat(result).isPresent();
    assertThat(result.get().getRoles()).containsExactlyInAnyOrderElementsOf(roles);
  }

  @Test
  public void testAuthenticate_malformedToken() throws AuthenticationException {
    // Given
    String malformedToken = "not-a-jwt-token-at-all";

    // When
    Optional<User> result = authenticator.authenticate(malformedToken);

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  public void testAuthenticate_tokenWithWrongSignature() throws AuthenticationException {
    // Given
    String tokenWithWrongSignature = createTokenWithWrongSignature();

    // When
    Optional<User> result = authenticator.authenticate(tokenWithWrongSignature);

    // Then
    assertThat(result).isEmpty();
  }

  private String createValidToken() {
    return Jwts.builder()
        .setSubject(USERNAME)
        .setIssuedAt(new Date())
        .setExpiration(Date.from(Instant.now().plus(1, ChronoUnit.HOURS)))
        .signWith(Keys.hmacShaKeyFor(JWT_SECRET.getBytes()))
        .compact();
  }

  private String createExpiredToken() {
    return Jwts.builder()
        .setSubject(USERNAME)
        .setIssuedAt(Date.from(Instant.now().minus(2, ChronoUnit.HOURS)))
        .setExpiration(Date.from(Instant.now().minus(1, ChronoUnit.HOURS)))
        .signWith(Keys.hmacShaKeyFor(JWT_SECRET.getBytes()))
        .compact();
  }

  private String createTokenWithoutSubject() {
    return Jwts.builder()
        .setIssuedAt(new Date())
        .setExpiration(Date.from(Instant.now().plus(1, ChronoUnit.HOURS)))
        .signWith(Keys.hmacShaKeyFor(JWT_SECRET.getBytes()))
        .compact();
  }

  private String createTokenWithWrongSignature() {
    String wrongSecret = "wrong-secret-key-that-is-long-enough-for-hmac-256";
    return Jwts.builder()
        .setSubject(USERNAME)
        .setIssuedAt(new Date())
        .setExpiration(Date.from(Instant.now().plus(1, ChronoUnit.HOURS)))
        .signWith(Keys.hmacShaKeyFor(wrongSecret.getBytes()))
        .compact();
  }
}
