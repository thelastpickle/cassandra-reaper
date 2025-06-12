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

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import javax.crypto.SecretKey;

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

  private static final String JWT_SECRET =
      "MySecretKeyForJWTWhichMustBeLongEnoughForHS256Algorithm";

  @Mock private UserStore mockUserStore;
  private JwtAuthenticator jwtAuthenticator;
  private SecretKey jwtKey;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    jwtKey = Keys.hmacShaKeyFor(JWT_SECRET.getBytes());
    jwtAuthenticator = new JwtAuthenticator(JWT_SECRET, mockUserStore);
  }

  @Test
  public void testAuthenticateValidToken() throws AuthenticationException {
    // Given
    String username = "test-user";
    Set<String> roles = Collections.singleton("user");

    String token =
        Jwts.builder().subject(username).claim("roles", roles).signWith(jwtKey).compact();

    // When
    when(mockUserStore.getUser(username)).thenReturn(Optional.of(new User(username, roles)));
    Optional<User> result = jwtAuthenticator.authenticate(token);

    // Then
    assertThat(result).isPresent();
    assertThat(result.get().getName()).isEqualTo(username);
    assertThat(result.get().hasRole("user")).isTrue();
  }

  @Test
  public void testAuthenticateTokenWithMultipleRoles() throws AuthenticationException {
    // Given
    String username = "admin-user";
    Set<String> roles = Set.of("user", "operator");

    String token =
        Jwts.builder().subject(username).claim("roles", roles).signWith(jwtKey).compact();

    // When
    when(mockUserStore.getUser(username)).thenReturn(Optional.of(new User(username, roles)));
    Optional<User> result = jwtAuthenticator.authenticate(token);

    // Then
    assertThat(result).isPresent();
    assertThat(result.get().getName()).isEqualTo(username);
    assertThat(result.get().hasRole("user")).isTrue();
    assertThat(result.get().hasRole("operator")).isTrue();
  }

  @Test
  public void testAuthenticateTokenWithoutRoles() throws AuthenticationException {
    // Given
    String username = "test-user";

    String token = Jwts.builder().subject(username).signWith(jwtKey).compact();

    // When
    when(mockUserStore.getUser(username))
        .thenReturn(Optional.of(new User(username, Collections.emptySet())));
    Optional<User> result = jwtAuthenticator.authenticate(token);

    // Then
    assertThat(result).isPresent();
    assertThat(result.get().getName()).isEqualTo(username);
    assertThat(result.get().getRoles()).isEmpty();
  }

  @Test
  public void testAuthenticateInvalidToken() throws AuthenticationException {
    // Given
    String invalidToken = "invalid.jwt.token";

    // When
    Optional<User> result = jwtAuthenticator.authenticate(invalidToken);

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  public void testAuthenticateTokenWithoutSubject() throws AuthenticationException {
    // Given
    String token = Jwts.builder().claim("someOtherClaim", "value").signWith(jwtKey).compact();

    // When
    Optional<User> result = jwtAuthenticator.authenticate(token);

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  public void testAuthenticateWrongSignature() throws AuthenticationException {
    // Given
    String username = "test-user";
    SecretKey wrongKey =
        Keys.hmacShaKeyFor("WrongSecretKeyForJWTWhichMustBeLongEnoughForHS256Algorithm".getBytes());

    String token = Jwts.builder().subject(username).signWith(wrongKey).compact();

    // When
    Optional<User> result = jwtAuthenticator.authenticate(token);

    // Then
    assertThat(result).isEmpty();
  }
}
