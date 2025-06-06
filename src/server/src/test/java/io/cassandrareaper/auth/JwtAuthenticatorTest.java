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
  public void testAuthenticateValidToken() {
    // Given
    String username = "test-user";
    Set<String> roles = Collections.singleton("user");
    User mockUser = new User(username, roles);
    when(mockUserStore.findUser(username)).thenReturn(mockUser);

    String token = Jwts.builder().subject(username).signWith(jwtKey).compact();

    // When
    Optional<User> result = jwtAuthenticator.authenticate(token);

    // Then
    assertThat(result).isPresent();
    assertThat(result.get().getName()).isEqualTo(username);
  }

  @Test
  public void testAuthenticateInvalidToken() {
    // Given
    String invalidToken = "invalid.jwt.token";

    // When
    Optional<User> result = jwtAuthenticator.authenticate(invalidToken);

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  public void testAuthenticateTokenWithoutSubject() {
    // Given
    String token = Jwts.builder().claim("someOtherClaim", "value").signWith(jwtKey).compact();

    // When
    Optional<User> result = jwtAuthenticator.authenticate(token);

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  public void testAuthenticateUserNotFound() {
    // Given
    String username = "nonexistent-user";
    when(mockUserStore.findUser(username)).thenReturn(null);

    String token = Jwts.builder().subject(username).signWith(jwtKey).compact();

    // When
    Optional<User> result = jwtAuthenticator.authenticate(token);

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  public void testAuthenticateWrongSignature() {
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
