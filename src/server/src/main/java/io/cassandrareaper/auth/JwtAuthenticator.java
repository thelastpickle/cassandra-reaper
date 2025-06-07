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

import java.security.Key;
import java.util.Optional;

import javax.crypto.SecretKey;

import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.Authenticator;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JwtAuthenticator implements Authenticator<String, User> {
  private static final Logger LOG = LoggerFactory.getLogger(JwtAuthenticator.class);

  private final Key jwtKey;
  private final UserStore userStore;

  public JwtAuthenticator(String jwtSecret, UserStore userStore) {
    this.jwtKey = Keys.hmacShaKeyFor(jwtSecret.getBytes());
    this.userStore = userStore;
  }

  @Override
  public Optional<User> authenticate(String token) throws AuthenticationException {
    LOG.info(
        "JWT AUTHENTICATOR: authenticate() called with token: {}",
        token != null ? "[PRESENT]" : "[NULL]");

    if (token == null || token.trim().isEmpty()) {
      LOG.info("JWT AUTHENTICATOR: Token is null or empty - authentication failed");
      return Optional.empty();
    }

    try {
      // Parse and validate the JWT token
      Jws<Claims> jws =
          Jwts.parser().verifyWith((SecretKey) jwtKey).build().parseSignedClaims(token);
      Claims claims = jws.getPayload();

      String username = claims.getSubject();
      LOG.info("JWT AUTHENTICATOR: Successfully parsed JWT for user: {}", username);

      if (username == null) {
        LOG.info("JWT AUTHENTICATOR: No subject found in JWT claims");
        return Optional.empty();
      }

      // Get user from user store
      Optional<User> user = userStore.getUser(username);
      if (user.isPresent()) {
        LOG.info(
            "JWT AUTHENTICATOR: User {} found with roles: {}", username, user.get().getRoles());
      } else {
        LOG.info("JWT AUTHENTICATOR: User {} not found in user store", username);
      }

      return user;
    } catch (JwtException e) {
      LOG.warn("Invalid JWT token: {}", e.getMessage());
      return Optional.empty();
    }
  }
}
