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
  public Optional<User> authenticate(String token) {
    try {
      Jws<Claims> jws =
          Jwts.parser().verifyWith((SecretKey) jwtKey).build().parseSignedClaims(token);

      Claims claims = jws.getPayload();
      String username = claims.getSubject();

      if (username == null) {
        LOG.warn("JWT token missing subject claim");
        return Optional.empty();
      }

      User user = userStore.findUser(username);
      if (user == null) {
        LOG.warn("User {} not found in user store", username);
        return Optional.empty();
      }

      return Optional.of(user);
    } catch (JwtException e) {
      LOG.warn("Invalid JWT token: {}", e.getMessage());
      return Optional.empty();
    }
  }
}
