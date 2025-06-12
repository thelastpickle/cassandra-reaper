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

import io.cassandrareaper.ReaperApplicationConfiguration;

import java.security.Key;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/")
public class AuthLoginResource {
  private static final Logger LOG = LoggerFactory.getLogger(AuthLoginResource.class);

  private final UserStore userStore;
  private final Key jwtKey;
  private final Duration tokenExpirationTime;

  public AuthLoginResource(UserStore userStore, String jwtSecret) {
    this(userStore, jwtSecret, null, null);
  }

  public AuthLoginResource(
      UserStore userStore,
      String jwtSecret,
      ReaperApplicationConfiguration.JwtConfiguration jwtConfig,
      Duration sessionTimeout) {
    this.userStore = userStore;
    this.jwtKey = Keys.hmacShaKeyFor(jwtSecret.getBytes());

    // Use sessionTimeout as the single source of truth for JWT token expiration
    this.tokenExpirationTime = sessionTimeout != null ? sessionTimeout : Duration.ofMinutes(10);

    LOG.info("JWT token expiration time set to: {}", this.tokenExpirationTime);
  }

  @Path("/login")
  @POST
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @Produces(MediaType.APPLICATION_JSON)
  public LoginResponse login(
      @FormParam("username") String username,
      @FormParam("password") String password,
      @FormParam("rememberMe") boolean rememberMe) {

    if (StringUtils.isBlank(username)) {
      throw new WebApplicationException(
          "Invalid credentials: missing username.", Response.Status.BAD_REQUEST);
    }
    if (StringUtils.isBlank(password)) {
      throw new WebApplicationException(
          "Invalid credentials: missing password.", Response.Status.BAD_REQUEST);
    }

    if (!userStore.authenticate(username, password)) {
      LOG.warn("Authentication failed for user: {}", username);
      throw new WebApplicationException("Invalid credentials", Response.Status.UNAUTHORIZED);
    }

    User user = userStore.findUser(username);
    if (user == null) {
      throw new WebApplicationException("User not found", Response.Status.UNAUTHORIZED);
    }

    // Generate JWT token using configured expiration time
    Instant now = Instant.now();
    Instant expiry;

    if (rememberMe) {
      // For "remember me", use 30 days as a reasonable default
      expiry = now.plus(30, ChronoUnit.DAYS);
    } else {
      // Use the configured token expiration time
      expiry = now.plus(tokenExpirationTime.toMillis(), ChronoUnit.MILLIS);
    }

    String token =
        Jwts.builder()
            .setSubject(username)
            .claim("roles", user.getRoles())
            .setIssuedAt(Date.from(now))
            .setExpiration(Date.from(expiry))
            .signWith(jwtKey)
            .compact();

    LOG.info("User {} logged in successfully with token expiry: {}", username, expiry);
    return new LoginResponse(token, user.getName(), user.getRoles());
  }

  public static class LoginResponse {
    @JsonProperty private final String token;

    @JsonProperty private final String username;

    @JsonProperty private final java.util.Set<String> roles;

    public LoginResponse(String token, String username, java.util.Set<String> roles) {
      this.token = token;
      this.username = username;
      this.roles = roles;
    }

    public String getToken() {
      return token;
    }

    public String getUsername() {
      return username;
    }

    public java.util.Set<String> getRoles() {
      return roles;
    }
  }
}
