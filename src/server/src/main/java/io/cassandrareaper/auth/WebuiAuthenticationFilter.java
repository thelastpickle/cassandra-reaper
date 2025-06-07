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

import java.io.IOException;
import java.security.Key;

import javax.crypto.SecretKey;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Servlet filter that protects WebUI paths by checking for valid JWT tokens. Redirects
 * unauthenticated users to the login page.
 */
public class WebuiAuthenticationFilter implements Filter {
  private static final Logger LOG = LoggerFactory.getLogger(WebuiAuthenticationFilter.class);

  private final Key jwtKey;
  private final UserStore userStore;

  public WebuiAuthenticationFilter(String jwtSecret, UserStore userStore) {
    this.jwtKey = Keys.hmacShaKeyFor(jwtSecret.getBytes());
    this.userStore = userStore;
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    // No initialization needed
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {

    HttpServletRequest httpRequest = (HttpServletRequest) request;
    HttpServletResponse httpResponse = (HttpServletResponse) response;

    String requestURI = httpRequest.getRequestURI();

    // Allow access to login page and static assets
    if (requestURI.endsWith("/login.html")
        || requestURI.contains("/assets/")
        || requestURI.endsWith(".css")
        || requestURI.endsWith(".js")
        || requestURI.endsWith(".png")
        || requestURI.endsWith(".jpg")
        || requestURI.endsWith(".gif")
        || requestURI.endsWith(".ico")
        || requestURI.endsWith(".woff")
        || requestURI.endsWith(".woff2")
        || requestURI.endsWith(".ttf")) {
      chain.doFilter(request, response);
      return;
    }

    // Check for JWT token in session storage (via Authorization header) or cookies
    String token = extractToken(httpRequest);

    if (token != null && isValidToken(token)) {
      // Valid token, allow request to proceed
      chain.doFilter(request, response);
    } else {
      // No valid token, redirect to login page
      String contextPath = httpRequest.getContextPath();
      String loginUrl = contextPath + "/webui/login.html";
      LOG.debug("Redirecting unauthenticated request {} to login page", requestURI);
      httpResponse.sendRedirect(loginUrl);
    }
  }

  private String extractToken(HttpServletRequest request) {
    // First try Authorization header (for AJAX requests)
    String authHeader = request.getHeader("Authorization");
    if (authHeader != null && authHeader.startsWith("Bearer ")) {
      return authHeader.substring(7);
    }

    // For HTML page requests, we need to check if the browser has the token
    // Since we can't access sessionStorage from server-side, we'll rely on
    // the frontend to include the token in a cookie for HTML requests
    jakarta.servlet.http.Cookie[] cookies = request.getCookies();
    if (cookies != null) {
      for (jakarta.servlet.http.Cookie cookie : cookies) {
        if ("jwtToken".equals(cookie.getName())) {
          return cookie.getValue();
        }
      }
    }

    return null;
  }

  private boolean isValidToken(String token) {
    try {
      Jws<Claims> jws =
          Jwts.parser().verifyWith((SecretKey) jwtKey).build().parseSignedClaims(token);

      Claims claims = jws.getPayload();
      String username = claims.getSubject();

      if (username == null) {
        return false;
      }

      // Verify user still exists in user store
      User user = userStore.findUser(username);
      return user != null;

    } catch (JwtException e) {
      LOG.debug("Invalid JWT token: {}", e.getMessage());
      return false;
    }
  }

  @Override
  public void destroy() {
    // No cleanup needed
  }
}
