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
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static org.mockito.Mockito.*;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class WebuiAuthenticationFilterTest {

  private static final String JWT_SECRET =
      "MySecretKeyForJWTWhichMustBeLongEnoughForHS256Algorithm";

  @Mock private HttpServletRequest mockRequest;
  @Mock private HttpServletResponse mockResponse;
  @Mock private FilterChain mockFilterChain;

  private WebuiAuthenticationFilter filter;
  private UserStore userStore;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    userStore = new UserStore();
    Set<String> roles = new HashSet<>();
    roles.add("operator");
    userStore.addUser("testuser", "testpass", roles);

    filter = new WebuiAuthenticationFilter(JWT_SECRET, userStore);
  }

  @Test
  public void testAllowsLoginPage() throws IOException, ServletException {
    // Given
    when(mockRequest.getRequestURI()).thenReturn("/webui/login.html");

    // When
    filter.doFilter(mockRequest, mockResponse, mockFilterChain);

    // Then
    verify(mockFilterChain).doFilter(mockRequest, mockResponse);
    verify(mockResponse, never()).sendRedirect(anyString());
  }

  @Test
  public void testAllowsStaticAssets() throws IOException, ServletException {
    // Given
    when(mockRequest.getRequestURI()).thenReturn("/webui/assets/style.css");

    // When
    filter.doFilter(mockRequest, mockResponse, mockFilterChain);

    // Then
    verify(mockFilterChain).doFilter(mockRequest, mockResponse);
    verify(mockResponse, never()).sendRedirect(anyString());
  }

  @Test
  public void testRedirectsUnauthenticatedRequest() throws IOException, ServletException {
    // Given
    when(mockRequest.getRequestURI()).thenReturn("/webui/index.html");
    when(mockRequest.getContextPath()).thenReturn("");
    when(mockRequest.getHeader("Authorization")).thenReturn(null);
    when(mockRequest.getCookies()).thenReturn(null);

    // When
    filter.doFilter(mockRequest, mockResponse, mockFilterChain);

    // Then
    verify(mockResponse).sendRedirect("/webui/login.html");
    verify(mockFilterChain, never()).doFilter(mockRequest, mockResponse);
  }

  @Test
  public void testAllowsValidJwtInHeader() throws IOException, ServletException {
    // Given
    String validToken = createValidJwtToken("testuser");
    when(mockRequest.getRequestURI()).thenReturn("/webui/index.html");
    when(mockRequest.getHeader("Authorization")).thenReturn("Bearer " + validToken);

    // When
    filter.doFilter(mockRequest, mockResponse, mockFilterChain);

    // Then
    verify(mockFilterChain).doFilter(mockRequest, mockResponse);
    verify(mockResponse, never()).sendRedirect(anyString());
  }

  @Test
  public void testAllowsValidJwtInCookie() throws IOException, ServletException {
    // Given
    String validToken = createValidJwtToken("testuser");
    Cookie jwtCookie = new Cookie("jwtToken", validToken);
    when(mockRequest.getRequestURI()).thenReturn("/webui/index.html");
    when(mockRequest.getHeader("Authorization")).thenReturn(null);
    when(mockRequest.getCookies()).thenReturn(new Cookie[] {jwtCookie});

    // When
    filter.doFilter(mockRequest, mockResponse, mockFilterChain);

    // Then
    verify(mockFilterChain).doFilter(mockRequest, mockResponse);
    verify(mockResponse, never()).sendRedirect(anyString());
  }

  @Test
  public void testRejectsInvalidJwtToken() throws IOException, ServletException {
    // Given
    when(mockRequest.getRequestURI()).thenReturn("/webui/index.html");
    when(mockRequest.getContextPath()).thenReturn("");
    when(mockRequest.getHeader("Authorization")).thenReturn("Bearer invalid_token");

    // When
    filter.doFilter(mockRequest, mockResponse, mockFilterChain);

    // Then
    verify(mockResponse).sendRedirect("/webui/login.html");
    verify(mockFilterChain, never()).doFilter(mockRequest, mockResponse);
  }

  @Test
  public void testRejectsExpiredJwtToken() throws IOException, ServletException {
    // Given
    String expiredToken = createExpiredJwtToken("testuser");
    when(mockRequest.getRequestURI()).thenReturn("/webui/index.html");
    when(mockRequest.getContextPath()).thenReturn("");
    when(mockRequest.getHeader("Authorization")).thenReturn("Bearer " + expiredToken);

    // When
    filter.doFilter(mockRequest, mockResponse, mockFilterChain);

    // Then
    verify(mockResponse).sendRedirect("/webui/login.html");
    verify(mockFilterChain, never()).doFilter(mockRequest, mockResponse);
  }

  private String createValidJwtToken(String username) {
    Instant now = Instant.now();
    Instant expiry = now.plus(1, ChronoUnit.HOURS);

    return Jwts.builder()
        .setSubject(username)
        .setIssuedAt(Date.from(now))
        .setExpiration(Date.from(expiry))
        .signWith(Keys.hmacShaKeyFor(JWT_SECRET.getBytes()))
        .compact();
  }

  private String createExpiredJwtToken(String username) {
    Instant now = Instant.now();
    Instant expiry = now.minus(1, ChronoUnit.HOURS); // Expired 1 hour ago

    return Jwts.builder()
        .setSubject(username)
        .setIssuedAt(Date.from(now.minus(2, ChronoUnit.HOURS)))
        .setExpiration(Date.from(expiry))
        .signWith(Keys.hmacShaKeyFor(JWT_SECRET.getBytes()))
        .compact();
  }
}
