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
 *  stributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.cassandrareaper.auth;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class WebuiAuthenticationFilterTest {

  private static final String JWT_SECRET = "test-secret-key-that-is-long-enough-for-hmac-256";
  private static final String TEST_USERNAME = "testuser";

  @Mock private UserStore userStore;
  @Mock private HttpServletRequest request;
  @Mock private HttpServletResponse response;
  @Mock private FilterChain filterChain;
  @Mock private FilterConfig filterConfig;

  private WebuiAuthenticationFilter filter;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    filter = new WebuiAuthenticationFilter(JWT_SECRET, userStore);
  }

  @Test
  public void testInit() throws ServletException {
    // Given/When
    filter.init(filterConfig);

    // Then - no exception should be thrown
    assertThat(filter).isNotNull();
  }

  @Test
  public void testDestroy() {
    // Given/When
    filter.destroy();

    // Then - no exception should be thrown
    assertThat(filter).isNotNull();
  }

  @Test
  public void testDoFilter_allowsLoginPage() throws IOException, ServletException {
    // Given
    when(request.getRequestURI()).thenReturn("/webui/login.html");

    // When
    filter.doFilter(request, response, filterChain);

    // Then
    verify(filterChain).doFilter(request, response);
    verify(response, never()).sendRedirect(anyString());
  }

  @Test
  public void testDoFilter_allowsStaticAssets() throws IOException, ServletException {
    // Given
    String testPath = "/webui/assets/style.css";
    when(request.getRequestURI()).thenReturn(testPath);

    // When
    filter.doFilter(request, response, filterChain);

    // Then
    verify(filterChain).doFilter(request, response);
    verify(response, never()).sendRedirect(anyString());
  }

  @Test
  public void testDoFilter_allowsJavaScript() throws IOException, ServletException {
    // Given
    when(request.getRequestURI()).thenReturn("/webui/script.js");

    // When
    filter.doFilter(request, response, filterChain);

    // Then
    verify(filterChain).doFilter(request, response);
    verify(response, never()).sendRedirect(anyString());
  }

  @Test
  public void testDoFilter_allowsImages() throws IOException, ServletException {
    // Given
    when(request.getRequestURI()).thenReturn("/webui/logo.png");

    // When
    filter.doFilter(request, response, filterChain);

    // Then
    verify(filterChain).doFilter(request, response);
    verify(response, never()).sendRedirect(anyString());
  }

  @Test
  public void testDoFilter_allowsFonts() throws IOException, ServletException {
    // Given
    when(request.getRequestURI()).thenReturn("/webui/font.woff");

    // When
    filter.doFilter(request, response, filterChain);

    // Then
    verify(filterChain).doFilter(request, response);
    verify(response, never()).sendRedirect(anyString());
  }

  @Test
  public void testDoFilter_redirectsWhenNoToken() throws IOException, ServletException {
    // Given
    when(request.getRequestURI()).thenReturn("/webui/dashboard.html");
    when(request.getContextPath()).thenReturn("/reaper");
    when(request.getHeader("Authorization")).thenReturn(null);
    when(request.getCookies()).thenReturn(null);

    // When
    filter.doFilter(request, response, filterChain);

    // Then
    verify(response).sendRedirect("/reaper/webui/login.html");
    verify(filterChain, never()).doFilter(any(), any());
  }

  @Test
  public void testDoFilter_allowsValidBearerToken() throws IOException, ServletException {
    // Given
    String validToken = createValidToken();
    when(request.getRequestURI()).thenReturn("/webui/dashboard.html");
    when(request.getHeader("Authorization")).thenReturn("Bearer " + validToken);
    when(userStore.findUser(TEST_USERNAME)).thenReturn(new User(TEST_USERNAME, Set.of("user")));

    // When
    filter.doFilter(request, response, filterChain);

    // Then
    verify(filterChain).doFilter(request, response);
    verify(response, never()).sendRedirect(anyString());
  }

  @Test
  public void testDoFilter_allowsValidCookieToken() throws IOException, ServletException {
    // Given
    String validToken = createValidToken();
    Cookie jwtCookie = new Cookie("jwtToken", validToken);
    Cookie[] cookies = {jwtCookie};

    when(request.getRequestURI()).thenReturn("/webui/dashboard.html");
    when(request.getHeader("Authorization")).thenReturn(null);
    when(request.getCookies()).thenReturn(cookies);
    when(userStore.findUser(TEST_USERNAME)).thenReturn(new User(TEST_USERNAME, Set.of("user")));

    // When
    filter.doFilter(request, response, filterChain);

    // Then
    verify(filterChain).doFilter(request, response);
    verify(response, never()).sendRedirect(anyString());
  }

  @Test
  public void testDoFilter_redirectsWhenUserNotFound() throws IOException, ServletException {
    // Given
    String validToken = createValidToken();
    when(request.getRequestURI()).thenReturn("/webui/dashboard.html");
    when(request.getContextPath()).thenReturn("");
    when(request.getHeader("Authorization")).thenReturn("Bearer " + validToken);
    when(userStore.findUser(TEST_USERNAME)).thenReturn(null);

    // When
    filter.doFilter(request, response, filterChain);

    // Then
    verify(response).sendRedirect("/webui/login.html");
    verify(filterChain, never()).doFilter(any(), any());
  }

  @Test
  public void testDoFilter_redirectsWhenTokenExpired() throws IOException, ServletException {
    // Given
    String expiredToken = createExpiredToken();
    when(request.getRequestURI()).thenReturn("/webui/dashboard.html");
    when(request.getContextPath()).thenReturn("");
    when(request.getHeader("Authorization")).thenReturn("Bearer " + expiredToken);

    // When
    filter.doFilter(request, response, filterChain);

    // Then
    verify(response).sendRedirect("/webui/login.html");
    verify(filterChain, never()).doFilter(any(), any());
  }

  @Test
  public void testDoFilter_redirectsWhenTokenMalformed() throws IOException, ServletException {
    // Given
    String malformedToken = "invalid.token.here";
    when(request.getRequestURI()).thenReturn("/webui/dashboard.html");
    when(request.getContextPath()).thenReturn("");
    when(request.getHeader("Authorization")).thenReturn("Bearer " + malformedToken);

    // When
    filter.doFilter(request, response, filterChain);

    // Then
    verify(response).sendRedirect("/webui/login.html");
    verify(filterChain, never()).doFilter(any(), any());
  }

  @Test
  public void testDoFilter_handlesEmptyContextPath() throws IOException, ServletException {
    // Given
    when(request.getRequestURI()).thenReturn("/webui/dashboard.html");
    when(request.getContextPath()).thenReturn("");
    when(request.getHeader("Authorization")).thenReturn(null);
    when(request.getCookies()).thenReturn(null);

    // When
    filter.doFilter(request, response, filterChain);

    // Then
    verify(response).sendRedirect("/webui/login.html");
  }

  @Test
  public void testDoFilter_handlesMultipleCookies() throws IOException, ServletException {
    // Given
    String validToken = createValidToken();
    Cookie otherCookie = new Cookie("other", "value");
    Cookie jwtCookie = new Cookie("jwtToken", validToken);
    Cookie[] cookies = {otherCookie, jwtCookie};

    when(request.getRequestURI()).thenReturn("/webui/dashboard.html");
    when(request.getHeader("Authorization")).thenReturn(null);
    when(request.getCookies()).thenReturn(cookies);
    when(userStore.findUser(TEST_USERNAME)).thenReturn(new User(TEST_USERNAME, Set.of("user")));

    // When
    filter.doFilter(request, response, filterChain);

    // Then
    verify(filterChain).doFilter(request, response);
    verify(response, never()).sendRedirect(anyString());
  }

  @Test
  public void testDoFilter_prefersAuthorizationHeaderOverCookie()
      throws IOException, ServletException {
    // Given
    String validToken = createValidToken();
    String invalidToken = "invalid.token";
    Cookie jwtCookie = new Cookie("jwtToken", invalidToken);
    Cookie[] cookies = {jwtCookie};

    when(request.getRequestURI()).thenReturn("/webui/dashboard.html");
    when(request.getHeader("Authorization")).thenReturn("Bearer " + validToken);
    when(request.getCookies()).thenReturn(cookies);
    when(userStore.findUser(TEST_USERNAME)).thenReturn(new User(TEST_USERNAME, Set.of("user")));

    // When
    filter.doFilter(request, response, filterChain);

    // Then
    verify(filterChain).doFilter(request, response);
    verify(response, never()).sendRedirect(anyString());
  }

  private String createValidToken() {
    return Jwts.builder()
        .setSubject(TEST_USERNAME)
        .setIssuedAt(new Date())
        .setExpiration(Date.from(Instant.now().plus(1, ChronoUnit.HOURS)))
        .signWith(Keys.hmacShaKeyFor(JWT_SECRET.getBytes()))
        .compact();
  }

  private String createExpiredToken() {
    return Jwts.builder()
        .setSubject(TEST_USERNAME)
        .setIssuedAt(Date.from(Instant.now().minus(2, ChronoUnit.HOURS)))
        .setExpiration(Date.from(Instant.now().minus(1, ChronoUnit.HOURS)))
        .signWith(Keys.hmacShaKeyFor(JWT_SECRET.getBytes()))
        .compact();
  }
}
