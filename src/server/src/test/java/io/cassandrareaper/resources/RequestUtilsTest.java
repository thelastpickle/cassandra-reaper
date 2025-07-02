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

package io.cassandrareaper.resources;

import java.lang.reflect.Constructor;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.servlet.ServletRequest;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.HttpMethod;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for RequestUtils class. Tests all static utility methods for handling CORS settings,
 * session timeout configuration, and request type detection.
 */
public class RequestUtilsTest {

  @BeforeEach
  void setUp() {
    // Reset static state before each test
    RequestUtils.setCorsEnabled(false);
    RequestUtils.setSessionTimeout(Duration.ofMinutes(-1));
  }

  @Test
  void testPrivateConstructor_ShouldBePrivate() throws NoSuchMethodException {
    // Given: Private constructor
    Constructor<RequestUtils> constructor = RequestUtils.class.getDeclaredConstructor();

    // Then: Constructor should be private
    assertThat(constructor.getModifiers()).isEqualTo(java.lang.reflect.Modifier.PRIVATE);

    // And: We can instantiate it via reflection (though discouraged)
    constructor.setAccessible(true);
    try {
      RequestUtils instance = constructor.newInstance();
      assertThat(instance).isNotNull();
    } catch (Exception e) {
      // If it throws, that's also fine
    }
  }

  @Test
  void testSetCorsEnabled_WithTrue_ShouldEnableCors() {
    // Given: CORS is initially disabled
    assertThat(RequestUtils.isCorsEnabled()).isFalse();

    // When: setCorsEnabled is called with true
    RequestUtils.setCorsEnabled(true);

    // Then: CORS should be enabled
    assertThat(RequestUtils.isCorsEnabled()).isTrue();
  }

  @Test
  void testSetCorsEnabled_WithFalse_ShouldDisableCors() {
    // Given: CORS is enabled
    RequestUtils.setCorsEnabled(true);
    assertThat(RequestUtils.isCorsEnabled()).isTrue();

    // When: setCorsEnabled is called with false
    RequestUtils.setCorsEnabled(false);

    // Then: CORS should be disabled
    assertThat(RequestUtils.isCorsEnabled()).isFalse();
  }

  @Test
  void testIsCorsEnabled_DefaultValue_ShouldBeFalse() {
    // Given: No CORS setting has been applied

    // When: isCorsEnabled is called
    boolean corsEnabled = RequestUtils.isCorsEnabled();

    // Then: Should return false by default
    assertThat(corsEnabled).isFalse();
  }

  @Test
  void testSetSessionTimeout_WithValidDuration_ShouldSetTimeout() {
    // Given: A valid duration
    Duration timeout = Duration.ofMinutes(30);

    // When: setSessionTimeout is called
    RequestUtils.setSessionTimeout(timeout);

    // Then: Session timeout should be set
    assertThat(RequestUtils.getSessionTimeout()).isEqualTo(timeout);
  }

  @Test
  void testSetSessionTimeout_WithZeroDuration_ShouldSetZeroTimeout() {
    // Given: Zero duration
    Duration timeout = Duration.ZERO;

    // When: setSessionTimeout is called
    RequestUtils.setSessionTimeout(timeout);

    // Then: Session timeout should be zero
    assertThat(RequestUtils.getSessionTimeout()).isEqualTo(Duration.ZERO);
  }

  @Test
  void testSetSessionTimeout_WithNegativeDuration_ShouldSetNegativeTimeout() {
    // Given: Negative duration
    Duration timeout = Duration.ofMinutes(-10);

    // When: setSessionTimeout is called
    RequestUtils.setSessionTimeout(timeout);

    // Then: Session timeout should be negative
    assertThat(RequestUtils.getSessionTimeout()).isEqualTo(timeout);
  }

  @Test
  void testGetSessionTimeout_DefaultValue_ShouldBeNegativeOneMinute() {
    // Given: No session timeout has been set

    // When: getSessionTimeout is called
    Duration timeout = RequestUtils.getSessionTimeout();

    // Then: Should return -1 minute by default
    assertThat(timeout).isEqualTo(Duration.ofMinutes(-1));
  }

  @Test
  void testIsOptionsRequest_WithOptionsHttpRequest_ShouldReturnTrue() {
    // Given: An OPTIONS HTTP request
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    when(mockRequest.getMethod()).thenReturn(HttpMethod.OPTIONS);

    // When: isOptionsRequest is called
    boolean isOptions = RequestUtils.isOptionsRequest(mockRequest);

    // Then: Should return true
    assertThat(isOptions).isTrue();
  }

  @Test
  void testIsOptionsRequest_WithOptionsHttpRequestLowercase_ShouldReturnTrue() {
    // Given: An OPTIONS HTTP request with lowercase method
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    when(mockRequest.getMethod()).thenReturn("options");

    // When: isOptionsRequest is called
    boolean isOptions = RequestUtils.isOptionsRequest(mockRequest);

    // Then: Should return true (case insensitive)
    assertThat(isOptions).isTrue();
  }

  @Test
  void testIsOptionsRequest_WithGetHttpRequest_ShouldReturnFalse() {
    // Given: A GET HTTP request
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    when(mockRequest.getMethod()).thenReturn(HttpMethod.GET);

    // When: isOptionsRequest is called
    boolean isOptions = RequestUtils.isOptionsRequest(mockRequest);

    // Then: Should return false
    assertThat(isOptions).isFalse();
  }

  @Test
  void testIsOptionsRequest_WithPostHttpRequest_ShouldReturnFalse() {
    // Given: A POST HTTP request
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    when(mockRequest.getMethod()).thenReturn(HttpMethod.POST);

    // When: isOptionsRequest is called
    boolean isOptions = RequestUtils.isOptionsRequest(mockRequest);

    // Then: Should return false
    assertThat(isOptions).isFalse();
  }

  @Test
  void testIsOptionsRequest_WithNonHttpServletRequest_ShouldReturnFalse() {
    // Given: A non-HTTP servlet request
    ServletRequest mockRequest = mock(ServletRequest.class);

    // When: isOptionsRequest is called
    boolean isOptions = RequestUtils.isOptionsRequest(mockRequest);

    // Then: Should return false
    assertThat(isOptions).isFalse();
  }

  @Test
  void testIsOptionsRequest_WithNullRequest_ShouldReturnFalse() {
    // Given: Null request

    // When: isOptionsRequest is called
    boolean isOptions = RequestUtils.isOptionsRequest(null);

    // Then: Should return false
    assertThat(isOptions).isFalse();
  }

  @Test
  void testCorsEnabledToggle_MultipleChanges_ShouldMaintainState() {
    // Given: Initial state
    assertThat(RequestUtils.isCorsEnabled()).isFalse();

    // When: Multiple toggles
    RequestUtils.setCorsEnabled(true);
    assertThat(RequestUtils.isCorsEnabled()).isTrue();

    RequestUtils.setCorsEnabled(false);
    assertThat(RequestUtils.isCorsEnabled()).isFalse();

    RequestUtils.setCorsEnabled(true);
    assertThat(RequestUtils.isCorsEnabled()).isTrue();

    RequestUtils.setCorsEnabled(true);
    assertThat(RequestUtils.isCorsEnabled()).isTrue();
  }

  @Test
  void testSessionTimeout_MultipleChanges_ShouldMaintainState() {
    // Given: Initial state
    assertThat(RequestUtils.getSessionTimeout()).isEqualTo(Duration.ofMinutes(-1));

    // When: Multiple changes
    RequestUtils.setSessionTimeout(Duration.ofMinutes(10));
    assertThat(RequestUtils.getSessionTimeout()).isEqualTo(Duration.ofMinutes(10));

    RequestUtils.setSessionTimeout(Duration.ofHours(1));
    assertThat(RequestUtils.getSessionTimeout()).isEqualTo(Duration.ofHours(1));

    RequestUtils.setSessionTimeout(Duration.ofSeconds(30));
    assertThat(RequestUtils.getSessionTimeout()).isEqualTo(Duration.ofSeconds(30));
  }
}
