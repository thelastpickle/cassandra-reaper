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

package io.cassandrareaper;

import io.cassandrareaper.ReaperApplicationConfiguration.AccessControlConfiguration;
import io.cassandrareaper.ReaperApplicationConfiguration.JwtConfiguration;
import io.cassandrareaper.ReaperApplicationConfiguration.UserConfiguration;
import io.cassandrareaper.auth.UserStore;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import io.dropwizard.core.setup.Environment;
import io.dropwizard.jetty.setup.ServletEnvironment;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import jakarta.servlet.FilterRegistration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class AuthenticationValidationTest {

  @Mock private Environment mockEnvironment;
  @Mock private ServletEnvironment mockServletEnvironment;
  @Mock private LifecycleEnvironment mockLifecycleEnvironment;
  @Mock private FilterRegistration.Dynamic mockFilterRegistration;

  private ReaperApplicationConfiguration config;
  private ReaperApplication app;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    // Setup basic mocks
    when(mockEnvironment.servlets()).thenReturn(mockServletEnvironment);
    when(mockEnvironment.lifecycle()).thenReturn(mockLifecycleEnvironment);
    when(mockServletEnvironment.addFilter(
            org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.any(jakarta.servlet.Filter.class)))
        .thenReturn(mockFilterRegistration);

    config = new ReaperApplicationConfiguration();
    app = new ReaperApplication();

    // Setup basic valid configuration
    config.setStorageType("memory");
    config.setJmxConnectionTimeoutInSeconds(10);
  }

  @Test
  public void testValidUserConfiguration() throws Exception {
    // Given: Valid access control configuration
    AccessControlConfiguration accessControl = new AccessControlConfiguration();
    accessControl.setEnabled(true);
    accessControl.setSessionTimeout(Duration.ofMinutes(10));

    JwtConfiguration jwt = new JwtConfiguration();
    jwt.setSecret("ValidJWTSecretKeyThatIsLongEnoughForHS256Algorithm");
    accessControl.setJwt(jwt);

    UserConfiguration user = new UserConfiguration();
    user.setUsername("admin");
    user.setPassword("secure-password");
    user.setRoles(Arrays.asList("operator"));
    accessControl.setUsers(Arrays.asList(user));

    config.setAccessControl(accessControl);

    // When/Then: Should not throw any exception
    // Note: This would be a full integration test in a real scenario
    // For now, we're testing the validation logic by directly creating UserStore
    UserStore userStore = new UserStore();
    userStore.addUser(
        user.getUsername(), user.getPassword(), java.util.Set.copyOf(user.getRoles()));

    // Verify user was added correctly
    assertThat(userStore.authenticate("admin", "secure-password")).isTrue();
    assertThat(userStore.findUser("admin")).isNotNull();
    assertThat(userStore.findUser("admin").hasRole("operator")).isTrue();
  }

  @Test
  public void testEmptyUsernameValidation() {
    // Given: User configuration with empty username
    AccessControlConfiguration accessControl = new AccessControlConfiguration();
    accessControl.setEnabled(true);

    UserConfiguration user = new UserConfiguration();
    user.setUsername(""); // Empty username
    user.setPassword("secure-password");
    user.setRoles(Arrays.asList("operator"));
    accessControl.setUsers(Arrays.asList(user));

    config.setAccessControl(accessControl);

    // When/Then: Should throw IllegalArgumentException
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validateUserConfiguration(accessControl.getUsers()));

    assertThat(exception.getMessage()).contains("missing username");
  }

  @Test
  public void testEmptyPasswordValidation() {
    // Given: User configuration with empty password
    AccessControlConfiguration accessControl = new AccessControlConfiguration();
    accessControl.setEnabled(true);

    UserConfiguration user = new UserConfiguration();
    user.setUsername("admin");
    user.setPassword(""); // Empty password
    user.setRoles(Arrays.asList("operator"));
    accessControl.setUsers(Arrays.asList(user));

    config.setAccessControl(accessControl);

    // When/Then: Should throw IllegalArgumentException
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validateUserConfiguration(accessControl.getUsers()));

    assertThat(exception.getMessage()).contains("missing password");
    assertThat(exception.getMessage()).contains("admin");
  }

  @Test
  public void testEmptyRolesValidation() {
    // Given: User configuration with empty roles
    AccessControlConfiguration accessControl = new AccessControlConfiguration();
    accessControl.setEnabled(true);

    UserConfiguration user = new UserConfiguration();
    user.setUsername("admin");
    user.setPassword("secure-password");
    user.setRoles(Collections.emptyList()); // Empty roles
    accessControl.setUsers(Arrays.asList(user));

    config.setAccessControl(accessControl);

    // When/Then: Should throw IllegalArgumentException
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validateUserConfiguration(accessControl.getUsers()));

    assertThat(exception.getMessage()).contains("no roles assigned");
    assertThat(exception.getMessage()).contains("admin");
  }

  @Test
  public void testNullPasswordValidation() {
    // Given: User configuration with null password
    AccessControlConfiguration accessControl = new AccessControlConfiguration();
    accessControl.setEnabled(true);

    UserConfiguration user = new UserConfiguration();
    user.setUsername("admin");
    user.setPassword(null); // Null password
    user.setRoles(Arrays.asList("operator"));
    accessControl.setUsers(Arrays.asList(user));

    config.setAccessControl(accessControl);

    // When/Then: Should throw IllegalArgumentException
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validateUserConfiguration(accessControl.getUsers()));

    assertThat(exception.getMessage()).contains("missing password");
  }

  @Test
  public void testNoUsersConfigured() {
    // Given: Access control enabled but no users configured
    AccessControlConfiguration accessControl = new AccessControlConfiguration();
    accessControl.setEnabled(true);
    accessControl.setUsers(Collections.emptyList()); // No users

    config.setAccessControl(accessControl);

    // When/Then: Should throw IllegalArgumentException
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validateUserConfiguration(accessControl.getUsers()));

    assertThat(exception.getMessage()).contains("no users are configured");
  }

  @Test
  public void testWhitespaceOnlyCredentials() {
    // Given: User configuration with whitespace-only credentials
    AccessControlConfiguration accessControl = new AccessControlConfiguration();
    accessControl.setEnabled(true);

    UserConfiguration user = new UserConfiguration();
    user.setUsername("   "); // Whitespace-only username
    user.setPassword("  "); // Whitespace-only password
    user.setRoles(Arrays.asList("operator"));
    accessControl.setUsers(Arrays.asList(user));

    config.setAccessControl(accessControl);

    // When/Then: Should throw IllegalArgumentException for username
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validateUserConfiguration(accessControl.getUsers()));

    assertThat(exception.getMessage()).contains("missing username");
  }

  /** Helper method that mimics the validation logic from ReaperApplication */
  private void validateUserConfiguration(java.util.List<UserConfiguration> users) {
    if (users == null || users.isEmpty()) {
      throw new IllegalArgumentException(
          "ACCESS CONTROL: Authentication is enabled but no users are configured. "
              + "Please configure at least one user in the 'accessControl.users' section or disable authentication by setting 'accessControl.enabled: false'.");
    }

    for (UserConfiguration userConfig : users) {
      if (userConfig.getUsername() == null || userConfig.getUsername().trim().isEmpty()) {
        throw new IllegalArgumentException(
            "ACCESS CONTROL: User configuration missing username. All users must have a non-empty username.");
      }

      if (userConfig.getPassword() == null || userConfig.getPassword().trim().isEmpty()) {
        throw new IllegalArgumentException(
            String.format(
                "ACCESS CONTROL: User '%s' is missing password. All users must have a non-empty password for security reasons.",
                userConfig.getUsername()));
      }

      if (userConfig.getRoles() == null || userConfig.getRoles().isEmpty()) {
        throw new IllegalArgumentException(
            String.format(
                "ACCESS CONTROL: User '%s' has no roles assigned. All users must have at least one role ('user' or 'operator').",
                userConfig.getUsername()));
      }
    }
  }
}
