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

package io.cassandrareaper.resources.auth;

import io.cassandrareaper.auth.AuthLoginResource;
import io.cassandrareaper.auth.User;
import io.cassandrareaper.auth.UserStore;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

public final class LoginResourceTest {

  private UserStore userStore;
  private AuthLoginResource loginResource;
  private static final String JWT_SECRET =
      "MySecretKeyForJWTWhichMustBeLongEnoughForHS256Algorithm";

  @Before
  public void setUp() {
    userStore = new UserStore();
    // Add test users explicitly since we removed default users for security
    userStore.addUser("admin", "admin", java.util.Set.of("operator"));
    userStore.addUser("user", "user", java.util.Set.of("user"));
    loginResource = new AuthLoginResource(userStore, JWT_SECRET);
  }

  @Test
  public void testUserStoreAddedUsers() {
    // Test that explicitly added users are available
    User adminUser = userStore.findUser("admin");
    assertThat(adminUser).isNotNull();
    assertThat(adminUser.getName()).isEqualTo("admin");
    assertThat(adminUser.hasRole("operator")).isTrue();

    User regularUser = userStore.findUser("user");
    assertThat(regularUser).isNotNull();
    assertThat(regularUser.getName()).isEqualTo("user");
    assertThat(regularUser.hasRole("user")).isTrue();
  }

  @Test
  public void testAuthentication() {
    // Test admin authentication
    assertThat(userStore.authenticate("admin", "admin")).isTrue();
    assertThat(userStore.authenticate("admin", "wrong")).isFalse();

    // Test user authentication
    assertThat(userStore.authenticate("user", "user")).isTrue();
    assertThat(userStore.authenticate("user", "wrong")).isFalse();

    // Test non-existent user
    assertThat(userStore.authenticate("nonexistent", "password")).isFalse();
  }

  @Test
  public void testLogin() {
    // Test successful login
    AuthLoginResource.LoginResponse response = loginResource.login("admin", "admin", false);
    assertThat(response).isNotNull();
    assertThat(response.getToken()).isNotNull();
    assertThat(response.getUsername()).isEqualTo("admin");
    assertThat(response.getRoles()).contains("operator");
  }

  @Test(expected = jakarta.ws.rs.WebApplicationException.class)
  public void testLoginFailure() {
    // Test failed login
    loginResource.login("admin", "wrong_password", false);
  }
}
