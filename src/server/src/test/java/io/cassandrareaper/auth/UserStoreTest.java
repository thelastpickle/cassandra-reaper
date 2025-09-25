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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.cassandrareaper.auth;

import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

public class UserStoreTest {

  private UserStore userStore;

  @Before
  public void setUp() {
    userStore = new UserStore();
  }

  @Test
  public void testAddUser_singleRole() {
    // Given
    String username = "testuser";
    String password = "testpass";
    Set<String> roles = Set.of("user");

    // When
    userStore.addUser(username, password, roles);

    // Then
    User user = userStore.findUser(username);
    assertThat(user).isNotNull();
    assertThat(user.getName()).isEqualTo(username);
    assertThat(user.getRoles()).containsExactly("user");
  }

  @Test
  public void testAddUser_multipleRoles() {
    // Given
    String username = "admin";
    String password = "adminpass";
    Set<String> roles = Set.of("user", "admin", "operator");

    // When
    userStore.addUser(username, password, roles);

    // Then
    User user = userStore.findUser(username);
    assertThat(user).isNotNull();
    assertThat(user.getName()).isEqualTo(username);
    assertThat(user.getRoles()).containsExactlyInAnyOrderElementsOf(roles);
  }

  @Test
  public void testAddUser_emptyRoles() {
    // Given
    String username = "noroles";
    String password = "password";
    Set<String> roles = Set.of();

    // When
    userStore.addUser(username, password, roles);

    // Then
    User user = userStore.findUser(username);
    assertThat(user).isNotNull();
    assertThat(user.getName()).isEqualTo(username);
    assertThat(user.getRoles()).isEmpty();
  }

  @Test
  public void testAuthenticate_validCredentials() {
    // Given
    String username = "testuser";
    String password = "testpass";
    userStore.addUser(username, password, Set.of("user"));

    // When
    boolean authenticated = userStore.authenticate(username, password);

    // Then
    assertThat(authenticated).isTrue();
  }

  @Test
  public void testAuthenticate_invalidPassword() {
    // Given
    String username = "testuser";
    String password = "testpass";
    userStore.addUser(username, password, Set.of("user"));

    // When
    boolean authenticated = userStore.authenticate(username, "wrongpass");

    // Then
    assertThat(authenticated).isFalse();
  }

  @Test
  public void testAuthenticate_nonExistentUser() {
    // Given/When
    boolean authenticated = userStore.authenticate("nonexistent", "password");

    // Then
    assertThat(authenticated).isFalse();
  }

  @Test
  public void testFindUser_existingUser() {
    // Given
    String username = "testuser";
    String password = "testpass";
    Set<String> roles = Set.of("user", "admin");
    userStore.addUser(username, password, roles);

    // When
    User user = userStore.findUser(username);

    // Then
    assertThat(user).isNotNull();
    assertThat(user.getName()).isEqualTo(username);
    assertThat(user.getRoles()).containsExactlyInAnyOrderElementsOf(roles);
  }

  @Test
  public void testFindUser_nonExistentUser() {
    // Given/When
    User user = userStore.findUser("nonexistent");

    // Then
    assertThat(user).isNull();
  }

  @Test
  public void testGetUser_existingUser() {
    // Given
    String username = "testuser";
    String password = "testpass";
    Set<String> roles = Set.of("operator");
    userStore.addUser(username, password, roles);

    // When
    Optional<User> userOptional = userStore.getUser(username);

    // Then
    assertThat(userOptional).isPresent();
    assertThat(userOptional.get().getName()).isEqualTo(username);
    assertThat(userOptional.get().getRoles()).containsExactly("operator");
  }

  @Test
  public void testGetUser_nonExistentUser() {
    // Given/When
    Optional<User> userOptional = userStore.getUser("nonexistent");

    // Then
    assertThat(userOptional).isEmpty();
  }

  @Test
  public void testOverwriteUser() {
    // Given
    String username = "testuser";
    userStore.addUser(username, "oldpass", Set.of("user"));

    // When - add same user with different password and roles
    userStore.addUser(username, "newpass", Set.of("admin", "operator"));

    // Then
    assertThat(userStore.authenticate(username, "oldpass")).isFalse();
    assertThat(userStore.authenticate(username, "newpass")).isTrue();

    User user = userStore.findUser(username);
    assertThat(user.getRoles()).containsExactlyInAnyOrder("admin", "operator");
  }

  @Test
  public void testMultipleUsers() {
    // Given
    userStore.addUser("user1", "pass1", Set.of("user"));
    userStore.addUser("user2", "pass2", Set.of("admin"));
    userStore.addUser("user3", "pass3", Set.of("operator", "user"));

    // When/Then
    assertThat(userStore.authenticate("user1", "pass1")).isTrue();
    assertThat(userStore.authenticate("user2", "pass2")).isTrue();
    assertThat(userStore.authenticate("user3", "pass3")).isTrue();

    assertThat(userStore.authenticate("user1", "pass2")).isFalse();
    assertThat(userStore.authenticate("user2", "pass1")).isFalse();

    assertThat(userStore.findUser("user1").getRoles()).containsExactly("user");
    assertThat(userStore.findUser("user2").getRoles()).containsExactly("admin");
    assertThat(userStore.findUser("user3").getRoles())
        .containsExactlyInAnyOrder("operator", "user");
  }

  @Test
  public void testEmptyUserStore() {
    // Given - empty user store

    // When/Then
    assertThat(userStore.authenticate("anyone", "anything")).isFalse();
    assertThat(userStore.findUser("anyone")).isNull();
    assertThat(userStore.getUser("anyone")).isEmpty();
  }
}
