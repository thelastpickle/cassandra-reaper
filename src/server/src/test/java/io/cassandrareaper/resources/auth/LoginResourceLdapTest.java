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

import io.cassandrareaper.auth.UserStore;

import java.util.Arrays;
import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

public final class LoginResourceLdapTest {

  private UserStore userStore;

  @Before
  public void setUp() {
    userStore = new UserStore();
  }

  @Test
  public void testUserStoreCustomUsers() {
    // Test adding custom users (similar to what might be done with LDAP integration)
    userStore.addUser("ldapuser", "ldappass", new HashSet<>(Arrays.asList("user")));

    // Verify user was added
    assertThat(userStore.authenticate("ldapuser", "ldappass")).isTrue();
    assertThat(userStore.authenticate("ldapuser", "wrongpass")).isFalse();

    // Verify user details
    var user = userStore.findUser("ldapuser");
    assertThat(user).isNotNull();
    assertThat(user.getName()).isEqualTo("ldapuser");
    assertThat(user.hasRole("user")).isTrue();
    assertThat(user.hasRole("operator")).isFalse();
  }

  @Test
  public void testUserStoreWithOperatorRole() {
    // Test adding an operator user
    userStore.addUser("ldapadmin", "adminpass", new HashSet<>(Arrays.asList("operator")));

    // Verify operator user
    assertThat(userStore.authenticate("ldapadmin", "adminpass")).isTrue();

    var user = userStore.findUser("ldapadmin");
    assertThat(user).isNotNull();
    assertThat(user.hasRole("operator")).isTrue();
  }
}
