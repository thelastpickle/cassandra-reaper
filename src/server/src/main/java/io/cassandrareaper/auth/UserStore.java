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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class UserStore {
  private final Map<String, UserCredentials> users = new HashMap<>();

  public UserStore() {
    // Default hardcoded users (matching current shiro.ini)
    addUser("admin", "admin", new HashSet<>(Arrays.asList("operator")));
    addUser("user", "user", new HashSet<>(Arrays.asList("user")));
  }

  public void addUser(String username, String password, Set<String> roles) {
    users.put(username, new UserCredentials(username, password, roles));
  }

  public boolean authenticate(String username, String password) {
    UserCredentials creds = users.get(username);
    return creds != null && creds.password.equals(password);
  }

  public User findUser(String username) {
    UserCredentials creds = users.get(username);
    return creds != null ? new User(creds.username, creds.roles) : null;
  }

  public Optional<User> getUser(String username) {
    User user = findUser(username);
    return Optional.ofNullable(user);
  }

  private static class UserCredentials {
    final String username;
    final String password;
    final Set<String> roles;

    UserCredentials(String username, String password, Set<String> roles) {
      this.username = username;
      this.password = password;
      this.roles = roles;
    }
  }
}
