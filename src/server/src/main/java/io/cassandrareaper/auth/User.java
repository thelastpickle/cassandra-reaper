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

import java.security.Principal;
import java.util.Set;

public class User implements Principal {
  private final String name;
  private final Set<String> roles;

  public User(String name, Set<String> roles) {
    this.name = name;
    this.roles = roles;
  }

  @Override
  public String getName() {
    return name;
  }

  public Set<String> getRoles() {
    return roles;
  }

  public boolean hasRole(String role) {
    return roles.contains(role);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    User user = (User) o;
    return name.equals(user.name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public String toString() {
    return "User{name='" + name + "', roles=" + roles + '}';
  }
}
