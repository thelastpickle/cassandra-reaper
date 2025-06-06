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

import java.util.Optional;

import io.dropwizard.auth.Authenticator;
import io.dropwizard.auth.basic.BasicCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicAuthenticator implements Authenticator<BasicCredentials, User> {
  private static final Logger LOG = LoggerFactory.getLogger(BasicAuthenticator.class);

  private final UserStore userStore;

  public BasicAuthenticator(UserStore userStore) {
    this.userStore = userStore;
  }

  @Override
  public Optional<User> authenticate(BasicCredentials credentials) {
    String username = credentials.getUsername();
    String password = credentials.getPassword();

    if (userStore.authenticate(username, password)) {
      User user = userStore.findUser(username);
      if (user != null) {
        LOG.debug("Authenticated user: {}", username);
        return Optional.of(user);
      }
    }

    LOG.warn("Authentication failed for user: {}", username);
    return Optional.empty();
  }
}
