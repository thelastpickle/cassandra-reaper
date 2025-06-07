/*
 * Copyright 2015-2017 Spotify AB
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

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.Before;
import org.junit.Test;

public final class ReaperApplicationConfigurationYamlTest {

  private ObjectMapper objectMapper;

  @Before
  public void setUp() {
    objectMapper = new ObjectMapper(new YAMLFactory());
    objectMapper.registerModule(new JavaTimeModule());
  }

  @Test
  public void testAccessControlConfigurationFromYaml() throws Exception {
    String yamlConfig =
        "accessControl:\n"
            + "  sessionTimeout: PT15M\n"
            + "  jwt:\n"
            + "    secret: 'test-jwt-secret-key'\n"
            + "    tokenExpirationTime: PT30M\n"
            + "  users:\n"
            + "    - username: 'admin'\n"
            + "      password: 'admin123'\n"
            + "      roles: ['operator']\n"
            + "    - username: 'user'\n"
            + "      password: 'user123'\n"
            + "      roles: ['user']";

    ReaperApplicationConfiguration config =
        objectMapper.readValue(yamlConfig, ReaperApplicationConfiguration.class);

    assertThat(config.getAccessControl()).isNotNull();
    assertThat(config.getAccessControl().getSessionTimeout()).isEqualTo(Duration.ofMinutes(15));

    assertThat(config.getAccessControl().getJwt()).isNotNull();
    assertThat(config.getAccessControl().getJwt().getSecret()).isEqualTo("test-jwt-secret-key");
    assertThat(config.getAccessControl().getJwt().getTokenExpirationTime())
        .isEqualTo(Duration.ofMinutes(30));

    assertThat(config.getAccessControl().getUsers()).hasSize(2);

    assertThat(config.getAccessControl().getUsers().get(0).getUsername()).isEqualTo("admin");
    assertThat(config.getAccessControl().getUsers().get(0).getPassword()).isEqualTo("admin123");
    assertThat(config.getAccessControl().getUsers().get(0).getRoles()).containsExactly("operator");

    assertThat(config.getAccessControl().getUsers().get(1).getUsername()).isEqualTo("user");
    assertThat(config.getAccessControl().getUsers().get(1).getPassword()).isEqualTo("user123");
    assertThat(config.getAccessControl().getUsers().get(1).getRoles()).containsExactly("user");
  }

  @Test
  public void testMinimalAccessControlConfigurationFromYaml() throws Exception {
    String yamlConfig =
        "accessControl:\n"
            + "  jwt:\n"
            + "    secret: 'minimal-secret'\n"
            + "  users:\n"
            + "    - username: 'admin'\n"
            + "      password: 'pass'\n"
            + "      roles: ['operator']";

    ReaperApplicationConfiguration config =
        objectMapper.readValue(yamlConfig, ReaperApplicationConfiguration.class);

    assertThat(config.getAccessControl()).isNotNull();
    assertThat(config.getAccessControl().getSessionTimeout())
        .isEqualTo(Duration.ofMinutes(10)); // default

    assertThat(config.getAccessControl().getJwt()).isNotNull();
    assertThat(config.getAccessControl().getJwt().getSecret()).isEqualTo("minimal-secret");
    assertThat(config.getAccessControl().getJwt().getTokenExpirationTime()).isNull();

    assertThat(config.getAccessControl().getUsers()).hasSize(1);
    assertThat(config.getAccessControl().getUsers().get(0).getUsername()).isEqualTo("admin");
  }

  @Test
  public void testAccessControlConfigurationWithEnvironmentVariables() throws Exception {
    String yamlConfig =
        "accessControl:\n"
            + "  jwt:\n"
            + "    secret: '${JWT_SECRET:-default-secret}'\n"
            + "    tokenExpirationTime: PT10M\n"
            + "  users:\n"
            + "    - username: '${REAPER_ADMIN_USER:-admin}'\n"
            + "      password: '${REAPER_ADMIN_PASSWORD:-admin}'\n"
            + "      roles: ['operator']\n"
            + "    - username: '${REAPER_USER:-user}'\n"
            + "      password: '${REAPER_USER_PASSWORD:-user}'\n"
            + "      roles: ['user']";

    ReaperApplicationConfiguration config =
        objectMapper.readValue(yamlConfig, ReaperApplicationConfiguration.class);

    assertThat(config.getAccessControl()).isNotNull();
    assertThat(config.getAccessControl().getJwt()).isNotNull();
    assertThat(config.getAccessControl().getJwt().getSecret())
        .isEqualTo("${JWT_SECRET:-default-secret}");
    assertThat(config.getAccessControl().getJwt().getTokenExpirationTime())
        .isEqualTo(Duration.ofMinutes(10));
    assertThat(config.getAccessControl().getUsers()).hasSize(2);
    assertThat(config.getAccessControl().getUsers().get(0).getUsername())
        .isEqualTo("${REAPER_ADMIN_USER:-admin}");
  }

  @Test
  public void testAccessControlConfigurationAfterEnvironmentSubstitution() throws Exception {
    // This test simulates what the configuration would look like after
    // Dropwizard's EnvironmentVariableSubstitutor has processed it
    String yamlConfig =
        "accessControl:\n"
            + "  jwt:\n"
            + "    secret: 'production-jwt-secret-key'\n"
            + "    tokenExpirationTime: PT1H\n"
            + "  users:\n"
            + "    - username: 'prodadmin'\n"
            + "      password: 'secure-admin-password'\n"
            + "      roles: ['operator']\n"
            + "    - username: 'produser'\n"
            + "      password: 'secure-user-password'\n"
            + "      roles: ['user']";

    ReaperApplicationConfiguration config =
        objectMapper.readValue(yamlConfig, ReaperApplicationConfiguration.class);

    assertThat(config.getAccessControl()).isNotNull();
    assertThat(config.getAccessControl().getJwt()).isNotNull();
    assertThat(config.getAccessControl().getJwt().getSecret())
        .isEqualTo("production-jwt-secret-key");
    assertThat(config.getAccessControl().getJwt().getTokenExpirationTime())
        .isEqualTo(Duration.ofHours(1));
    assertThat(config.getAccessControl().getUsers()).hasSize(2);
    assertThat(config.getAccessControl().getUsers().get(0).getUsername()).isEqualTo("prodadmin");
    assertThat(config.getAccessControl().getUsers().get(0).getPassword())
        .isEqualTo("secure-admin-password");
  }

  @Test
  public void testEmptyAccessControlConfiguration() throws Exception {
    String yamlConfig = "accessControl: {}";

    ReaperApplicationConfiguration config =
        objectMapper.readValue(yamlConfig, ReaperApplicationConfiguration.class);

    assertThat(config.getAccessControl()).isNotNull();
    assertThat(config.getAccessControl().getSessionTimeout())
        .isEqualTo(Duration.ofMinutes(10)); // default
    assertThat(config.getAccessControl().getJwt()).isNull();
    assertThat(config.getAccessControl().getUsers()).isEmpty();
  }

  @Test
  public void testNoAccessControlConfiguration() throws Exception {
    String yamlConfig = "storageType: 'memory'";

    ReaperApplicationConfiguration config =
        objectMapper.readValue(yamlConfig, ReaperApplicationConfiguration.class);

    assertThat(config.getAccessControl()).isNull();
  }

  @Test
  public void testJwtOnlyConfiguration() throws Exception {
    String yamlConfig =
        "accessControl:\n"
            + "  jwt:\n"
            + "    secret: 'jwt-only-secret'\n"
            + "    tokenExpirationTime: PT1H";

    ReaperApplicationConfiguration config =
        objectMapper.readValue(yamlConfig, ReaperApplicationConfiguration.class);

    assertThat(config.getAccessControl()).isNotNull();
    assertThat(config.getAccessControl().getJwt()).isNotNull();
    assertThat(config.getAccessControl().getJwt().getSecret()).isEqualTo("jwt-only-secret");
    assertThat(config.getAccessControl().getJwt().getTokenExpirationTime())
        .isEqualTo(Duration.ofHours(1));
    assertThat(config.getAccessControl().getUsers()).isEmpty();
  }

  @Test
  public void testUsersOnlyConfiguration() throws Exception {
    String yamlConfig =
        "accessControl:\n"
            + "  users:\n"
            + "    - username: 'testuser'\n"
            + "      password: 'testpass'\n"
            + "      roles: ['user', 'operator']";

    ReaperApplicationConfiguration config =
        objectMapper.readValue(yamlConfig, ReaperApplicationConfiguration.class);

    assertThat(config.getAccessControl()).isNotNull();
    assertThat(config.getAccessControl().getJwt()).isNull();
    assertThat(config.getAccessControl().getUsers()).hasSize(1);
    assertThat(config.getAccessControl().getUsers().get(0).getUsername()).isEqualTo("testuser");
    assertThat(config.getAccessControl().getUsers().get(0).getPassword()).isEqualTo("testpass");
    assertThat(config.getAccessControl().getUsers().get(0).getRoles())
        .containsExactly("user", "operator");
  }

  @Test
  public void testUserWithEmptyRoles() throws Exception {
    String yamlConfig =
        "accessControl:\n"
            + "  users:\n"
            + "    - username: 'noroleuser'\n"
            + "      password: 'password'\n"
            + "      roles: []";

    ReaperApplicationConfiguration config =
        objectMapper.readValue(yamlConfig, ReaperApplicationConfiguration.class);

    assertThat(config.getAccessControl()).isNotNull();
    assertThat(config.getAccessControl().getUsers()).hasSize(1);
    assertThat(config.getAccessControl().getUsers().get(0).getUsername()).isEqualTo("noroleuser");
    assertThat(config.getAccessControl().getUsers().get(0).getRoles()).isEmpty();
  }
}
