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
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import brave.Tracing;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.datastax.oss.driver.api.core.CqlSession;
import io.dropwizard.cassandra.CassandraFactory;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.apache.cassandra.repair.RepairParallelism;
import org.hibernate.validator.HibernateValidator;
import org.junit.Before;
import org.junit.Test;

public final class ReaperApplicationConfigurationTest {

  private final Validator validator =
      Validation.byProvider(HibernateValidator.class)
          .configure()
          .buildValidatorFactory()
          .getValidator();

  private final ReaperApplicationConfiguration config = new ReaperApplicationConfiguration();

  private final class TestCassandraFactory extends CassandraFactory {
    @Override
    public CqlSession build(
        MetricRegistry metricRegistry,
        LifecycleEnvironment lifecycleEnvironment,
        HealthCheckRegistry healthCheckRegistry,
        Tracing tracing) {
      return mock(CqlSession.class);
    }
  }

  @Before
  public void setUp() {
    // create a valid config
    CassandraFactory cassandraFactory = new TestCassandraFactory();
    config.setCassandraFactory(cassandraFactory);
    config.setHangingRepairTimeoutMins(1);
    config.setRepairParallelism(RepairParallelism.DATACENTER_AWARE);
    config.setRepairRunThreadCount(1);
    config.setSegmentCount(1);
    config.setScheduleDaysBetween(7);
    config.setStorageType("foo");
    config.setIncrementalRepair(false);
    config.setSubrangeIncrementalRepair(false);
    config.setBlacklistTwcsTables(true);
    config.setRepairIntensity(1.0);
  }

  @Test
  public void testRepairIntensity() {
    config.setRepairIntensity(-0.1);
    assertThat(validator.validate(config)).hasSize(1);

    config.setRepairIntensity(0);
    assertThat(validator.validate(config)).hasSize(1);

    config.setRepairIntensity(1);
    assertThat(validator.validate(config)).hasSize(0);
  }

  @Test
  public void testAccessControlConfiguration() {
    // Test null access control (should work)
    config.setAccessControl(null);
    assertThat(config.getAccessControl()).isNull();

    // Test valid access control configuration
    ReaperApplicationConfiguration.AccessControlConfiguration accessControl =
        new ReaperApplicationConfiguration.AccessControlConfiguration();

    accessControl.setSessionTimeout(Duration.ofMinutes(15));

    ReaperApplicationConfiguration.JwtConfiguration jwt =
        new ReaperApplicationConfiguration.JwtConfiguration();
    jwt.setSecret("test-secret-key-for-jwt-that-is-long-enough");
    jwt.setTokenExpirationTime(Duration.ofMinutes(30));
    accessControl.setJwt(jwt);

    ReaperApplicationConfiguration.UserConfiguration user1 =
        new ReaperApplicationConfiguration.UserConfiguration();
    user1.setUsername("admin");
    user1.setPassword("admin123");
    user1.setRoles(Arrays.asList("operator"));

    ReaperApplicationConfiguration.UserConfiguration user2 =
        new ReaperApplicationConfiguration.UserConfiguration();
    user2.setUsername("user");
    user2.setPassword("user123");
    user2.setRoles(Arrays.asList("user"));

    accessControl.setUsers(Arrays.asList(user1, user2));

    config.setAccessControl(accessControl);

    // Verify configuration
    assertThat(config.getAccessControl()).isNotNull();
    assertThat(config.getAccessControl().getSessionTimeout()).isEqualTo(Duration.ofMinutes(15));
    assertThat(config.getAccessControl().getJwt()).isNotNull();
    assertThat(config.getAccessControl().getJwt().getSecret())
        .isEqualTo("test-secret-key-for-jwt-that-is-long-enough");
    assertThat(config.getAccessControl().getJwt().getTokenExpirationTime())
        .isEqualTo(Duration.ofMinutes(30));
    assertThat(config.getAccessControl().getUsers()).hasSize(2);
    assertThat(config.getAccessControl().getUsers().get(0).getUsername()).isEqualTo("admin");
    assertThat(config.getAccessControl().getUsers().get(0).getRoles()).containsExactly("operator");
    assertThat(config.getAccessControl().getUsers().get(1).getUsername()).isEqualTo("user");
    assertThat(config.getAccessControl().getUsers().get(1).getRoles()).containsExactly("user");
  }

  @Test
  public void testAccessControlConfigurationDefaults() {
    ReaperApplicationConfiguration.AccessControlConfiguration accessControl =
        new ReaperApplicationConfiguration.AccessControlConfiguration();

    // Test default values
    assertThat(accessControl.getSessionTimeout()).isEqualTo(Duration.ofMinutes(10));
    assertThat(accessControl.getJwt()).isNull();
    assertThat(accessControl.getUsers()).isEmpty();
  }

  @Test
  public void testJwtConfiguration() {
    ReaperApplicationConfiguration.JwtConfiguration jwt =
        new ReaperApplicationConfiguration.JwtConfiguration();

    // Test initial null values
    assertThat(jwt.getSecret()).isNull();
    assertThat(jwt.getTokenExpirationTime()).isNull();

    // Test setting values
    jwt.setSecret("my-jwt-secret");
    jwt.setTokenExpirationTime(Duration.ofHours(1));

    assertThat(jwt.getSecret()).isEqualTo("my-jwt-secret");
    assertThat(jwt.getTokenExpirationTime()).isEqualTo(Duration.ofHours(1));
  }

  @Test
  public void testUserConfiguration() {
    ReaperApplicationConfiguration.UserConfiguration user =
        new ReaperApplicationConfiguration.UserConfiguration();

    // Test initial values
    assertThat(user.getUsername()).isNull();
    assertThat(user.getPassword()).isNull();
    assertThat(user.getRoles()).isEmpty();

    // Test setting values
    user.setUsername("testuser");
    user.setPassword("testpass");
    user.setRoles(Arrays.asList("admin", "user"));

    assertThat(user.getUsername()).isEqualTo("testuser");
    assertThat(user.getPassword()).isEqualTo("testpass");
    assertThat(user.getRoles()).containsExactly("admin", "user");

    // Test setting null roles
    user.setRoles(null);
    assertThat(user.getRoles()).isEmpty();

    // Test setting empty roles
    user.setRoles(Collections.emptyList());
    assertThat(user.getRoles()).isEmpty();
  }

  @Test
  public void testAccessControlConfigurationNullHandling() {
    ReaperApplicationConfiguration.AccessControlConfiguration accessControl =
        new ReaperApplicationConfiguration.AccessControlConfiguration();

    // Test null session timeout
    accessControl.setSessionTimeout(null);
    assertThat(accessControl.getSessionTimeout()).isEqualTo(Duration.ofMinutes(10));

    // Test null users list
    accessControl.setUsers(null);
    assertThat(accessControl.getUsers()).isEmpty();
  }

  @Test
  public void testCompleteAccessControlConfigurationValidation() {
    // Create a complete valid access control configuration
    ReaperApplicationConfiguration.AccessControlConfiguration accessControl =
        new ReaperApplicationConfiguration.AccessControlConfiguration();

    accessControl.setSessionTimeout(Duration.ofMinutes(5));

    ReaperApplicationConfiguration.JwtConfiguration jwt =
        new ReaperApplicationConfiguration.JwtConfiguration();
    jwt.setSecret("valid-jwt-secret-key");
    jwt.setTokenExpirationTime(Duration.ofMinutes(60));
    accessControl.setJwt(jwt);

    ReaperApplicationConfiguration.UserConfiguration adminUser =
        new ReaperApplicationConfiguration.UserConfiguration();
    adminUser.setUsername("admin");
    adminUser.setPassword("strongPassword123");
    adminUser.setRoles(Arrays.asList("operator"));

    accessControl.setUsers(Arrays.asList(adminUser));

    config.setAccessControl(accessControl);

    // The configuration should be valid
    assertThat(validator.validate(config)).hasSize(0);
  }
}
