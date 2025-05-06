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

import javax.validation.Validation;
import javax.validation.Validator;

import brave.Tracing;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.datastax.oss.driver.api.core.CqlSession;
import io.dropwizard.cassandra.CassandraFactory;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import org.apache.cassandra.repair.RepairParallelism;
import org.hibernate.validator.HibernateValidator;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public final class ReaperApplicationConfigurationTest {

  private final Validator validator = Validation
      .byProvider(HibernateValidator.class)
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
    //create a valid config
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
}
