/*
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

package com.spotify.reaper;


import javax.validation.Validation;
import javax.validation.Validator;

import io.dropwizard.db.DataSourceFactory;
import org.apache.cassandra.repair.RepairParallelism;
import org.hibernate.validator.HibernateValidator;
import org.junit.Before;
import org.junit.Test;
import systems.composable.dropwizard.cassandra.CassandraFactory;

import static org.fest.assertions.api.Assertions.assertThat;

public final class ReaperApplicationConfigurationTest {

  private final Validator validator = Validation
      .byProvider(HibernateValidator.class)
      .configure()
      .buildValidatorFactory()
      .getValidator();

  private final ReaperApplicationConfiguration config = new ReaperApplicationConfiguration();

  @Before
  public void setUp() {
    //create a valid config
    DataSourceFactory dataSourceFactory = new DataSourceFactory();
    dataSourceFactory.setDriverClass("org.postgresql.Driver");
    dataSourceFactory.setUrl("jdbc:postgresql://db.example.com/db-prod");
    dataSourceFactory.setUser("user");
    CassandraFactory cassandraFactory = new CassandraFactory();
    cassandraFactory.setContactPoints(new String[]{"127.0.0.1"});
    config.setCassandraFactory(cassandraFactory);
    config.setDataSourceFactory(dataSourceFactory);
    config.setHangingRepairTimeoutMins(1);
    config.setRepairParallelism(RepairParallelism.DATACENTER_AWARE);
    config.setRepairRunThreadCount(1);
    config.setSegmentCount(1);
    config.setScheduleDaysBetween(7);
    config.setStorageType("foo");
    config.setIncrementalRepair(false);
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
