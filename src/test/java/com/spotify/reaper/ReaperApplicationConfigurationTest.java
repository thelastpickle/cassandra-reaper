package com.spotify.reaper;

import org.apache.cassandra.repair.RepairParallelism;
import org.hibernate.validator.HibernateValidator;
import org.junit.Before;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;

import io.dropwizard.db.DataSourceFactory;

import static org.fest.assertions.api.Assertions.assertThat;

public class ReaperApplicationConfigurationTest {

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
    config.setDataSourceFactory(dataSourceFactory);
    config.setHangingRepairTimeoutMins(1);
    config.setRepairParallelism(RepairParallelism.DATACENTER_AWARE);
    config.setRepairRunThreadCount(1);
    config.setSegmentCount(1);
    config.setScheduleDaysBetween(7);
    config.setStorageType("foo");
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