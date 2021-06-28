/*
 * Copyright 2021-2021 DataStax, Inc.
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

package io.cassandrareaper.storage;

import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.ReaperException;

import java.util.UUID;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.jdbi.DBIFactory;
import io.dropwizard.setup.Environment;
import org.apache.commons.lang3.StringUtils;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class InitializeStorage {

  private static final Logger LOG = LoggerFactory.getLogger(InitializeStorage.class);
  private final ReaperApplicationConfiguration config;
  private final Environment environment;
  private final UUID reaperInstanceId;

  private InitializeStorage(ReaperApplicationConfiguration config, Environment environment, UUID reaperInstanceId) {
    this.config = config;
    this.environment = environment;
    this.reaperInstanceId = reaperInstanceId;
  }

  public static InitializeStorage initializeStorage(ReaperApplicationConfiguration config, Environment environment) {
    return new InitializeStorage(config, environment, UUID.randomUUID());
  }

  public static InitializeStorage initializeStorage(
      ReaperApplicationConfiguration config,
      Environment environment,
      UUID reaperInstanceId) {
    return new InitializeStorage(config, environment, reaperInstanceId);
  }

  public IStorage initializeStorageBackend()
    throws ReaperException {
    IStorage storage;
    LOG.info("Initializing the database and performing schema migrations");

    if ("memory".equalsIgnoreCase(config.getStorageType())) {
      storage = new MemoryStorage();
    } else if (Lists.newArrayList("cassandra", "astra").contains(config.getStorageType())) {
      CassandraStorage.CassandraMode mode = config.getStorageType().equals("cassandra")
          ? CassandraStorage.CassandraMode.CASSANDRA
          : CassandraStorage.CassandraMode.ASTRA;
      storage = new CassandraStorage(reaperInstanceId, config, environment, mode);
    } else if ("postgres".equalsIgnoreCase(config.getStorageType())
        || "h2".equalsIgnoreCase(config.getStorageType())
        || "database".equalsIgnoreCase(config.getStorageType())) {
      // create DBI instance
      final DBIFactory factory = new DBIFactory();
      if (StringUtils.isEmpty(config.getDataSourceFactory().getDriverClass())
          && "postgres".equalsIgnoreCase(config.getStorageType())) {
        config.getDataSourceFactory().setDriverClass("org.postgresql.Driver");
      } else if (StringUtils.isEmpty(config.getDataSourceFactory().getDriverClass())
          && "h2".equalsIgnoreCase(config.getStorageType())) {
        config.getDataSourceFactory().setDriverClass("org.h2.Driver");
      }
      // instantiate store
      storage = new PostgresStorage(
          reaperInstanceId,
          factory.build(environment, config.getDataSourceFactory(), "postgresql")
      );
      initDatabase(config);
    } else {
      LOG.error("invalid storageType: {}", config.getStorageType());
      throw new ReaperException("invalid storage type: " + config.getStorageType());
    }
    Preconditions.checkState(storage.isStorageConnected(), "Failed to connect storage");
    return storage;
  }

  private void initDatabase(ReaperApplicationConfiguration config) throws ReaperException {
    Flyway flyway = new Flyway();
    DataSourceFactory dsfactory = config.getDataSourceFactory();
    flyway.setDataSource(
        dsfactory.getUrl(),
        dsfactory.getUser(),
        dsfactory.getPassword());

    if ("database".equals(config.getStorageType())) {
      LOG.warn("!!!!!!!!!!    USAGE 'database' AS STORAGE TYPE IS NOW DEPRECATED   !!!!!!!!!!!!!!");
      LOG.warn("!!!!!!!!!!    PLEASE USE EITHER 'postgres' OR 'h2' FROM NOW ON     !!!!!!!!!!!!!!");
      if (config.getDataSourceFactory().getUrl().contains("h2")) {
        flyway.setLocations("/db/h2");
      } else {
        flyway.setLocations("/db/postgres");
      }
    } else {
      flyway.setLocations("/db/".concat(config.getStorageType().toLowerCase()));
    }
    flyway.setBaselineOnMigrate(true);
    flyway.repair();
    flyway.migrate();
  }
}
