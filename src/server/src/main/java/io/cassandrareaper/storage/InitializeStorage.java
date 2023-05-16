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
import io.cassandrareaper.storage.cassandra.CassandraStorage;

import java.util.UUID;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.dropwizard.setup.Environment;
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
    } else {
      LOG.error("invalid storageType: {}", config.getStorageType());
      throw new ReaperException("invalid storage type: " + config.getStorageType());
    }
    Preconditions.checkState(storage.isStorageConnected(), "Failed to connect storage");
    return storage;
  }
}
