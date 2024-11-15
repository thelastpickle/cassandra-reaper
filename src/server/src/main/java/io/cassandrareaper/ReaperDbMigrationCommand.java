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

package io.cassandrareaper;

import io.cassandrareaper.storage.InitializeStorage;

import javax.validation.Validation;

import io.dropwizard.core.cli.ConfiguredCommand;
import io.dropwizard.core.setup.Bootstrap;
import io.dropwizard.core.setup.Environment;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ReaperDbMigrationCommand extends ConfiguredCommand<ReaperApplicationConfiguration> {
  private static final Logger LOG = LoggerFactory.getLogger(ReaperDbMigrationCommand.class);

  protected ReaperDbMigrationCommand(
      String name,
      String description
  ) {
    super(name, description);
  }

  @Override
  protected void run(
      Bootstrap<ReaperApplicationConfiguration> bootstrap,
      Namespace namespace,
      ReaperApplicationConfiguration configuration
  ) throws Exception {
    final Environment environment = new Environment(bootstrap.getApplication().getName(),
                                                        bootstrap.getObjectMapper(),
                                                        Validation.buildDefaultValidatorFactory(),
                                                        bootstrap.getMetricRegistry(),
                                                        bootstrap.getClassLoader(),
                                                        bootstrap.getHealthCheckRegistry(),
                                                        configuration);
    InitializeStorage.initializeStorage(configuration, environment).initializeStorageBackend();
    System.exit(0);
  }
}
