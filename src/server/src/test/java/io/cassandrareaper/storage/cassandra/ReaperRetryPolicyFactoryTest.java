/*
 * Copyright 2016-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
 * Copyright 2020-2020 DataStax, Inc.
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

package io.cassandrareaper.storage.cassandra;

import static org.mockito.Mockito.verify;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import io.dropwizard.cassandra.DropwizardProgrammaticDriverConfigLoaderBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public final class ReaperRetryPolicyFactoryTest {

  @Mock private DropwizardProgrammaticDriverConfigLoaderBuilder mockBuilder;

  private ReaperRetryPolicyFactory retryPolicyFactory;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    retryPolicyFactory = new ReaperRetryPolicyFactory();
  }

  @Test
  public void testAcceptConfiguresRetryPolicyClass() {
    // Execute
    retryPolicyFactory.accept(mockBuilder);

    // Verify - Should configure the builder with RetryPolicyImpl class
    verify(mockBuilder).withClass(DefaultDriverOption.RETRY_POLICY_CLASS, RetryPolicyImpl.class);
  }

  @Test
  public void testFactoryCanBeInstantiated() {
    // Test that factory can be created without issues
    ReaperRetryPolicyFactory factory = new ReaperRetryPolicyFactory();
    // Should complete without error
  }
}
