/*
 * Copyright 2018-2018 The Last Pickle Ltd
 *
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

package io.cassandrareaper.resources;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration.DatacenterAvailability;
import io.cassandrareaper.service.TestRepairConfiguration;
import io.cassandrareaper.storage.MemoryStorageFacade;

import javax.ws.rs.core.Response;

import com.google.common.collect.BiMap;
import junit.framework.TestCase;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.Test;

public class ReaperResourceTest extends TestCase {

  @Test
  public void testGetDatacenterAvailability() throws Exception {
    final MockObjects mocks = initMocks();

    ReaperResource resource = new ReaperResource(mocks.context);
    Response response = resource.getDatacenterAvailability();
    BiMap<String, DatacenterAvailability> config =
        (BiMap<String, DatacenterAvailability>) response.getEntity();

    assertEquals(config.get("datacenterAvailability"), DatacenterAvailability.EACH);
    assertEquals(HttpStatus.OK_200, response.getStatus());
  }

  private MockObjects initMocks() {
    AppContext context = new AppContext();
    context.storage = new MemoryStorageFacade();
    context.config = TestRepairConfiguration.defaultConfig();
    context.config.setDatacenterAvailability(DatacenterAvailability.EACH);

    return new MockObjects(context);
  }

  private static final class MockObjects {

    final AppContext context;

    MockObjects(AppContext context) {
      super();
      this.context = context;
    }
  }
}
