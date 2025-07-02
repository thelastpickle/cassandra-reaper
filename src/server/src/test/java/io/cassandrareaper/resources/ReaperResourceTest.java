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
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.ReaperApplicationConfiguration.DatacenterAvailability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.BiMap;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for ReaperResource class. Tests the REST endpoint for retrieving Reaper configuration
 * parameters.
 */
public class ReaperResourceTest {

  private AppContext mockAppContext;
  private ReaperApplicationConfiguration mockConfig;
  private ReaperResource reaperResource;

  @BeforeEach
  void setUp() {
    mockAppContext = new AppContext();
    mockConfig = mock(ReaperApplicationConfiguration.class);
    mockAppContext.config = mockConfig;
    reaperResource = new ReaperResource(mockAppContext);
  }

  @Test
  void testGetDatacenterAvailability_WithAllDatacenters_ShouldReturnAll() {
    // Given: Configuration set to ALL
    when(mockConfig.getDatacenterAvailability()).thenReturn(DatacenterAvailability.ALL);

    // When: getDatacenterAvailability is called
    Response response = reaperResource.getDatacenterAvailability();

    // Then: Should return OK with ALL
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getEntity()).isInstanceOf(BiMap.class);
    @SuppressWarnings("unchecked")
    BiMap<String, DatacenterAvailability> result =
        (BiMap<String, DatacenterAvailability>) response.getEntity();
    assertThat(result).containsEntry("datacenterAvailability", DatacenterAvailability.ALL);
  }

  @Test
  void testGetDatacenterAvailability_WithLocalDatacenter_ShouldReturnLocal() {
    // Given: Configuration set to LOCAL
    when(mockConfig.getDatacenterAvailability()).thenReturn(DatacenterAvailability.LOCAL);

    // When: getDatacenterAvailability is called
    Response response = reaperResource.getDatacenterAvailability();

    // Then: Should return OK with LOCAL
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getEntity()).isInstanceOf(BiMap.class);
    @SuppressWarnings("unchecked")
    BiMap<String, DatacenterAvailability> result =
        (BiMap<String, DatacenterAvailability>) response.getEntity();
    assertThat(result).containsEntry("datacenterAvailability", DatacenterAvailability.LOCAL);
  }

  @Test
  void testGetDatacenterAvailability_WithEachDatacenter_ShouldReturnEach() {
    // Given: Configuration set to EACH
    when(mockConfig.getDatacenterAvailability()).thenReturn(DatacenterAvailability.EACH);

    // When: getDatacenterAvailability is called
    Response response = reaperResource.getDatacenterAvailability();

    // Then: Should return OK with EACH
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getEntity()).isInstanceOf(BiMap.class);
    @SuppressWarnings("unchecked")
    BiMap<String, DatacenterAvailability> result =
        (BiMap<String, DatacenterAvailability>) response.getEntity();
    assertThat(result).containsEntry("datacenterAvailability", DatacenterAvailability.EACH);
  }

  @Test
  void testGetDatacenterAvailability_WithSidecarDatacenter_ShouldReturnSidecar() {
    // Given: Configuration set to SIDECAR
    when(mockConfig.getDatacenterAvailability()).thenReturn(DatacenterAvailability.SIDECAR);

    // When: getDatacenterAvailability is called
    Response response = reaperResource.getDatacenterAvailability();

    // Then: Should return OK with SIDECAR
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getEntity()).isInstanceOf(BiMap.class);
    @SuppressWarnings("unchecked")
    BiMap<String, DatacenterAvailability> result =
        (BiMap<String, DatacenterAvailability>) response.getEntity();
    assertThat(result).containsEntry("datacenterAvailability", DatacenterAvailability.SIDECAR);
  }

  @Test
  void testGetDatacenterAvailability_ResponseStructure_ShouldBeCorrect() {
    // Given: Any configuration
    when(mockConfig.getDatacenterAvailability()).thenReturn(DatacenterAvailability.ALL);

    // When: getDatacenterAvailability is called
    Response response = reaperResource.getDatacenterAvailability();

    // Then: Response should have correct structure
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getEntity()).isInstanceOf(BiMap.class);
    @SuppressWarnings("unchecked")
    BiMap<String, DatacenterAvailability> result =
        (BiMap<String, DatacenterAvailability>) response.getEntity();
    assertThat(result).hasSize(1);
    assertThat(result).containsKey("datacenterAvailability");
  }

  @Test
  void testConstructor_WithValidContext_ShouldCreateInstance() {
    // Given: Valid AppContext
    AppContext context = new AppContext();
    context.config = mock(ReaperApplicationConfiguration.class);

    // When: Constructor is called
    ReaperResource resource = new ReaperResource(context);

    // Then: Should create instance
    assertThat(resource).isNotNull();
  }

  @Test
  void testGetDatacenterAvailability_WithNullAvailability_ShouldHandleGracefully() {
    // Given: Configuration returns null
    when(mockConfig.getDatacenterAvailability()).thenReturn(null);

    // When: getDatacenterAvailability is called
    // Then: Should handle gracefully - ImmutableMap doesn't allow null values
    // so this will throw an exception, which is expected behavior
    try {
      Response response = reaperResource.getDatacenterAvailability();
      // If we get here, check that the response is still valid
      assertThat(response.getStatus()).isEqualTo(200);
    } catch (NullPointerException e) {
      // This is expected since ImmutableMap doesn't allow null values
      assertThat(e.getMessage()).contains("null");
    }
  }

  @Test
  void testGetDatacenterAvailability_MultipleCallsSameResult_ShouldBeConsistent() {
    // Given: Configuration set to a specific value
    when(mockConfig.getDatacenterAvailability()).thenReturn(DatacenterAvailability.LOCAL);

    // When: getDatacenterAvailability is called multiple times
    Response response1 = reaperResource.getDatacenterAvailability();
    Response response2 = reaperResource.getDatacenterAvailability();
    Response response3 = reaperResource.getDatacenterAvailability();

    // Then: All responses should be the same
    assertThat(response1.getStatus()).isEqualTo(200);
    assertThat(response2.getStatus()).isEqualTo(200);
    assertThat(response3.getStatus()).isEqualTo(200);
    assertThat(response1.getEntity()).isEqualTo(response2.getEntity());
    assertThat(response2.getEntity()).isEqualTo(response3.getEntity());
  }
}
