/*
 * Copyright 2025-2025 DataStax, Inc.
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

package io.cassandrareaper.service;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.CompactionStats;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.management.ClusterFacade;
import io.cassandrareaper.storage.IStorageDao;

import java.io.IOException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** Simple unit tests for CompactionService class. */
public class CompactionServiceSimpleTest {

  private AppContext mockContext;
  private IStorageDao mockStorage;
  private ClusterFacade mockClusterFacade;
  private CompactionService compactionService;

  @BeforeEach
  void setUp() {
    mockContext = mock(AppContext.class);
    mockStorage = mock(IStorageDao.class);
    mockClusterFacade = mock(ClusterFacade.class);
    mockContext.storage = mockStorage;
    compactionService = CompactionService.create(mockContext);
  }

  @Test
  void testCreate_ShouldReturnNewInstance() {
    // When: Creating a new CompactionService
    CompactionService service = CompactionService.create(mockContext);

    // Then: Should return a non-null instance
    assertThat(service).isNotNull();
  }

  @Test
  void testListActiveCompactions_Success() throws Exception {
    // Given: A node and expected compaction stats
    Node node =
        Node.builder()
            .withHostname("127.0.0.1")
            .withCluster(
                Cluster.builder()
                    .withName("test-cluster")
                    .withSeedHosts(Collections.singleton("127.0.0.1"))
                    .build())
            .build();

    CompactionStats expectedStats = mock(CompactionStats.class);

    try (MockedStatic<ClusterFacade> mockedClusterFacade = mockStatic(ClusterFacade.class)) {
      mockedClusterFacade
          .when(() -> ClusterFacade.create(mockContext))
          .thenReturn(mockClusterFacade);
      when(mockClusterFacade.listActiveCompactions(node)).thenReturn(expectedStats);

      // When: Listing active compactions
      CompactionStats result = compactionService.listActiveCompactions(node);

      // Then: Should return the expected stats
      assertThat(result).isEqualTo(expectedStats);
    }
  }

  @Test
  void testListActiveCompactions_ReaperException_ShouldThrowReaperException() throws Exception {
    // Given: A node and ReaperException
    Node node =
        Node.builder()
            .withHostname("127.0.0.1")
            .withCluster(
                Cluster.builder()
                    .withName("test-cluster")
                    .withSeedHosts(Collections.singleton("127.0.0.1"))
                    .build())
            .build();

    try (MockedStatic<ClusterFacade> mockedClusterFacade = mockStatic(ClusterFacade.class)) {
      mockedClusterFacade
          .when(() -> ClusterFacade.create(mockContext))
          .thenReturn(mockClusterFacade);
      when(mockClusterFacade.listActiveCompactions(node))
          .thenThrow(new ReaperException("Management error"));

      // When/Then: Should throw ReaperException
      assertThatThrownBy(() -> compactionService.listActiveCompactions(node))
          .isInstanceOf(ReaperException.class)
          .hasMessage("Management error");
    }
  }

  @Test
  void testListActiveCompactions_IOException_ShouldThrowReaperException() throws Exception {
    // Given: A node and IOException
    Node node =
        Node.builder()
            .withHostname("127.0.0.1")
            .withCluster(
                Cluster.builder()
                    .withName("test-cluster")
                    .withSeedHosts(Collections.singleton("127.0.0.1"))
                    .build())
            .build();

    try (MockedStatic<ClusterFacade> mockedClusterFacade = mockStatic(ClusterFacade.class)) {
      mockedClusterFacade
          .when(() -> ClusterFacade.create(mockContext))
          .thenReturn(mockClusterFacade);
      when(mockClusterFacade.listActiveCompactions(node)).thenThrow(new IOException("IO error"));

      // When/Then: Should throw ReaperException
      assertThatThrownBy(() -> compactionService.listActiveCompactions(node))
          .isInstanceOf(ReaperException.class)
          .hasCauseInstanceOf(IOException.class);
    }
  }
}
