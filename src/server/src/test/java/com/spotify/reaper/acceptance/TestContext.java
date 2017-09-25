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

package com.spotify.reaper.acceptance;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Helper class for holding acceptance test scenario state.
 * Contains also methods for getting related resources for testing, like mocks etc.
 */
public final class TestContext {

  public static String TEST_USER = "test_user";
  public static String SEED_HOST;
  public static String TEST_CLUSTER;

  /* Used for targeting an object accessed in last test step. */
  public static UUID LAST_MODIFIED_ID;

  /* Testing cluster seed host mapped to cluster name. */
  public static Map<String, String> TEST_CLUSTER_SEED_HOSTS = new HashMap<>();

  /* Testing cluster name mapped to keyspace name mapped to tables list. */
  public static Map<String, Map<String, Set<String>>> TEST_CLUSTER_INFO = new HashMap<>();

  private TestContext() {}

  /**
   * Adds testing cluster information for testing purposes.
   * Used to create mocks and prepare testing access for added testing clusters.
   */
  public static void addClusterInfo(String clusterName, String keyspace, Set<String> tables) {
    if (!TEST_CLUSTER_INFO.containsKey(clusterName)) {
      TEST_CLUSTER_INFO.put(clusterName, new HashMap<>());
    }
    TEST_CLUSTER_INFO.get(clusterName).put(keyspace, tables);
  }

  public static void addSeedHostToClusterMapping(String seedHost, String clusterName) {
    TEST_CLUSTER_SEED_HOSTS.put(seedHost, clusterName);
  }

}
