package com.spotify.reaper.acceptance;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Helper class for holding acceptance test scenario state.
 * Contains also methods for getting related resources for testing, like mocks etc.
 */
public class TestContext {

  public static  String TEST_USER = "test_user";
  public static String SEED_HOST;

  /* Used for targeting an object accessed in last test step. */
  public static Long LAST_MODIFIED_ID;

  /* Testing cluster seed host mapped to cluster name. */
  public static Map<String, String> TEST_CLUSTER_SEED_HOSTS = new HashMap<>();

  /* Testing cluster name mapped to keyspace name mapped to tables list. */
  public static Map<String, Map<String, Set<String>>> TEST_CLUSTER_INFO = new HashMap<>();

  /**
   * Adds testing cluster information for testing purposes.
   * Used to create mocks and prepare testing access for added testing clusters.
   */
  public static void addClusterInfo(String clusterName, String keyspace, Set<String> tables) {
    if (!TEST_CLUSTER_INFO.containsKey(clusterName)) {
      TEST_CLUSTER_INFO.put(clusterName, new HashMap<String, Set<String>>());
    }
    TEST_CLUSTER_INFO.get(clusterName).put(keyspace, tables);
  }

  public static void addSeedHostToClusterMapping(String seedHost, String clusterName) {
    TEST_CLUSTER_SEED_HOSTS.put(seedHost, clusterName);
  }


}
