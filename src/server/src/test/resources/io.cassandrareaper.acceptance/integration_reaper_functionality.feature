# Copyright 2016-2017 Spotify AB
# Copyright 2016-2019 The Last Pickle Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

Feature: Using Reaper

  Background:
    Given cluster seed host "127.0.0.1" points to cluster with name "test"
    And cluster "test" has keyspace "booya" with tables "booya1, booya2"
    And cluster "test" has keyspace "booya" with tables "booya_twcs"
    And cluster "test" has keyspace "test_keyspace" with tables "test_table1, test_table2"
    And cluster "test" has keyspace "test_keyspace2" with tables "test_table1, test_table2"
    And cluster "test" has keyspace "test_keyspace3" with tables "test_table1, test_table2"

@all_nodes_reachable
  @cassandra_3_11_onwards
  @snapshots
  Scenario Outline: Create a cluster, create a cluster wide snapshot and delete it
    Given that reaper <version> is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
    When a request is made to clear the existing snapshot cluster wide
    And a cluster wide snapshot request is made to Reaper
    Then there is 1 snapshot returned when listing snapshots
    And a cluster wide snapshot request is made to Reaper for keyspace "booya"
    And a cluster wide snapshot request fails for keyspace "nonexistent"
    Then there is 2 snapshot returned when listing snapshots cluster wide
    And I fail listing cluster wide snapshots for cluster "fake"
    When a request is made to clear the existing snapshot cluster wide
    Then there is 0 snapshot returned when listing snapshots
    When the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage
  ${cucumber.upgrade-versions}

  @all_nodes_reachable
  @cassandra_3_11_onwards
  @snapshots
  Scenario Outline: Create a cluster, create a snapshot on a single host and delete it
    Given that reaper <version> is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
    When a request is made to clear the seed host existing snapshots
    And a snapshot request for the seed host is made to Reaper
    Then there is 1 snapshot returned when listing snapshots
    And a snapshot request for the seed host and keyspace "booya" is made to Reaper
    And a snapshot request for the seed host and keyspace "fake" fails
    Then there is 2 snapshot returned when listing snapshots
    And I fail listing snapshots for cluster "fake" and host "127.0.0.1"
    And I fail listing snapshots for cluster "test" and host "fakenode"
    When a request is made to clear the seed host existing snapshots
    Then there is 0 snapshot returned when listing snapshots
    When the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage
  ${cucumber.upgrade-versions}

  @sidecar
  Scenario Outline: Registering a cluster with JMX auth
    Given that reaper <version> is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper with authentication
    Then reaper has the last added cluster in storage
    Then reaper has the last added cluster in storage
    When the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage
  ${cucumber.upgrade-versions}
 
  @sidecar
  Scenario Outline: Registering a cluster with JMX auth but no encryption
    Given that reaper <version> is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper with authentication and no encryption
    Then reaper has no cluster in storage
  ${cucumber.upgrade-versions}
  
  @sidecar
  Scenario Outline: Registering a cluster without JMX auth
    Given that reaper <version> is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
    Then reaper has the last added cluster in storage
    When the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage
  ${cucumber.upgrade-versions}

  @sidecar
  Scenario Outline: Force deleting a cluster
    Given that reaper <version> is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
    And reaper has 0 scheduled repairs for cluster called "test"
    When a new daily "full" repair schedule is added for "test" and keyspace "test_keyspace"
    Then reaper has a cluster called "test" in storage
    And reaper has 1 scheduled repairs for cluster called "test"
    Then reaper has a cluster called "test" in storage
    And reaper has 1 scheduled repairs for cluster called "test"
    When deleting cluster called "test" fails
    Then reaper has a cluster called "test" in storage
    And reaper has 1 scheduled repairs for cluster called "test"
    When the last added cluster is force deleted
    And reaper has 0 scheduled repairs for cluster called "test"
    Then reaper has no longer the last added cluster in storage
  ${cucumber.upgrade-versions}

  @sidecar
  Scenario Outline: Create a cluster and a scheduled repair run and delete them
    Given that reaper <version> is running
    And cluster seed host "127.0.0.2" points to cluster with name "test"
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
    And the seed node has vnodes
    And reaper has 0 scheduled repairs for the last added cluster
    And we can collect the tpstats from a seed node
    And we can collect the dropped messages stats from a seed node
    And we can collect the client request metrics from a seed node
    When a new daily "full" repair schedule is added for the last added cluster and keyspace "booya"
    Then reaper has 1 scheduled repairs for the last added cluster
    Then reaper has 1 scheduled repairs for the last added cluster
    And deleting cluster called "test" fails
    When the last added schedule is deleted for the last added cluster
    And the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage
  ${cucumber.upgrade-versions}

  @sidecar
  Scenario Outline: Registering multiple scheduled repairs
    Given that reaper <version> is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
    And reaper has 0 scheduled repairs for cluster called "test"
    When a new daily "full" repair schedule is added for "test" and keyspace "test_keyspace"
    Then reaper has a cluster called "test" in storage
    And reaper has 1 scheduled repairs for cluster called "test"
    When a new daily "full" repair schedule is added for "test" and keyspace "test_keyspace2"
    And a second daily repair schedule is added for "test" and keyspace "test_keyspace3"
    Then reaper has a cluster called "test" in storage
    And reaper has 3 scheduled repairs for cluster called "test"
    And reaper has 3 scheduled repairs for cluster called "test"
    When the last added schedule is deleted for cluster called "test"
    Then reaper has 2 scheduled repairs for cluster called "test"
    When a new daily "full" repair schedule is added that already exists for "test" and keyspace "test_keyspace2"
    Then reaper has 2 scheduled repairs for cluster called "test"
    And deleting cluster called "test" fails
    When all added schedules are deleted for the last added cluster
    And the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage
  ${cucumber.upgrade-versions}

  @sidecar
  @all_nodes_reachable
  @cassandra_2_1_onwards
  Scenario Outline: Adding a scheduled full repair and a scheduled incremental repair for the same keyspace
    Given that reaper <version> is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
    And reaper has 0 scheduled repairs for cluster called "test"
    When a new daily "incremental" repair schedule is added for "test" and keyspace "test_keyspace3"
    And a new daily "full" repair schedule is added that already exists for "test" and keyspace "test_keyspace3"
    Then reaper has 1 scheduled repairs for cluster called "test"
    Then reaper has 1 scheduled repairs for cluster called "test"
    And deleting cluster called "test" fails
    When all added schedules are deleted for the last added cluster
    And the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage
  ${cucumber.upgrade-versions}

  @sidecar
  @all_nodes_reachable
  @cassandra_2_1_onwards
  Scenario Outline: Adding a scheduled full repair and a scheduled incremental repair for the same keyspace with force option
    Given that reaper <version> is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
    And reaper has 0 scheduled repairs for cluster called "test"
    When a new daily "incremental" repair schedule is added for "test" and keyspace "test_keyspace3"
    And a new daily "full" repair schedule fails to be added that already exists for "test" and keyspace "test_keyspace3"
    And a new daily "full" repair schedule is added that already exists for "test" and keyspace "test_keyspace3" with force option
    Then reaper has 2 scheduled repairs for cluster called "test"
    Then reaper has 2 scheduled repairs for cluster called "test"
    And deleting cluster called "test" fails
    When all added schedules are deleted for the last added cluster
    And the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage
  ${cucumber.upgrade-versions}

  @sidecar
  @all_nodes_reachable
  @cassandra_3_11_onwards
  Scenario Outline: Add a scheduled incremental repair and collect percent repaired metrics
    Given that reaper <version> is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
    And reaper has 0 scheduled repairs for cluster called "test"
    When a new daily "incremental" repair schedule is added for "test" and keyspace "test_keyspace3"
    Then reaper has 1 scheduled repairs for cluster called "test"
    Then reaper has 1 scheduled repairs for cluster called "test"
    And percent repaired metrics get collected for the existing schedule
    And deleting cluster called "test" fails
    When all added schedules are deleted for the last added cluster
    And the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage
  ${cucumber.upgrade-versions}

  @sidecar
  Scenario Outline: Create a cluster and a scheduled repair run with repair run history and delete them
    Given that reaper <version> is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
    And reaper has 0 scheduled repairs for the last added cluster
    When a new daily repair schedule is added for the last added cluster and keyspace "booya" with next repair immediately
    Then reaper has 1 scheduled repairs for the last added cluster
    And deleting cluster called "test" fails
    When we wait for a scheduled repair run has started for cluster "test"
    And we wait for at least 1 segments to be repaired
    Then reaper has 1 started or done repairs for the last added cluster
    When the last added repair is stopped
    Then reseting one segment sets its state to not started
    And all added repair runs are deleted for the last added cluster
    And deleting cluster called "test" fails
    When all added schedules are deleted for the last added cluster
    And the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage
  ${cucumber.upgrade-versions}

  @sidecar
  Scenario Outline: Create a cluster and a repair run and delete them
    Given that reaper <version> is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
    And reaper has 0 repairs for the last added cluster
    When a new repair is added for the last added cluster and keyspace "booya" with the table "booya2" blacklisted
    And the last added repair has table "booya2" in the blacklist
    And the last added repair has twcs table "booya_twcs" in the blacklist
    And the last added repair has table "booya2" in the blacklist
    And deleting cluster called "test" fails
    And the last added repair is activated
    And we wait for at least 1 segments to be repaired
    Then reaper has 1 started or done repairs for the last added cluster
    When the last added repair is stopped
    And all added repair runs are deleted for the last added cluster
    And the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage
  ${cucumber.upgrade-versions}

  # this has a problem in the upgrade integration tests, ref: 88d4d5c
  Scenario Outline: Create a cluster and a repair run with auto twcs blacklist and delete them
    Given that reaper <version> is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
    And reaper has 0 repairs for the last added cluster
    When a new repair is added for the last added cluster and keyspace "booya"
    And the last added repair has twcs table "booya_twcs" in the blacklist
    And the last added repair has twcs table "booya_twcs" in the blacklist
    And deleting cluster called "test" fails
    And the last added repair is activated
    And we wait for at least 1 segments to be repaired
    Then reaper has 1 started or done repairs for the last added cluster
    When the last added repair is stopped
    And all added repair runs are deleted for the last added cluster
    And the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage
 ${cucumber.upgrade-versions}

  @sidecar
  @all_nodes_reachable
  @cassandra_2_1_onwards
  Scenario Outline: Create a cluster and an incremental repair run and delete them
    Given that reaper <version> is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
    And reaper has 0 repairs for the last added cluster
    When a new incremental repair is added for the last added cluster and keyspace "booya"
    And deleting cluster called "test" fails
    And the last added repair is activated
    And we wait for at least 1 segments to be repaired
    Then reaper has 1 started or done repairs for the last added cluster
    Then reaper has 1 started or done repairs for the last added cluster
    When the last added repair is stopped
    When a new repair is added for the last added cluster and keyspace "booya" with force option
    And the last added repair is activated
    And we wait for at least 1 segments to be repaired
    Then reaper has 2 repairs for cluster called "test"
    When the last added repair is stopped
    And all added repair runs are deleted for the last added cluster
    And the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage
  ${cucumber.upgrade-versions}

  @sidecar
    @all_nodes_reachable
    @cassandra_4_0_onwards
    Scenario Outline: Create a cluster and a subrange incremental repair run and delete them
      Given that reaper <version> is running
      And reaper has no cluster in storage
      When an add-cluster request is made to reaper
      Then reaper has the last added cluster in storage
      And reaper has 0 repairs for the last added cluster
      When a new subrange incremental repair is added for the last added cluster and keyspace "booya"
      And deleting cluster called "test" fails
      And the last added repair is activated
      And we wait for at least 1 segments to be repaired
      Then reaper has 1 started or done repairs for the last added cluster
      Then reaper has 1 started or done repairs for the last added cluster
      When the last added repair is stopped
      When a new repair is added for the last added cluster and keyspace "booya" with force option
      And the last added repair is activated
      And we wait for at least 1 segments to be repaired
      Then reaper has 2 repairs for cluster called "test"
      When the last added repair is stopped
      And all added repair runs are deleted for the last added cluster
      And the last added cluster is deleted
      Then reaper has no longer the last added cluster in storage
    ${cucumber.upgrade-versions}

  @sidecar
  @all_nodes_reachable
  @cassandra_2_1_onwards
  Scenario Outline: Create a cluster and one incremental repair run and one full repair run
    Given that reaper <version> is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
    And reaper has 0 scheduled repairs for cluster called "test"
    When a new incremental repair is added for "test" and keyspace "test_keyspace"
    And the last added repair is activated
    And a new repair is added for "test" and keyspace "test_keyspace2"
    And the last added repair is activated
    Then reaper has 2 repairs for cluster called "test"
    Then reaper has 2 started or done repairs for the last added cluster
    Then reaper has 2 started or done repairs for the last added cluster
    And we wait for at least 1 segments to be repaired
    And modifying the run state of the last added repair to "PAUSED" succeeds
    And modifying the run state of the last added repair to "RUNNING" succeeds
    And I can purge repair runs
    When all added repair runs are deleted for the last added cluster
    And the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage
  ${cucumber.upgrade-versions}

  @all_nodes_reachable
  @cassandra_2_1_onwards
  Scenario Outline: Test resources failure paths
    Given that reaper <version> is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
    And reaper has 0 scheduled repairs for cluster called "test"
    And a new repair fails to be added for keyspace "test_keyspace" and "tables" "nonexistent_table"
    And a new repair fails to be added for keyspace "test_keyspace" and "blacklistedTables" "nonexistent_table"
    And a new repair fails to be added for keyspace "test_keyspace" and "nodes" "nonexistent_node"
    And a new repair fails to be added for keyspace "test_keyspace" and "segmentCountPerNode" "2000"
    And a new repair fails to be added for keyspace "test_keyspace" and "repairParallelism" "whatever"
    And a new repair fails to be added for keyspace "test_keyspace" including both node and datacenter lists
    And a new repair fails to be added for keyspace "test_keyspace" including both tables and blacklisted tables lists
    And a new repair fails to be added for keyspace "test_keyspace" without the "clusterName" param
    And a new repair fails to be added for keyspace "test_keyspace" without the "owner" param
    And a new repair fails to be added for keyspace "test_keyspace" without the "keyspace" param
    And a new daily repair schedule fails being added with "2000-01-01T00:00:00" activation time
    And a new daily repair schedule fails being added with "200-1-01T00-0-000" activation time
    And a new daily repair schedule fails being added without "clusterName"
    And a new daily repair schedule fails being added without "keyspace"
    And a new daily repair schedule fails being added without "owner"
    And a new daily repair schedule fails being added without "scheduleDaysBetween"
    And a new daily repair schedule fails being added with "clusterName" "nonexistent_cluster"
    And a new daily repair schedule fails being added with "tables" "nonexistent_table"
    And a new daily repair schedule fails being added with "blacklistedTables" "nonexistent_table"
    And a new daily repair schedule fails being added with "nodes" "nonexistent_node"
    And a new daily repair schedule fails being added with "segmentCountPerNode" "2000"
    And a new daily repair schedule fails being added with "repairParallelism" "whatever"
    And a new daily repair schedule fails being added with "incrementalRepair" "True"
    And a new daily repair schedule fails being added with "percentUnrepairedThreshold" "10"
    And getting repair run "whatever" fails
    And aborting a segment from a non existent repair fails
    When a new incremental repair is added for the last added cluster and keyspace "booya"
    Then aborting a segment on the last added repair fails
    And the last added repair fails to be deleted
    When the last added repair is activated
    And the last added repair fails to be deleted
    And modifying the run state of the last added repair to "WHATEVER" fails
    And we wait for at least 1 segments to be repaired
    And the last added repair is stopped
    And the last added repair fails to be deleted with owner ""
    And the last added repair fails to be deleted with owner "whatever"
    And all added repair runs are deleted for the last added cluster
    And the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage
  ${cucumber.upgrade-versions}

  @sidecar
  @all_nodes_reachable
  @cassandra_2_1_onwards
  Scenario Outline: Exhaustive testing on schedules
    Given that reaper <version> is running
    And cluster seed host "127.0.0.2" points to cluster with name "test"
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
    And the seed node has vnodes
    And reaper has 0 scheduled repairs for the last added cluster
    When a new daily "full" repair schedule is added for the last added cluster and keyspace "booya"
    Then reaper has 1 scheduled repairs for cluster "test" and keyspace "booya"
    Then reaper has 1 scheduled repairs for cluster "" and keyspace "booya"
    Then reaper has 1 scheduled repairs for cluster "test" and keyspace ""
    Then reaper has 1 scheduled repairs for cluster "" and keyspace ""
    And I can set the last added schedule state to "ACTIVE"
    And the last added schedule fails being deleted for the last added cluster
    And I can set the last added schedule state to "PAUSED"
    And I can set the last added schedule state to "ACTIVE"
    And I cannot set the last added schedule state to "UNKNOWN"
    And I cannot set the last added schedule state to "DELETED"
    And I cannot set an unknown schedule state to "ACTIVE"
    And deleting cluster called "test" fails
    And the last added schedule fails being deleted for the last added cluster with owner ""
    And the last added schedule fails being deleted for the last added cluster with owner "fake"
    And I can start the last added schedule
    And I cannot start an unknown schedule
    When the last added cluster is force deleted
    Then reaper has no longer the last added cluster in storage
  ${cucumber.upgrade-versions}

  @sidecar
  Scenario Outline: Verify that ongoing repairs are prioritized over finished ones when listing the runs
    Given that reaper <version> is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper with authentication
    Then reaper has the last added cluster in storage
    When we add a fake cluster named "fake1"
    And we add a fake cluster named "fake2"
    And I add 5 and abort the most recent 3 repairs for cluster "fake1" and keyspace "test_keyspace2"
    And I add 25 and abort the most recent 24 repairs for cluster "fake2" and keyspace "test_keyspace2"
    And I add 25 and abort the most recent 24 repairs for cluster "test" and keyspace "test_keyspace2"
    When I list the last 10 repairs for cluster "test", I can see 1 repairs at "PAUSED" state
    When I list the last 10 repairs for cluster "test", I can see 9 repairs at "ABORTED" state
    When I list the last 10 repairs for cluster "fake1", I can see 2 repairs at "PAUSED" state
    When I list the last 10 repairs for cluster "fake1", I can see 3 repairs at "ABORTED" state
    When I list the last 10 repairs for cluster "fake2", I can see 1 repairs at "PAUSED" state
    When I list the last 10 repairs for cluster "fake2", I can see 9 repairs at "ABORTED" state
    When I list the last 10 repairs, I can see 4 repairs at "PAUSED" state
    When I list the last 10 repairs, I can see 6 repairs at "ABORTED" state
    When the last added cluster is force deleted
    Then reaper has no longer the last added cluster in storage
  ${cucumber.upgrade-versions}