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

  @sidecar
  Scenario Outline: Registering a cluster with JMX auth
    Given that reaper <version> is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper with authentication
    Then reaper has the last added cluster in storage
    When reaper is upgraded to latest
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
    When reaper is upgraded to latest
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
    When reaper is upgraded to latest
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
    When reaper is upgraded to latest
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
    When reaper is upgraded to latest
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
    When reaper is upgraded to latest
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
    And a new daily "full" repair schedule is added that already exists for "test" and keyspace "test_keyspace3" with force option
    Then reaper has 2 scheduled repairs for cluster called "test"
    When reaper is upgraded to latest
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
    When reaper is upgraded to latest
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
    When reaper is upgraded to latest
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
    When reaper is upgraded to latest
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
    When reaper is upgraded to latest
    Then reaper has 1 started or done repairs for the last added cluster
    When the last added repair is stopped
    And a new repair is added for the last added cluster and keyspace "booya"
    Then reaper has 1 repairs for cluster called "test"
    When a new repair is added for the last added cluster and keyspace "booya" with force option
    Then reaper has 2 repairs for cluster called "test"
    When all added repair runs are deleted for the last added cluster
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
    When reaper is upgraded to latest
    Then reaper has 2 started or done repairs for the last added cluster
    And we wait for at least 1 segments to be repaired
    When all added repair runs are deleted for the last added cluster
    And the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage
  ${cucumber.upgrade-versions}

  @all_nodes_reachable
  @cassandra_2_1_onwards
  Scenario Outline: Create a cluster, create a cluster wide snapshot and delete it
    Given that reaper <version> is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
    When a request is made to clear the existing snapshot cluster wide
    And a cluster wide snapshot request is made to Reaper
    Then there is 1 snapshot returned when listing snapshots
    When reaper is upgraded to latest
    Then there is 1 snapshot returned when listing snapshots
    When a request is made to clear the existing snapshot cluster wide
    Then there is 0 snapshot returned when listing snapshots
    When the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage
  ${cucumber.upgrade-versions}


  @all_nodes_reachable
  @cassandra_2_1_onwards
  Scenario Outline: Create a cluster, create a snapshot on a single host and delete it
    Given that reaper <version> is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
    When a request is made to clear the seed host existing snapshots
    And a snapshot request for the seed host is made to Reaper
    Then there is 1 snapshot returned when listing snapshots
    When reaper is upgraded to latest
    Then there is 1 snapshot returned when listing snapshots
    When a request is made to clear the seed host existing snapshots
    Then there is 0 snapshot returned when listing snapshots
    When the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage
  ${cucumber.upgrade-versions}
