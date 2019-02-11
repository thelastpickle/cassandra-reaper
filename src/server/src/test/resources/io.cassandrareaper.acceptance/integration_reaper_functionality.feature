# Copyright 2016-2017 Spotify AB
# Copyright 2016-2018 The Last Pickle Ltd
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
    Given cluster seed host "127.0.0.1@test" points to cluster with name "test"
    And cluster "test" has keyspace "booya" with tables "booya1, booya2"
    And cluster "test" has keyspace "booya" with tables "booya_twcs"
    And cluster "test" has keyspace "test_keyspace" with tables "test_table1, test_table2"
    And cluster "test" has keyspace "test_keyspace2" with tables "test_table1, test_table2"
    And cluster "test" has keyspace "test_keyspace3" with tables "test_table1, test_table2"


  Scenario Outline: Registering a cluster
    Given that reaper <version> is running
    And that we are going to use "127.0.0.1@test" as cluster seed host
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
    When reaper is upgraded to latest
    Then reaper has the last added cluster in storage
    When the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage
  ${cucumber.upgrade-versions}

  Scenario Outline: Create a cluster and a scheduled repair run and delete them
    Given that reaper <version> is running
    And that we are going to use "127.0.0.1@test" as cluster seed host
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    And we can collect the tpstats from the seed node
    And we can collect the dropped messages stats from the seed node
    And we can collect the client request metrics from the seed node
    Then reaper has the last added cluster in storage
    And the seed node has vnodes
    And reaper has 0 scheduled repairs for the last added cluster
    When a new daily "full" repair schedule is added for the last added cluster and keyspace "booya"
    Then reaper has 1 scheduled repairs for the last added cluster
    When reaper is upgraded to latest
    Then reaper has 1 scheduled repairs for the last added cluster
    And deleting the last added cluster fails
    When the last added schedule is deleted for the last added cluster
    And the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage
  ${cucumber.upgrade-versions}

  Scenario Outline: Registering multiple scheduled repairs
    Given that reaper <version> is running
    And that we are going to use "127.0.0.1@test" as cluster seed host
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

  @all_nodes_reachable
  Scenario Outline: Adding a scheduled full repair and a scheduled incremental repair for the same keyspace
    Given that reaper <version> is running
    And that we are going to use "127.0.0.1@test" as cluster seed host
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

  Scenario Outline: Create a cluster and a scheduled repair run with repair run history and delete them
    Given that reaper <version> is running
    And that we are going to use "127.0.0.1@test" as cluster seed host
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
    And reaper has 0 scheduled repairs for the last added cluster
    When a new daily repair schedule is added for the last added cluster and keyspace "booya" with next repair immediately
    Then reaper has 1 scheduled repairs for the last added cluster
    And deleting the last added cluster fails
    When we wait for a scheduled repair run has started for cluster "test"
    And we wait for at least 1 segments to be repaired
    Then reaper has 1 started or done repairs for the last added cluster
    When the last added repair is stopped
    Then reseting one segment sets its state to not started
    And the last added repair run is deleted
    And deleting the last added cluster fails
    When all added schedules are deleted for the last added cluster
    And the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage
  ${cucumber.upgrade-versions}

  Scenario Outline: Create a cluster and a repair run and delete them
    Given that reaper <version> is running
    And that we are going to use "127.0.0.1@test" as cluster seed host
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
    And reaper has 0 repairs for the last added cluster
    When a new repair is added for the last added cluster and keyspace "booya" with the table "booya2" blacklisted
    And the last added repair has table "booya2" in the blacklist
    And the last added repair has table "booya_twcs" in the blacklist
    When reaper is upgraded to latest
    And the last added repair has table "booya2" in the blacklist
    And deleting the last added cluster fails
    And the last added repair is activated
    And we wait for at least 1 segments to be repaired
    Then reaper has 1 started or done repairs for the last added cluster
    When the last added repair is stopped
    And the last added repair run is deleted
    And the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage
  ${cucumber.upgrade-versions}


# Commented out auto twcs blacklist scenario as classloading correctly atm (FIXME), ref: 88d4d5c 

  @all_nodes_reachable
  Scenario Outline: Create a cluster and an incremental repair run and delete them
    Given that reaper <version> is running
    And that we are going to use "127.0.0.1@test" as cluster seed host
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
    And reaper has 0 repairs for the last added cluster
    When a new incremental repair is added for the last added cluster and keyspace "booya"
    And deleting the last added cluster fails
    And the last added repair is activated
    And we wait for at least 1 segments to be repaired
    Then reaper has 1 started or done repairs for the last added cluster
    When reaper is upgraded to latest
    Then reaper has 1 started or done repairs for the last added cluster
    When the last added repair is stopped
    And the last added repair run is deleted
    And the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage
  ${cucumber.upgrade-versions}

  @all_nodes_reachable
  Scenario Outline: Create a cluster and one incremental repair run and one full repair run
    Given that reaper <version> is running
    And that we are going to use "127.0.0.1@test" as cluster seed host
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
  Scenario Outline: Create a cluster, create a cluster wide snapshot and delete it
    Given that reaper <version> is running
    And that we are going to use "127.0.0.1@test" as cluster seed host
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
  Scenario Outline: Create a cluster, create a snapshot on a single host and delete it
    Given that reaper <version> is running
    And that we are going to use "127.0.0.1@test" as cluster seed host
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
