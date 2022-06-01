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

@metrics
Feature: Using Reaper

  Background:
    Given cluster seed host "127.0.0.1" points to cluster with name "test"
    And cluster "test" has keyspace "booya" with tables "booya1"
    And cluster "test" has keyspace "booya" with tables "booya_twcs"
    And cluster "test" has keyspace "test_keyspace" with tables "test_table1, test_table2"
    And cluster "test" has keyspace "test_keyspace2" with tables "test_table1, test_table2"
    And cluster "test" has keyspace "test_keyspace3" with tables "test_table1, test_table2"

  Scenario Outline: Schedule metrics are present for every schedule and deleted when the schedule is removed
    Given that reaper <version> is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
    And reaper has 0 scheduled repairs for cluster called "test"
    When a new daily "full" repair schedule is added for "test" and keyspace "test_keyspace"
    Then reaper has a cluster called "test" in storage
    And reaper has 1 scheduled repairs for cluster called "test"
    And metrics contain 1 repair schedule metrics for cluster called "test"
    When a new daily "full" repair schedule is added for "test" and keyspace "test_keyspace2"
    And a second daily repair schedule is added for "test" and keyspace "test_keyspace3"
    Then reaper has a cluster called "test" in storage
    And reaper has 3 scheduled repairs for cluster called "test"
    And metrics contain 3 repair schedule metrics for cluster called "test"
    When the last added schedule is deleted for cluster called "test"
    Then reaper has 2 scheduled repairs for cluster called "test"
    And metrics contain 2 repair schedule metrics for cluster called "test"
    When a new daily "full" repair schedule is added that already exists for "test" and keyspace "test_keyspace2"
    Then reaper has 2 scheduled repairs for cluster called "test"
    And metrics contain 2 repair schedule metrics for cluster called "test"
    And deleting cluster called "test" fails
    When all added schedules are deleted for the last added cluster
    And the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage
    And metrics contain no repair schedule metrics for cluster
  ${cucumber.upgrade-versions}

  Scenario Outline: Create a scheduled repair and see it complete and the metrics should update
    Given that reaper <version> is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
    And reaper has 0 scheduled repairs for the last added cluster
    When a new daily repair schedule is added for the last added cluster and keyspace "booya" with next repair immediately
    Then reaper has 1 scheduled repairs for the last added cluster
    When we wait for a scheduled repair run has started for cluster "test"
    And we wait for all segments to be repaired
    Then reaper has 1 started or done repairs for the last added cluster
    And the schedule metrics for a cluster called "test" show that the repairs just completed
  ${cucumber.upgrade-versions}
