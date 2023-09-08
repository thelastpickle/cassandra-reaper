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
  @cassandra_3_11_onwards
  @http_management
  @focus
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
