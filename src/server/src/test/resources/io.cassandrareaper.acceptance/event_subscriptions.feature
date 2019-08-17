# Copyright 2019-2019 The Last Pickle Ltd
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

Feature: Manage diagnostic event subscriptions

  Scenario: Retrieving an empty subscriptions list
    Given a reaper service is running
    When a get all subscriptions request is made
    Then the returned list of subscriptions is empty

  Scenario: Retrieving an empty subscriptions list by existing cluster name
    Given a reaper service is running
    When a get-subscriptions request is made for cluster "test_cluster"
    Then the returned list of subscriptions is empty

  Scenario: Retrieve subscription for non existing cluster name
    Given a reaper service is running
    And the following subscriptions are created:
      | clusterName    | description  | includeNodes | events    | exportSse  | exportFileLogger   | exportHttpEndpoint |
      | test_cluster_X | Test Cluster |              | A,B       | true       |                    |                    |
    When a get-subscriptions request is made for cluster "test_cluster_non_existing"
    Then the returned list of subscriptions is empty

  Scenario: Retrieve subscriptions for existing cluster name
    Given a reaper service is running
    And the following subscriptions are created:
      | clusterName    | description  | includeNodes | events    | exportSse  | exportFileLogger   | exportHttpEndpoint |
      | test_cluster   | Sub A        |              | A,B       | true       |                    |                    |
      | test_cluster   | Sub B        |              | B         | false      | mylogger           |                    |
      | test_cluster_2 |              | 127.0.0.100  | A,C,D     | true       |                    |                    |
    When a get-subscriptions request is made for cluster "test_cluster"
    Then the returned list of subscriptions is:
      | clusterName    | description  | includeNodes | events    | exportSse  | exportFileLogger   | exportHttpEndpoint |
      | test_cluster   | Sub A        |              | A,B       | true       |                    |                    |
      | test_cluster   | Sub B        |              | B         | false      | mylogger           |                    |

  Scenario: Retrieve last inserted subscription by ID
    Given a reaper service is running
    When a get-subscription request is made for the last inserted ID
    Then the returned list of subscriptions is:
      | clusterName    | description  | includeNodes | events    | exportSse  | exportFileLogger   | exportHttpEndpoint |
      | test_cluster_2 |              | 127.0.0.100  | A,C,D     | true       |                    |                    |

  Scenario: Add duplicate subscriptions
    Given a reaper service is running
    And the following subscriptions are created:
      | clusterName    | description  | includeNodes | events    | exportSse  | exportFileLogger   | exportHttpEndpoint |
      | test_cluster_3 | Sub Y        |              | Y,Z       | false      |                    | http://localhost/1 |
      | test_cluster_3 | Sub Y        |              | Y,Z       | false      |                    |                    |
    When a get-subscriptions request is made for cluster "test_cluster_3"
    Then the returned list of subscriptions is:
      | clusterName    | description  | includeNodes | events    | exportSse  | exportFileLogger   | exportHttpEndpoint |
      | test_cluster_3 | Sub Y        |              | Y,Z       | false      |                    | http://localhost/1 |
      | test_cluster_3 | Sub Y        |              | Y,Z       | false      |                    | http://localhost/1 |

  Scenario: Add duplicate subscriptions
    Given a reaper service is running
    When the last created subscription is deleted
    And a get-subscriptions request is made for cluster "test_cluster_3"
    Then the returned list of subscriptions is:
      | clusterName    | description  | includeNodes | events    | exportSse  | exportFileLogger   | exportHttpEndpoint |
      | test_cluster_3 | Sub Y        |              | Y,Z       | false      |                    | http://localhost/1 |

  Scenario: Retrieve all subscriptions
    Given a reaper service is running
    When a get all subscriptions request is made
    Then the returned list of subscriptions is:
      | clusterName    | description  | includeNodes | events    | exportSse  | exportFileLogger   | exportHttpEndpoint |
      | test_cluster_X | Test Cluster |              | A,B       | true       |                    |                    |
      | test_cluster   | Sub A        |              | A,B       | true       |                    |                    |
      | test_cluster   | Sub B        |              | B         | false      | mylogger           |                    |
      | test_cluster_2 |              | 127.0.0.100  | A,C,D     | true       |                    |                    |
      | test_cluster_3 | Sub Y        |              | Y,Z       | false      |                    | http://localhost/1 |


