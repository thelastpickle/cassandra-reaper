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

@cassandra_4_0_onwards
Feature: Manage diagnostic event subscriptions

  Scenario: Retrieving an empty subscriptions list
    Given that reaper   is running
    When a get all subscriptions request is made
    Then the returned list of subscriptions is empty

  Scenario: Retrieving an empty subscriptions list by existing cluster name
    Given that reaper   is running
    When a get-subscriptions request is made for cluster "test"
    Then the returned list of subscriptions is empty

  Scenario: Retrieve subscription for non existing cluster name
    Given that reaper   is running
    When a get all subscriptions request is made
    Then the returned list of subscriptions is empty
    And the following subscriptions are created:
      | clusterName | description  | nodes       | events                                        | exportSse | exportFileLogger | exportHttpEndpoint |
      | test        | Test Cluster | 127.0.0.2   | AuditEvent,BootstrapEvent                     | true      |                  |                    |
    When a get-subscriptions request is made for cluster "test_cluster_non_existing"
    Then the returned list of subscriptions is empty
    And all created subscriptions are deleted

  Scenario: Retrieve subscriptions for existing cluster name
    Given that reaper   is running
    When a get all subscriptions request is made
    Then the returned list of subscriptions is empty
    And the following subscriptions are created:
      | clusterName | description  | nodes       | events                                        | exportSse | exportFileLogger | exportHttpEndpoint |
      | test        | Sub A        | 127.0.0.2   | AuditEvent,BootstrapEvent                     | true      |                  |                    |
      | test        | Sub B        | 127.0.0.2   | BootstrapEvent                                | false     | mylogger         |                    |
    When a get-subscriptions request is made for cluster "test"
    Then the returned list of subscriptions is:
      | clusterName | description  | nodes      | events                                        | exportSse | exportFileLogger | exportHttpEndpoint |
      | test        | Sub A        | 127.0.0.2  | AuditEvent,BootstrapEvent                     | true      |                  |                    |
      | test        | Sub B        | 127.0.0.2  | BootstrapEvent                                | false     | mylogger         |                    |
    And all created subscriptions are deleted

  Scenario: Retrieve last inserted subscription by ID
    Given that reaper   is running
    When a get all subscriptions request is made
    Then the returned list of subscriptions is empty
    And the following subscriptions are created:
      | clusterName | description  | nodes       | events                                        | exportSse | exportFileLogger | exportHttpEndpoint |
      | test        | Sub A        | 127.0.0.2   | AuditEvent,BootstrapEvent                     | true      |                  |                    |
      | test        | Sub B        | 127.0.0.2   | BootstrapEvent                                | false     | mylogger         |                    |
      | test        |              | 127.0.0.100 | AuditEvent,GossiperEvent,HintEvent            | true      |                  |                    |
    When a get-subscription request is made for the last inserted ID
    Then the returned list of subscriptions is:
      | clusterName | description  | nodes       | events                                        | exportSse  | exportFileLogger   | exportHttpEndpoint |
      | test        |              | 127.0.0.100 | AuditEvent,GossiperEvent,HintEvent            | true       |                    |                    |
    And all created subscriptions are deleted

  Scenario: Add duplicate subscription
    Given that reaper   is running
    When a get all subscriptions request is made
    Then the returned list of subscriptions is empty
    And the following subscriptions are created:
      | clusterName | description  | nodes       | events                                        | exportSse | exportFileLogger | exportHttpEndpoint |
      | test        | Sub Y        | 127.0.0.2   | HintsServiceEvent,SchemaEvent                 | false     |                  | http://localhost/1 |
      | test        | Sub Y        | 127.0.0.2   | HintsServiceEvent,SchemaEvent                 | false     |                  |                    |
    And a get-subscriptions request is made for cluster "test"
    Then the returned list of subscriptions is:
      | clusterName | description  | nodes       | events                                        | exportSse | exportFileLogger | exportHttpEndpoint |
      | test        | Sub Y        | 127.0.0.2   | HintsServiceEvent,SchemaEvent                 | false     |                  | http://localhost/1 |
    And all created subscriptions are deleted

  Scenario: Add duplicate subscriptions
    Given that reaper   is running
    When a get all subscriptions request is made
    Then the returned list of subscriptions is empty
    And the following subscriptions are created:
      | clusterName | description  | nodes       | events                                        | exportSse | exportFileLogger | exportHttpEndpoint |
      | test        | Sub Y        | 127.0.0.2   | HintsServiceEvent,SchemaEvent                 | false     |                  | http://localhost/1 |
      | test        | Sub Z        | 127.0.0.2   | AuditEvent,GossiperEvent,HintEvent            | false     |                  |                    |
      | test        | Sub Y        | 127.0.0.2   | HintsServiceEvent,SchemaEvent                 | false     |                  | http://localhost/1 |
    When a get-subscriptions request is made for cluster "test"
    Then the returned list of subscriptions is:
      | clusterName | description  | nodes       | events                                        | exportSse | exportFileLogger | exportHttpEndpoint |
      | test        | Sub Y        | 127.0.0.2   | HintsServiceEvent,SchemaEvent                 | false     |                  | http://localhost/1 |
      | test        | Sub Z        | 127.0.0.2   | AuditEvent,GossiperEvent,HintEvent            | false     |                  |                    |
    And all created subscriptions are deleted

  Scenario: Retrieve all subscriptions
    Given that reaper   is running
    When a get all subscriptions request is made
    Then the returned list of subscriptions is empty
    And the following subscriptions are created:
      | clusterName | description  | nodes       | events                                        | exportSse | exportFileLogger | exportHttpEndpoint |
      | test        | Test Cluster | 127.0.0.2   | AuditEvent,BootstrapEvent                     | true      |                  |                    |
      | test        | Sub A        | 127.0.0.2   | AuditEvent,BootstrapEvent,HintEvent           | true      |                  |                    |
      | test        | Sub B        | 127.0.0.2   | BootstrapEvent                                | false     | mylogger         |                    |
      | test        |              | 127.0.0.100 | AuditEvent,GossiperEvent,HintEvent            | true      |                  |                    |
      | test        | Sub Y        | 127.0.0.2   | HintsServiceEvent,SchemaEvent                 | false     |                  | http://localhost/1 |
      | test        | Sub X        | 127.0.0.2   | HintsServiceEvent,SchemaEvent,ReadRepairEvent | false     |                  |                    |
    When the last created subscription is deleted
    When a get all subscriptions request is made
    Then the returned list of subscriptions is:
      | clusterName | description  | nodes       | events                                        | exportSse | exportFileLogger | exportHttpEndpoint |
      | test        | Test Cluster | 127.0.0.2   | AuditEvent,BootstrapEvent                     | true      |                  |                    |
      | test        | Sub A        | 127.0.0.2   | AuditEvent,BootstrapEvent,HintEvent           | true      |                  |                    |
      | test        | Sub B        | 127.0.0.2   | BootstrapEvent                                | false     | mylogger         |                    |
      | test        |              | 127.0.0.100 | AuditEvent,GossiperEvent,HintEvent            | true      |                  |                    |
      | test        | Sub Y        | 127.0.0.2   | HintsServiceEvent,SchemaEvent                 | false     |                  | http://localhost/1 |
    And all created subscriptions are deleted


