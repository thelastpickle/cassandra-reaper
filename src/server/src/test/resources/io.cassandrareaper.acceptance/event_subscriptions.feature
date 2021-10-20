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
  Background:
    Given cluster seed host "127.0.0.1" points to cluster with name "test"

  Scenario: Retrieve diagnostic events 
    Given that reaper   is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
    When a get all subscriptions request is made
    Then the returned list of subscriptions is empty
    And the following subscriptions are created:
      | clusterName | description  | nodes       | events                                        | exportSse | exportFileLogger | exportHttpEndpoint |
      | test        | Sub X        | 127.0.0.1   | GossiperEvent                                 | true      |                  |                    |
    When we listen for diagnostic events on the last created subscription
    And all adhoc subscriptions move to inactive state
    Then all created subscriptions are deleted
    When the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage

  Scenario: Retrieving an empty subscriptions list
    Given that reaper   is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
    When a get all subscriptions request is made
    Then the returned list of subscriptions is empty
    When the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage

  Scenario: Retrieving an empty subscriptions list by existing cluster name
    Given that reaper   is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
    When a get-subscriptions request is made for cluster "test"
    Then the returned list of subscriptions is empty
    When the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage

  Scenario: Retrieve subscription for non existing cluster name
    Given that reaper   is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
    When a get all subscriptions request is made
    Then the returned list of subscriptions is empty
    And the following subscriptions are created:
      | clusterName | description  | nodes       | events                                        | exportSse | exportFileLogger | exportHttpEndpoint |
      | test        | Test Cluster | 127.0.0.2   | AuditEvent,BootstrapEvent                     | true      |                  |                    |
    When a get-subscriptions request is made for cluster "test_cluster_non_existing"
    Then the returned list of subscriptions is empty
    And all created subscriptions are deleted
    When the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage

  Scenario: Retrieve subscriptions for existing cluster name
    Given that reaper   is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
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
    When the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage

  Scenario: Retrieve last inserted subscription by ID
    Given that reaper   is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
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
    When the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage

  Scenario: Add duplicate subscription
    Given that reaper   is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
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
    When the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage

  Scenario: Add duplicate subscriptions
    Given that reaper   is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
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
    When the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage

  Scenario: Retrieve all subscriptions
    Given that reaper   is running
    And reaper has no cluster in storage
    When an add-cluster request is made to reaper
    Then reaper has the last added cluster in storage
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
    When the last added cluster is deleted
    Then reaper has no longer the last added cluster in storage
