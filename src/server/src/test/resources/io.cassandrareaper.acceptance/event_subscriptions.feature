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


