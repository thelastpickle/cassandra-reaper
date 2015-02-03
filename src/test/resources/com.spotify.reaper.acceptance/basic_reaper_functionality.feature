Feature: Using Reaper to launch repairs

  Background:
    Given a reaper service is running

  Scenario: Registering a cluster
    Given that we are going to use "127.0.0.1" as cluster seed host
    And reaper has no cluster with name "testcluster" in storage
    When an add-cluster request is made to reaper
    Then reaper has a cluster called "testcluster" in storage
