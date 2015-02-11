Feature: Using Reaper to launch repairs

  Background:
    Given a reaper service is running

  Scenario: Registering a cluster
    Given that we are going to use "127.0.0.1" as cluster seed host
    And reaper has no cluster with name "testcluster" in storage
    When an add-cluster request is made to reaper
    Then reaper has a cluster called "testcluster" in storage

  Scenario: Registering a scheduled repair
    Given reaper has a cluster called "testcluster" in storage
    And reaper has no scheduled repairs for "testcluster"
    When a new daily repair schedule is added for "testcluster" and keyspace "testkeyspace"
    Then reaper has a cluster called "testcluster" in storage
    And reaper has scheduled repair for cluster called "testcluster"
