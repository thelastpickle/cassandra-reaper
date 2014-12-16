cassandra-reaper
================

Cassandra Reaper is a centralized, stateful, and highly configurable tool for running Cassandra
repairs for multi-site clusters.


Overview
--------

Cassandra Reaper consists of a database containing the full state of the system, a REST-full API,
and a CLI tool called *spreaper* that provides an alternative way to issue commands to a running
Reaper instance. Communication with Cassandra clusters is handled through JMX to the given seed
node of each registered cluster.

You can also run the Reaper with memory storage, which is not persistent, and all the registered
clusters, column families, and repair runs will be lost upon service restart. Currently Reaper
supports only PostgreSQL database for persisting the state.

This project is built on top of Dropwizard:
http://dropwizard.io/


Usage
-----

To start the Reaper service providing the REST API, you need to configure the service in one
configuration file. Run the service by calling *bin/cassandra-reaper* command with one
argument, which is the path to the configuration file. See the available configuration options
in later section of this readme document.

You can call the service directly through the REST API using a tool like *curl*. More about
the REST API later.

You can also use the provided CLI tool in *bin/spreaper* to call the service. Run the tool with
*-h* or *--help* option to see usage instructions.


Configuration
-------------

An example testing configuration YAML file can be found from within this project repository:
*src/test/resources/cassandra-reaper.yaml*

The configuration file structure is provided by Dropwizard, and help on configuring the server,
database connection, or logging, can be found from:
*http://dropwizard.io/manual/configuration.html*

The Reaper service specific configuration values are:

* segmentCount:

  Defines the amount of repair segments to create for newly registered Cassandra column families
  (token rings). When running a repair run by the Reaper, each segment is repaired separately
  by the Reaper process, until all the segments in a token ring are repaired.

* snapshotRepair:

  Whether to use a snapshot repair by default when triggering repairs for repair segments.

* repairIntensity:

  Repair intensity is a value between 0.0 and 1.0, but not zero. Repair intensity defines
  the amount of time to sleep between triggering each repair segment while running a repair run.
  When intensity is one, it means that Reaper doesn't sleep at all before triggering next segment,
  and otherwise the sleep time is defined by how much time it took to repair the last segment
  divided by the intensity value. 0.5 means half of the time is spent sleeping, and half running.

* repairRunThreadCount:

  The amount of threads to use for handling the Reaper tasks. Have this big enough not to cause
  blocking in cause some thread is waiting for I/O, like calling a Cassandra cluster through JMX.


REST API
--------

TODO:

  GET     /ping (com.spotify.reaper.resources.PingResource)
  GET     /cluster (com.spotify.reaper.resources.ClusterResource)
  GET     /cluster/{name} (com.spotify.reaper.resources.ClusterResource)
  POST    /cluster (com.spotify.reaper.resources.ClusterResource)
  GET     /table/{clusterName}/{keyspace}/{table} (com.spotify.reaper.resources.TableResource)
  POST    /table (com.spotify.reaper.resources.TableResource)
  GET     /repair_run/{id} (com.spotify.reaper.resources.RepairRunResource)
