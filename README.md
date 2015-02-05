cassandra-reaper
================

Cassandra Reaper is a centralized, stateful, and highly configurable tool for running Cassandra
repairs for multi-site clusters.


System Overview
---------------

Cassandra Reaper consists of a database containing the full state of the system, a REST-full API,
and a CLI tool called *spreaper* that provides an alternative way to issue commands to a running
Reaper instance. Communication with Cassandra nodes in registered clusters is handled through JMX.

Reaper system does not use internal caches for state changes regarding running repairs and
registered clusters, which means that any changes done to the storage will reflect to the running
system dynamically.

You can also run the Reaper with memory storage, which is not persistent, which means that all
the registered clusters, column families, and repair runs will be lost upon service restart.
The memory based storage is meant to be used for testing purposes only.

This project is built on top of Dropwizard:
http://dropwizard.io/


Usage
-----

To run Cassandra Reaper you need to simply build a project package using Maven, and
then execute the created Java jar file, and give a path to the system configuration file
as the first and only argument. You can also use the provided *bin/cassandra-reaper* script
to run the service.

For configuring the service, see the available configuration options in later section
of this readme document.

You can call the service directly through the REST API using a tool like *curl*. You can also
use the provided CLI tool in *bin/spreaper* to call the service.
Run the tool with *-h* or *--help* option to see usage instructions.

Notice that you can also build a Debian package from this project by using *debuild*, for example:
*debuild -uc -us -b*


Configuration
-------------

An example testing configuration YAML file can be found from within this project repository:
*src/test/resources/cassandra-reaper.yaml*

The configuration file structure is provided by Dropwizard, and help on configuring the server,
database connection, or logging, can be found from:
*http://dropwizard.io/manual/configuration.html*

The Reaper service specific configuration values are:

* segmentCount:

  Defines the default amount of repair segments to create for newly registered Cassandra
  repair runs (token rings). When running a repair run by the Reaper, each segment is
  repaired separately by the Reaper process, until all the segments in a token ring
  are repaired. The count might be slightly off the defined value, as clusters residing
  in multiple data centers require additional small token ranges in addition to the expected.
  You can overwrite this value per repair run, when calling the Reaper.

* repairParallelism:

  Defines the default type of parallelism to use for repairs.
  Repair parallelism value must be one of: "sequential", "parallel", or "datacenter_aware".
  If you try to use "datacenter_aware" in clusters that don't support it yet (older than 2.0.12),
  Reaper will fall back into using "sequential" for those clusters.

* repairIntensity:

  Repair intensity is a value between 0.0 and 1.0, but not zero. Repair intensity defines
  the amount of time to sleep between triggering each repair segment while running a repair run.
  When intensity is one, it means that Reaper doesn't sleep at all before triggering next segment,
  and otherwise the sleep time is defined by how much time it took to repair the last segment
  divided by the intensity value. 0.5 means half of the time is spent sleeping, and half running.
  Intensity 0.75 means that 25% of the total time is used sleeping and 75% running.
  This value can also be overwritten per repair run when invoking repairs.

* repairRunThreadCount:

  The amount of threads to use for handling the Reaper tasks. Have this big enough not to cause
  blocking in cause some thread is waiting for I/O, like calling a Cassandra cluster through JMX.

* hangingRepairTimeoutMins:

  The amount of time in minutes to wait for a single repair to finish. If this timeout is reached,
  the repair segment in question will be cancelled, if possible, and then scheduled for later
  repair again within the same repair run process.

* storageType:

  Whether to use database or memory based storage for storing the system state.
  Value can be either "memory" or "database".
  If you are using the recommended (persistent) storage type "database", you need to define
  the database client parameters in a database section in the configuration file. See the example
  settings in provided testing configuration in *src/test/resources/cassandra-reaper.yaml*.

Notice that in the *server* section of the configuration, if you want to bind the service
to all interfaces, use value "0.0.0.0", or just leave the *bindHost* line away completely.
Using "*" as bind value won't work.


REST API
--------

## Ping Resource

* GET     /ping (com.spotify.reaper.resources.PingResource)
  * Expected query parameters: *None*
  * Simple ping resource that can be used to check whether the reaper is running.

## Cluster Resource

* GET     /cluster (com.spotify.reaper.resources.ClusterResource)
  * Expected query parameters: *None*
  * Returns a list of registered cluster names in the service.

* GET     /cluster/{cluster_name} (com.spotify.reaper.resources.ClusterResource)
  * Expected query parameters: *None*
  * Returns a cluster resource identified by the given "cluster_name" path parameter.

* GET     /cluster/{cluster_name}/{keyspace_name} (com.spotify.reaper.resources.ClusterResource)
  * Expected query parameters: *None*
  * Returns keyspace information identified by the given "cluster_name" and
    "keyspace_name" path parameters.

* POST    /cluster (com.spotify.reaper.resources.ClusterResource)
  * Expected query parameters:
      * *seedHost*: Host name or IP address of the added Cassandra
        clusters seed host.
  * Adds a new cluster to the service, and returns the newly added cluster resource,
    if the operation was successful.

## Repair Run Resource

* GET     /repair_run/{id} (com.spotify.reaper.resources.RepairRunResource)
  * Expected query parameters: *None*
  * Returns a repair run resource identified by the given "id" path parameter.

* GET     /repair_run/cluster/{cluster_name} (com.spotify.reaper.resources.RepairRunResource)
  * Expected query parameters: *None*
  * Returns a list of all repair run statuses found for the given "cluster_name" path parameter.

* POST    /repair_run (com.spotify.reaper.resources.RepairRunResource)
  * Expected query parameters:
    * *clusterName*: Name of the Cassandra cluster.
    * *keyspace*: The name of the table keyspace.
    * *tables*: The name of the targeted tables (column families) as comma separated list.
                If no tables given, then the whole keyspace is targeted. (Optional)
    * *owner*: Owner name for the table. This could be any string identifying the owner.
    * *cause*: Identifies the process, or cause the repair was started. (Optional)
    * *segmentCount*: Defines the amount of segments to create for repair run. (Optional)
    * *repairParallelism*: Defines the used repair parallelism for repair run. (Optional)
    * *intensity*: Defines the repair intensity for repair run. (Optional)

* PUT    /repair_run/{id} (com.spotify.reaper.resources.RepairRunResource)
  * Expected query parameters:
    * *state*: New value for the state of the repair run.
      Possible values for given state are: "PAUSED" or "RUNNING".
  * Starts, pauses, or resumes a repair run identified by the "id" path parameter.
