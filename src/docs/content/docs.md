# Reaper Documentation

## Downloads

See the [download](/download/) section for information on how to download and install reaper.


## Community

We have a [Mailing List](https://groups.google.com/forum/#!forum/tlp-apache-cassandra-reaper-users) and [Gitter chat](https://gitter.im/thelastpickle/cassandra-reaper) available.  


## Configuration

An example testing configuration YAML file can be found from within this project repository:
`src/test/resources/cassandra-reaper.yaml`

The configuration file structure is provided by Dropwizard, and help on configuring the server,
database connection, or logging, can be found at:

http://www.dropwizard.io/1.1.0/docs/manual/configuration.html

### Backends

Cassandra Reaper can be used with either an ephemeral memory storage or persistent database. 

For persistent relational database storage, you must either setup PostgreSQL or H2. You also need to set `storageType: database` in the config file.

Sample yaml files are available in the `resource` directory for each storage backend:  
* cassandra-reaper-memory.yaml
* cassandra-reaper-postgres.yaml
* cassandra-reaper-h2.yaml
* cassandra-reaper-cassandra.yaml

For configuring other aspects of the service, see the available configuration options in later section
of this page.

#### Memory Backend

Running Reaper with memory storage, which is not persistent, means that all
the registered clusters, column families, and repair runs will be lost upon service restart.
The memory based storage is meant to be used for testing purposes only. Enable this type of storage by using the `storageType: memory` setting in your config file (enabled by default).

```yaml
storageType: memory
```


#### H2 Backend

When using H2 storage the database will automatically created for you under the path configured in `cassandra-reaper.yaml`. Please
comment/uncomment the H2 settings and modify the path as needed or use the `cassandra-reaper-h2.yaml` as a base.

```yaml
storageType: database
database:
  # H2 JDBC settings
  driverClass: org.h2.Driver
  url: jdbc:h2:~/reaper-db/db;MODE=PostgreSQL
  user:
  password:
  
```

#### Postgres Backend

The schema will be initialized/upgraded automatically upon startup in the configured database.
Make sure to specify the correct credentials in your JDBC settings in `cassandra-reaper.yaml` to allow objects creation.


```yaml
storageType: database
database:
  # PostgreSQL JDBC settings
  driverClass: org.postgresql.Driver
  user: postgres
  password: 
  url: jdbc:postgresql://127.0.0.1/reaper
```

#### Cassandra Backend

For persistent Apache Cassandra storage, you need to set `storageType: cassandra` in the config file.
You'll also need to fill in the connection details to your Apache Cassandra cluster used to store the Reaper schema (reaper_db by default), in the `cassandra: ` section of the yaml file. 

```yaml
storageType: cassandra
cassandra:
  clusterName: "test"
  contactPoints: ["127.0.0.1"]
  keyspace: reaper_db
  queryOptions:
    consistencyLevel: LOCAL_QUORUM
    serialConsistencyLevel: SERIAL
```

When using SSL:

```yaml
cassandra:
  storageType: cassandra
  clusterName: "test"
  contactPoints: ["127.0.0.1"]
  keyspace: reaper_db
  authProvider:
    type: plainText
    username: cassandra
    password: cassandra
  ssl:
    type: jdk
```

The Apache Cassandra backend is the only one that allows running several Reaper instances at once. This provides high availability and allows to repair multi DC clusters (see the **Multi-DC** section below).

You will need to create a keyspace to store your data.  This is installation specific, you will need to fill in your own datacenter names.  For example:

```none
CREATE KEYSPACE reaper_db WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3};
```
    

### Other Configuration settings


The Reaper service specific configuration values are:

`segmentCount`:

Defines the default amount of repair segments to create for newly registered Cassandra
repair runs (token rings). When running a repair run by the Reaper, each segment is
repaired separately by the Reaper process, until all the segments in a token ring
are repaired. The count might be slightly off the defined value, as clusters residing
in multiple data centers require additional small token ranges in addition to the expected.
You can overwrite this value per repair run, when calling the Reaper.

`repairParallelism`:

Defines the default type of parallelism to use for repairs.
Repair parallelism value must be one of: "sequential", "parallel", or "datacenter_aware".
If you try to use "datacenter_aware" in clusters that don't support it yet (older than 2.0.12),
Reaper will fall back into using "sequential" for those clusters.

`repairIntensity`:

Repair intensity is a value between 0.0 and 1.0, but not zero. Repair intensity defines the amount of time to sleep between triggering each repair segment while running a repair run. When intensity is one, it means that Reaper doesn't sleep at all before triggering next segment, and otherwise the sleep time is defined by how much time it took to repair the last segment divided by the intensity value. 0.5 means half of the time is spent sleeping, and half running. Intensity 0.75 means that 25% of the total time is used sleeping and 75% running. This value can also be overwritten per repair run when invoking repairs.

`incrementalRepair`:

Incremental repair is a boolean value (true | false). Note that this is only supported with the PARALLEL repairParallelism setting. For more details in incremental repair, please refer to the following article.http://www.datastax.com/dev/blog/more-efficient-repairs

*Note*: We do not currently recommend using incremental repair before Cassandra 4.0 as subtle bugs can lead to overstreaming and cluster instabililty.

`repairRunThreadCount`:

The amount of threads to use for handling the Reaper tasks. Have this big enough not to cause
blocking in cause some thread is waiting for I/O, like calling a Cassandra cluster through JMX.

`hangingRepairTimeoutMins`:

The amount of time in minutes to wait for a single repair to finish. If this timeout is reached,
the repair segment in question will be cancelled, if possible, and then scheduled for later
repair again within the same repair run process.

`scheduleDaysBetween`:

Defines the amount of days to wait between scheduling new repairs.
The value configured here is the default for new repair schedules, but you can also
define it separately for each new schedule. Using value 0 for continuous repairs
is also supported.

`storageType`:

Whether to use database or memory based storage for storing the system state.
Value can be either "memory", "database" or "cassandra".
If you are using the recommended (persistent) storage type "database" or "cassandra", you need to define
the database client parameters in a database/cassandra section in the configuration file. See the example
settings in provided testing configuration in *src/test/resources/cassandra-reaper.yaml*.

`jmxPorts`:

Optional mapping of custom JMX ports to use for individual hosts. The used default JMX port
value is 7199. [CCM](https://github.com/pcmanus/ccm) users will find IP and port number
to add in `~/.ccm/<cluster>/*/node.conf` or by running `ccm <node> show`.

`jmxAuth`:

Optional setting for giving username and password credentials for the used JMX connections
in case you are using password based JMX authentication with your Cassandra clusters.

```
jmxAuth:
  username: cassandra
  password: cassandra
```

`enableCrossOrigin`:

Optional setting which you can set to be "true", if you wish to enable the CORS headers
for running an external GUI application, like [this project](https://github.com/spodkowinski/cassandra-reaper-ui).

`autoScheduling`:

Optional setting to automatically setup repair schedules for all non-system keyspaces in a cluster.
If enabled, adding a new cluster will automatically setup a schedule repair 
for each keyspace. Cluster keyspaces are monitored based on a configurable frequency,
so that adding or removing a keyspace will result in adding / removing the corresponding scheduled repairs.

`repairManagerSchedulingIntervalSeconds`:  

Controls the pace at which the Repair Manager will schedule processing of the next segment. Reducing this value from 30s (default) to a lower value can speed up fast repairs by orders of magnitude.
 
`jmxConnectionTimeoutInSeconds`:  

Controls the timeout for establishing JMX connections. The value should be low enough to avoid stalling simple operations in multi region clusters, but high enough to allow connections under normal conditions.  
The default is set to 5 seconds.
  
Notice that in the *server* section of the configuration, if you want to bind the service
to all interfaces, use value "0.0.0.0", or just leave the *bindHost* line away completely.
Using "*" as bind value won't work.  


## Multi-DC 

For security reasons, it is possible that Reaper will be able to access only a single DC nodes through JMX (multi region clusters for example).
In the case where the JMX port is accessible (with or without authentication) from the running Reaper instance only to the nodes in the current DC, it is possible to have a multiple instances of Reaper running in different DCs.

This setup works **with Apache Cassandra as a backend only**. It is unsuitable for memory, H2 and Postgres.

Reaper instances will rely on lightweight transactions to get leadership on segments before processing them.
Reaper checks the number of pending compactions and actively running repairs on all replicas before processing a segment. The `datacenterAvailability` setting in the yaml file controls the behavior for metrics collection :  

* `datacenterAvailability: ALL` requires direct JMX access to all nodes across all datacenters.
* `datacenterAvailability: LOCAL` requires jmx access to all nodes in the datacenter local to reaper. If Reaper instances exist in remote datacenters, metrics can be collected asynchronously through the Cassandra storage (not mandatory).
* `datacenterAvailability: EACH` means each datacenter requires at minimum one reaper instance which has jmx access to all nodes within that datacenter. Metrics from nodes in remote datacenters must be collected through the Cassandra storage backend. If all metrics aren't available, the segment will be postponed for later processing.


## Rest API


Source code for all the REST resources can be found from package com.spotify.reaper.resources.

## Ping Resource

* GET     /ping
  * Expected query parameters: *None*
  * Simple ping resource that can be used to check whether the reaper is running.

## Cluster Resource

* GET     /cluster
  * Expected query parameters:
      * *seedHost*: Limit the returned cluster list based on the given seed host. (Optional)
  * Returns a list of registered cluster names in the service.

* GET     /cluster/{cluster_name}
  * Expected query parameters:
    * *limit*: Limit the number of repair runs returned. Recent runs are prioritized. (Optional)
  * Returns a cluster object identified by the given "cluster_name" path parameter.

* POST    /cluster
  * Expected query parameters:
      * *seedHost*: Host name or IP address of the added Cassandra
        clusters seed host.
  * Adds a new cluster to the service, and returns the newly added cluster object,
    if the operation was successful.

* PUT     /cluster/{cluster_name}
  * Expected query parameters:
      * *seedHost*: New host name or IP address used as Cassandra cluster seed.
  * Modifies a cluster's seed host. Comes in handy when the previous seed has left the cluster.

* DELETE  /cluster/{cluster_name}
  * Expected query parameters: *None*
  * Delete a cluster object identified by the given "cluster_name" path parameter.
    Cluster will get deleted only if there are no schedules or repair runs for the cluster,
    or the request will fail. Delete repair runs and schedules first before calling this.

## Repair Run Resource

* GET     /repair_run
  * Optional query parameters:
    * *state*: Comma separated list of repair run state names. Only names found in
    com.spotify.reaper.core.RunState are accepted.
  * Returns a list of repair runs, optionally fetching only the ones with *state* state.

* GET     /repair_run/{id}
  * Expected query parameters: *None*
  * Returns a repair run object identified by the given "id" path parameter.

* GET     /repair_run/cluster/{cluster_name} (com.spotify.reaper.resources.RepairRunResource)
  * Expected query parameters: *None*
  * Returns a list of all repair run statuses found for the given "cluster_name" path parameter.

* POST    /repair_run
  * Expected query parameters:
    * *clusterName*: Name of the Cassandra cluster.
    * *keyspace*: The name of the table keyspace.
    * *tables*: The name of the targeted tables (column families) as comma separated list.
                If no tables given, then the whole keyspace is targeted. (Optional)
    * *owner*: Owner name for the run. This could be any string identifying the owner.
    * *cause*: Identifies the process, or cause the repair was started. (Optional)
    * *segmentCount*: Defines the amount of segments to create for repair run. (Optional)
    * *repairParallelism*: Defines the used repair parallelism for repair run. (Optional)
    * *intensity*: Defines the repair intensity for repair run. (Optional)
    * *incrementalRepair*: Defines if incremental repair should be done. [true/false] (Optional)

* PUT    /repair_run/{id}
  * Expected query parameters:
    * *state*: New value for the state of the repair run.
      Possible values for given state are: "PAUSED" or "RUNNING".
  * Starts, pauses, or resumes a repair run identified by the "id" path parameter.
  * Can also be used to reattempt a repair run in state "ERROR", picking up where it left off.

* DELETE  /repair_run/{id}
  * Expected query parameters:
    * *owner*: Owner name for the run. If the given owner does not match the stored owner,
               the delete request will fail.
  * Delete a repair run object identified by the given "id" path parameter.
    Repair run and all the related repair segments will be deleted from the database.

## Repair Schedule Resource

* GET     /repair_schedule
  * Expected query parameters:
      * *clusterName*: Filter the returned schedule list based on the given
        cluster name. (Optional)
      * *keyspaceName*: Filter the returned schedule list based on the given
        keyspace name. (Optional)
  * Returns all repair schedules present in the Reaper

* GET     /repair_schedule/{id}
  * Expected query parameters: *None*
  * Returns a repair schedule object identified by the given "id" path parameter.

* POST    /repair_schedule
  * Expected query parameters:
    * *clusterName*: Name of the Cassandra cluster.
    * *keyspace*: The name of the table keyspace.
    * *tables*: The name of the targeted tables (column families) as comma separated list.
                If no tables given, then the whole keyspace is targeted. (Optional)
    * *owner*: Owner name for the schedule. This could be any string identifying the owner.
    * *segmentCount*: Defines the amount of segments to create for scheduled repair runs. (Optional)
    * *repairParallelism*: Defines the used repair parallelism for scheduled repair runs. (Optional)
    * *intensity*: Defines the repair intensity for scheduled repair runs. (Optional)
    * *incrementalRepair*: Defines if incremental repair should be done. [true/false] (Optional)
    * *scheduleDaysBetween*: Defines the amount of days to wait between scheduling new repairs.
                             For example, use value 7 for weekly schedule, and 0 for continuous.
    * *scheduleTriggerTime*: Defines the time for first scheduled trigger for the run.
                             If you don't give this value, it will be next mid-night (UTC).
                             Give date values in ISO format, e.g. "2015-02-11T01:00:00". (Optional)

* DELETE  /repair_schedule/{id}
  * Expected query parameters:
    * *owner*: Owner name for the schedule. If the given owner does not match the stored owner,
               the delete request will fail.
  * Delete a repair schedule object identified by the given "id" path parameter.
    Repair schedule will get deleted only if there are no associated repair runs for the schedule.
    Delete all the related repair runs before calling this endpoint.


## Running through Docker


### Build Reaper Docker Image

First, build the Docker image and add it to your local image cache using the
`cassandra-reaper:latest` tag:

```mvn clean package docker:build```

### Start Docker Environment

First, start the Cassandra cluster:

```docker-compose up cassandra```

You can use the `nodetool` Docker Compose service to check on the Cassandra
node's status:

```docker-compose run nodetool status```

Once the Cassandra node is online and accepting CQL connections,
create the required `reaper_db` Cassandra keyspace to allow Reaper to save
its cluster and scheduling data.

By default, the `reaper_db` keyspace is created using a replication factor
of 1. To change this replication factor, provide the intended replication
factor as an optional argument:

```docker-compose run initialize-reaper_db [$REPLICATION_FACTOR]```

Wait a few moments for the `reaper_db` schema change to propagate,
then start Reaper:

```docker-compose up reaper```


### Access The Environment

Once started, the UI can be accessed through:

http://127.0.0.1:8080/webui/

When adding the Cassandra node to the Reaper UI, use the IP address found via:

```docker-compose run nodetool status```

The helper `cqlsh` Docker Compose service has also been included:

```docker-compose run cqlsh```

### Destroying the Docker Environment

When terminating the infrastructure, use the following command to stop
all related Docker Compose services:

```docker-compose down```

To completely clean up all persistent data, delete the `./data/` directory:

```rm -rf ./data/```
