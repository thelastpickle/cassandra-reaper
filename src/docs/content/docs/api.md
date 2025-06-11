---
title: "Rest API"
weight: 75
---

Source code for all the REST resources can be found from package io.cassandrareaper.resources.

## Login Resource

* **POST     /login**
  * Expected form parameters:  
  		* **username** : User to login with as defined in authentication settings (default user is **admin**)  
  		* **password** : Password to authenticate with (default password of user *admin* is: **admin**)  
  		* **rememberMe** : Boolean to have the Web UI remember the username (Optional)  
  * Endpoint for logging in to Reaper. Returns a JWT token in the response that must be passed in the `Authorization` HTTP header for authenticated requests:
```
Authorization: Bearer [JWT value]
```

## Ping Resource

* **GET     /ping**
  * Expected query parameters: *None*
  * Simple ping resource that can be used to check whether the reaper is running.

* **HEAD    /ping**
  * Expected query parameters: *None*
  * Health check endpoint that returns HTTP 204 if healthy, 500 if not.

## Cluster Resource

* **GET     /cluster**
  * Expected query parameters:
      * **seedHost**: Limit the returned cluster list based on the given seed host. (Optional)
  * Returns a list of registered cluster names in the service.
  
  
* **GET     /cluster/{cluster_name}**
  * Expected query parameters:
    * **limit**: Limit the number of repair runs returned. Recent runs are prioritized. (Optional)
  * Returns a cluster object identified by the given "cluster_name" path parameter.
  
  
* **GET     /cluster/{cluster_name}/tables**
  * Expected query parameters: *None*
  * Returns a map of `<KeyspaceName, List<TableName>>`
  
  
* **POST    /cluster**
  * Expected query parameters:
      * **seedHost**: Host name or IP address of the added Cassandra clusters seed host.  
      * **jmxPort**: A custom port that reaper will use to JMX into the Cluster. Defaults to 7199. (Optional)  
  * Adds a new cluster to the service, and returns the newly added cluster object, if the operation was successful. If the cluster is already registered, the list of hosts will be updated to match the current topology.

* **POST    /cluster/auth**
  * Expected form parameters:
      * **seedHost**: Host name or IP address of the added Cassandra clusters seed host.
      * **jmxPort**: A custom port that reaper will use to JMX into the Cluster. Defaults to 7199. (Optional)
      * **jmxUsername**: JMX Username specific to the Cluster. Defaults to what is defined in [configuration](http://cassandra-reaper.io/docs/configuration/reaper_specific/). (Optional)
      * **jmxPassword**: JMX Password specific to the Cluster. Defaults to what is defined in [configuration](http://cassandra-reaper.io/docs/configuration/reaper_specific/). (Optional)
  * Adds a new cluster to the service, and returns the newly added cluster object, if the operation was successful. If the cluster is already registered, the list of hosts will be updated to match the current topology.
  
  
* **PUT     /cluster/{cluster_name}**
  * Expected query parameters:
      * **seedHost**: New host name or IP address used as Cassandra cluster seed.
      * **jmxPort**: A custom port that reaper will use to JMX into the Cluster. Defaults to 7199. (Optional)
  * Adds a new cluster or modifies an existing cluster's seed host. Comes in handy when the previous seed has left the cluster.
  
* **PUT     /cluster/auth/{cluster_name}**
  * Expected form parameters:
      * **seedHost**: New host name or IP address used as Cassandra cluster seed.
      * **jmxPort**: A custom port that reaper will use to JMX into the Cluster. Defaults to 7199. (Optional)
      * **jmxUsername**: JMX Username specific to the Cluster. Defaults to what is defined in [configuration](http://cassandra-reaper.io/docs/configuration/reaper_specific/). (Optional)
      * **jmxPassword**: JMX Password specific to the Cluster. Defaults to what is defined in [configuration](http://cassandra-reaper.io/docs/configuration/reaper_specific/). (Optional)
  * Adds a new cluster or modifies an existing cluster's seed host. Comes in handy when the previous seed has left the cluster.
  
* **DELETE  /cluster/{cluster_name}**
  * Expected query parameters:
      * *force*: Enforce deletion of the cluster even if there are active schedules and a repair run history (Optional)
  * Delete a cluster object identified by the given "cluster_name" path parameter.
    Cluster will get deleted only if there are no schedules or repair runs for the cluster,
    or the request will fail. Delete repair runs and schedules first before calling this.

## Repair Run Resource

* **GET     /repair_run**
  * Optional query parameters:
      * **state**: Comma separated list of repair run state names. Only names found in `io.cassandrareaper.core.RunState` are accepted. (Optional)
      * **cluster_name**: Name of the Cassandra cluster. (Optional)
      * **keyspace_name**: The name of the table keyspace. (Optional)
      * **limit**: Limit the number of repair runs returned. (Optional)
  * Returns a list of repair runs, optionally fetching only the ones with specified filters.
  
  
* **GET     /repair_run/{id}**
  * Expected query parameters: *None*
  * Returns a repair run object identified by the given "id" path parameter.
  
  
* **GET     /repair_run/cluster/{cluster_name}** 
  * Expected query parameters:
      * **limit**: Limit the number of repair runs returned. (Optional)
  * Returns a list of all repair run statuses found for the given "cluster_name" path parameter.
  
  
* **GET     /repair_run/{id}/segments**
  * Expected query parameters: *None*
  * Returns the list of segments of the repair run.
  
  
* **POST     /repair_run/{id}/segments/abort/{segment_id}**
  * Expected query parameters: *None*
  * Aborts a running segment and puts it back in NOT_STARTED state. The segment will be processed again later during the lifetime of the repair run.
  
  
* **POST    /repair_run**
  * Expected query parameters:
	    * **clusterName**: Name of the Cassandra cluster.
	    * **keyspace**: The name of the table keyspace.
	    * **tables**: The name of the targeted tables (column families) as comma separated list.
	                If no tables given, then the whole keyspace is targeted. (Optional)
	    * **owner**: Owner name for the run. This could be any string identifying the owner.
	    * **cause**: Identifies the process, or cause the repair was started. (Optional)
	    * **segmentCountPerNode**: Defines the amount of segments per node to create for the repair run. (Optional)
	    * **repairParallelism**: Defines the used repair parallelism for repair run. (Optional)
	    * **intensity**: Defines the repair intensity for repair run. (Optional)
	    * **incrementalRepair**: Defines if incremental repair should be done. [true/false] (Optional)
	    * **subrangeIncrementalRepair**: Defines if subrange incremental repair should be done. [true/false] (Optional)
	    * **nodes**: a specific list of nodes whose tokens should be repaired. (Optional)
	    * **datacenters**: a specific list of datacenters to repair. (Optional) 
	    * **blacklistedTables**: The name of the tables that should not be repaired. Cannot be used in conjunction with the tables parameter. (Optional)
	    * **repairThreadCount**: Since Cassandra 2.2, repairs can be performed with up to 4 threads in order to parallelize the work on different token ranges. (Optional)
	    * **force**: Force the repair even if there are validation errors. (Optional)
	    * **timeout**: Timeout in seconds for repair operations. (Optional)
    * Endpoint used to create a repair run. Does not allow triggering the run. 
Creating a repair run includes generating the repair segments. 
Notice that query parameter "tables" can be a single String, or a comma-separated list of table names. If the "tables" parameter is omitted, and only the keyspace is defined, then created repair run will target all the tables in the keyspace.
	* Returns the ID of the newly created repair run if successful.
  
  
* **PUT    /repair_run/{id}/state/{state}**
  * Expected query parameters: *None*
  * Starts, pauses, or resumes a repair run identified by the "id" path parameter.  
Can also be used to reattempt a repair run in state "ERROR", picking up where it left off.  
Possible values for given state are: "PAUSED", "RUNNING", or "ABORTED".
  
  
* **PUT    /repair_run/{id}/intensity/{intensity}**
  * Expected query parameters: *None*
  * Modifies the intensity of a PAUSED repair run.
Returns OK if all goes well NOT_MODIFIED if new state is the same as the old one, and 409 (CONFLICT) if transition is not supported.
  
  
* **DELETE  /repair_run/{id}**
  * Expected query parameters:
    * **owner**: Owner name for the run. If the given owner does not match the stored owner,
               the delete request will fail. (Optional)
  * Delete a repair run object identified by the given "id" path parameter.
    Repair run and all the related repair segments will be deleted from the database.

* **POST   /repair_run/purge**
  * Expected query parameters: *None*
  * Purge completed repair runs from the database based on configured retention policies.

## Repair Schedule Resource

* **GET     /repair_schedule**
  * Expected query parameters:
      * **clusterName**: Filter the returned schedule list based on the given
        cluster name. (Optional)
      * **keyspace**: Filter the returned schedule list based on the given
        keyspace name. (Optional)
  * Returns all repair schedules present in the Reaper
  
  
* **GET     /repair_schedule/{id}**
  * Expected query parameters: *None*
  * Returns a repair schedule object identified by the given "id" path parameter.
  
  
* **GET     /repair_schedule/cluster/{cluster_name}**
  * Expected query parameters: *None*
  * Returns the repair schedule objects for the given cluster.
  
  
* **POST    /repair_schedule**
  * Expected query parameters:
	    * **clusterName**: Name of the Cassandra cluster.
	    * **keyspace**: The name of the table keyspace.
	    * **tables***: The name of the targeted tables (column families) as comma separated list.
	                If no tables given, then the whole keyspace is targeted. (Optional)
	    * **owner**: Owner name for the schedule. This could be any string identifying the owner.
	    * **segmentCountPerNode**: Defines the amount of segments per node to create for scheduled repair runs. (Optional)
	    * **repairParallelism**: Defines the used repair parallelism for scheduled repair runs. (Optional)
	    * **intensity**: Defines the repair intensity for scheduled repair runs. (Optional)
	    * **incrementalRepair**: Defines if incremental repair should be done on all tokens of each node at once. [true/false] (Optional)
      * **subrangeIncrementalRepair***: Defines if incremental repair should be done in subrange mode, against discrete token ranges. [true/false] (Optional)
	    * **scheduleDaysBetween**: Defines the amount of days to wait between scheduling new repairs.
	                             For example, use value 7 for weekly schedule, and 0 for continuous.
	    * **scheduleTriggerTime**: Defines the time for first scheduled trigger for the run.
	                             If you don't give this value, it will be next mid-night (UTC).
	                             Give date values in ISO format, e.g. "2015-02-11T01:00:00". (Optional)
 	    * **nodes**: a specific list of nodes whose tokens should be repaired. (Optional)
	    * **datacenters**: a specific list of datacenters to repair. (Optional) 
	    * **blacklistedTables**: The name of the tables that should not be repaired. Cannot be used in conjunction with the tables parameter. (Optional)
	    * **repairThreadCount**: Since Cassandra 2.2, repairs can be performed with up to 4 threads in order to parallelize the work on different token ranges. (Optional)
	    * **force**: Force the repair even if there are validation errors. (Optional)
	    * **timeout**: Timeout in seconds for repair operations. (Optional)
	    * **adaptive**: Enable adaptive scheduling based on repair metrics. [true/false] (Optional)
	    * **percentUnrepairedThreshold**: Threshold of unrepaired percentage that triggers a repair. (Optional)
    * Create and activate a scheduled repair.

* **PATCH   /repair_schedule/{id}**
  * Expected JSON body with repair schedule parameters:
      * **owner**: Owner name for the schedule. (Optional)
      * **repairParallelism**: Repair parallelism setting. (Optional) 
      * **intensity**: Repair intensity value. (Optional)
      * **daysBetween**: Days between scheduled repairs. (Optional)
      * **segmentCountPerNode**: Number of segments per node. (Optional)
      * **adaptive**: Enable adaptive scheduling. (Optional)
      * **percentUnrepairedThreshold**: Percentage threshold for repairs. (Optional)
  * Update specific fields of an existing repair schedule.

* **POST    /repair_schedule/start/{id}**
  * Expected query parameters: *None*
  * Force start a repair from a schedule.
  
  
* **DELETE  /repair_schedule/{id}**
  * Expected query parameters:
    * **owner**: Owner name for the schedule. If the given owner does not match the stored owner,
               the delete request will fail. (Optional)
  * Delete a repair schedule object identified by the given "id" path parameter.
    Repair schedule will get deleted only if there are no associated repair runs for the schedule.
    Delete all the related repair runs before calling this endpoint.
  
  
* **PUT /repair_schedule/{id}**
  * Expected query parameters:
    * **state**: "PAUSED" or "ACTIVE".
  * Enables or disables the repair schedule.

* **GET /repair_schedule/{clusterName}/{id}/percent_repaired**
  * Expected query parameters: *None*
  * Returns percent repaired metrics for the specified repair schedule.

  
## Snapshot Resource

* **GET /snapshot/cluster/{clusterName}/{host}**
  * Expected query parameters: *None*
  * Lists snapshots for the given host in the given cluster.
  
  
* **GET /snapshot/cluster/{clusterName}**
  * Expected query parameters: *None*
  * Lists all snapshots for the given cluster.
  
  
* **DELETE /snapshot/cluster/{clusterName}/{host}/{snapshotName}**
  * Expected query parameters: *None*
  * Deletes a specific snapshot on a given node.
  
  
* **DELETE /snapshot/cluster/{clusterName}/{snapshotName}**
  * Expected query parameters: *None*
  * Deletes a specific snapshot on all nodes in a given cluster.
  
    
* **POST /snapshot/cluster/{clusterName}**
  * Expected query parameters:
	    * **keyspace**: Name of the keyspace to snapshot.
	    * **snapshot_name**: name to use for the snapshot. (Optional)
	    * **owner**: Owner name for the snapshot. This could be any string identifying the owner. (Optional)
	    * **cause**: Identifies the process, or cause the snapshot was created. (Optional)
    * Create a snapshot on all hosts in a cluster, using the same name.
  
  
* **POST /snapshot/cluster/{clusterName}/{host}**
  * Expected query parameters:
	    * **keyspace**: Name of the keyspace to snapshot.
	    * **tables**: The name of the targeted tables (column families) as comma separated list.
	                If no tables given, then the whole keyspace is targeted. (Optional)
	    * **snapshot_name**: name to use for the snapshot. (Optional)
    * Create a snapshot on a specific host.

## Crypto Resource

* **GET /crypto/encrypt/{text}**
  * Expected query parameters: *None*
  * Path parameter:
	    * **text**: The text to encrypt.
  * Encrypt text when cryptograph settings are configured in the reaper yaml.

## Node Resource

* **GET /node/tpstats/{clusterName}/{host}**
    * Expected query parameters: *None*
    * Returns thread pool stats for a node.


* **GET /node/dropped/{clusterName}/{host}**
    * Expected query parameters: *None*
    * Returns dropped messages stats for a node.


* **GET /node/clientRequestLatencies/{clusterName}/{host}**
    * Expected query parameters: *None*
    * Returns client request latencies for a node.


* **GET /node/compactions/{clusterName}/{host}**
    * Expected query parameters: *None*
    * Returns active compactions for a node.


* **GET /node/tokens/{clusterName}/{host}**
    * Expected query parameters: *None*
    * Returns the tokens owned by a node.

## Reaper Resource

* **GET /**
    * Expected query parameters: *None*
    * Returns basic information about the Reaper instance.
