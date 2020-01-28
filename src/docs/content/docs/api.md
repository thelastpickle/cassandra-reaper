+++
[menu.docs]
name = "Rest API"
weight = 75
+++

# Rest API


Source code for all the REST resources can be found from package io.cassandrareaper.resources.

## Login Resource

* **POST     /login**
  * Expected form parameters:  
  		* *username*: User to login with as defined in Shiro settings (default user is **admin**)
  		* *password*: Password to authenticate with through Shiro (default password of user *admin* is: **admin**)
  		* *rememberMe*: Boolean to have the Web UI remember the username
  * Endpoint for logging in to Reaper

## Shiro JWT Provider
 
* **GET     /jwt**
  * Expected query parameters: *None*
  * Returns a JWT to use in all REST calls when authentication is turned on in Reaper. 
The token must be passed in the `Authorization` HTTP header in the following form:
```
Authorization: Bearer [JWT value]
```  
This operation expects that a call was previously made to `/login` and that the retrieved session id is passed in the cookies when requesting a JWT.

## Ping Resource

* **GET     /ping**
  * Expected query parameters: *None*
  * Simple ping resource that can be used to check whether the reaper is running.

## Cluster Resource

* **GET     /cluster**
  * Expected query parameters:
      * *seedHost*: Limit the returned cluster list based on the given seed host. (Optional)
  * Returns a list of registered cluster names in the service.
  
  
* **GET     /cluster/{cluster_name}**
  * Expected query parameters:
    * *limit*: Limit the number of repair runs returned. Recent runs are prioritized. (Optional)
  * Returns a cluster object identified by the given "cluster_name" path parameter.
  
  
* **GET     /cluster/{cluster_name}/tables**
  * Expected query parameters: *None*
  * Returns a map of `<KeyspaceName, List<TableName>>`
  
  
* **POST    /cluster**
  * Expected query parameters:
      * *seedHost*: Host name or IP address of the added Cassandra
        clusters seed host.
      * *jmxPort*: A custom port that reaper will use to JMX into the Cluster.  Defaults to 7199.  (Optional)
      * *jmxUsername*: JMX Username specific to the Cluster.  Defaults to what is defined in [configuration](http://cassandra-reaper.io/docs/configuration/reaper_specific/).  (Optional)
      * *jmxUsername*: JMX Password specific to the Cluster.  Defaults to what is defined in [configuration](http://cassandra-reaper.io/docs/configuration/reaper_specific/).  (Optional)
  * Adds a new cluster to the service, and returns the newly added cluster object, if the operation was successful. If the cluster is already registered, the list of host will be updated to match the current topology.
  
  
* **PUT     /cluster/{cluster_name}**
  * Expected query parameters:
      * *seedHost*: New host name or IP address used as Cassandra cluster seed.
      * *jmxPort*: A custom port that reaper will use to JMX into the Cluster.  Defaults to 7199.  (Optional)
      * *jmxUsername*: JMX Username specific to the Cluster.  Defaults to what is defined in [configuration](http://cassandra-reaper.io/docs/configuration/reaper_specific/).  (Optional)
      * *jmxUsername*: JMX Password specific to the Cluster.  Defaults to what is defined in [configuration](http://cassandra-reaper.io/docs/configuration/reaper_specific/).  (Optional)
  * Adds a new cluster or modifies an existing cluster's seed host. Comes in handy when the previous seed has left the cluster.
  
  
* **DELETE  /cluster/{cluster_name}**
  * Expected query parameters:
      * *force* : Enforce deletion of the cluster even if there are active schedules and a repair run history (Optional)
  * Delete a cluster object identified by the given "cluster_name" path parameter.
    Cluster will get deleted only if there are no schedules or repair runs for the cluster,
    or the request will fail. Delete repair runs and schedules first before calling this.

## Repair Run Resource

* **GET     /repair_run**
  * Optional query parameters:
    	* *state*: Comma separated list of repair run state names. Only names found in `io.cassandrareaper.core.RunState` are accepted.
  * Returns a list of repair runs, optionally fetching only the ones with *state* state.
  
  
* **GET     /repair_run/{id}**
  * Expected query parameters: *None*
  * Returns a repair run object identified by the given "id" path parameter.
  
  
* **GET     /repair_run/cluster/{cluster_name}** 
  * Expected query parameters: *None*
  * Returns a list of all repair run statuses found for the given "cluster_name" path parameter.
  
  
* **GET     /repair_run/{id}/segments**
  * Expected query parameters: *None*
  * Returns the list of segments of the repair run.
  
  
* **POST     /repair_run/{id}/segments/abort/{segment_id}**
  * Expected query parameters: *None*
  * Aborts a running segment and puts it back in NOT_STARTED state. The segment will be processed again later during the lifetime of the repair run.
  
  
* **POST    /repair_run**
  * Expected query parameters:
	    * *clusterName*: Name of the Cassandra cluster.
	    * *keyspace*: The name of the table keyspace.
	    * *tables*: The name of the targeted tables (column families) as comma separated list.
	                If no tables given, then the whole keyspace is targeted. (Optional)
	    * *owner*: Owner name for the run. This could be any string identifying the owner.
	    * *cause*: Identifies the process, or cause the repair was started. (Optional)
	    * *segmentCount*: Defines the amount of segments per node to create for the repair run. (Optional)
	    * *repairParallelism*: Defines the used repair parallelism for repair run. (Optional)
	    * *intensity*: Defines the repair intensity for repair run. (Optional)
	    * *incrementalRepair*: Defines if incremental repair should be done. [true/false] (Optional)
	    * *nodes* : a specific list of nodes whose tokens should be repaired. (Optional)
	    * *datacenters* : a specific list of datacenters to repair. (Optional) 
	    * *blacklistedTables* : The name of the tables that should not be repaired. Cannot be used in conjunction with the tables parameter. (Optional)
	    * *repairThreadCount* : Since Cassandra 2.2, repairs can be performed with up to 4 threads in order to parallelize the work on different token ranges. (Optional)
    * Endpoint used to create a repair run. Does not allow triggering the run. 
Creating a repair run includes generating the repair segments. 
Notice that query parameter "tables" can be a single String, or a comma-separated list of table names. If the "tables" parameter is omitted, and only the keyspace is defined, then created repair run will target all the tables in the keyspace.
	* Returns the ID of the newly created repair run if successful.
  
  
* **PUT    /repair_run/{id}/state/{state}**
  * Expected query parameters: *None*
  * Starts, pauses, or resumes a repair run identified by the "id" path parameter.  
Can also be used to reattempt a repair run in state "ERROR", picking up where it left off.  
Possible values for given state are: "PAUSED" or "RUNNING".
  
  
* **PUT    /repair_run/{id}/intensity/{intensity}**
  * Expected query parameters:
  		* **intensityStr** : New value for the intensity of the repair run. Expected is a float between 0.0 and 1.0.
  * Modifies the intensity of a PAUSED repair run.
Returns OK if all goes well NOT_MODIFIED if new state is the same as the old one, and 409 (CONFLICT) if transition is not supported.
  
  
* **DELETE  /repair_run/{id}**
  * Expected query parameters:
    * *owner*: Owner name for the run. If the given owner does not match the stored owner,
               the delete request will fail.
  * Delete a repair run object identified by the given "id" path parameter.
    Repair run and all the related repair segments will be deleted from the database.

## Repair Schedule Resource

* **GET     /repair_schedule**
  * Expected query parameters:
      * *clusterName*: Filter the returned schedule list based on the given
        cluster name. (Optional)
      * *keyspaceName*: Filter the returned schedule list based on the given
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
 	    * *nodes* : a specific list of nodes whose tokens should be repaired. (Optional)
	    * *datacenters* : a specific list of datacenters to repair. (Optional) 
	    * *blacklistedTables* : The name of the tables that should not be repaired. Cannot be used in conjunction with the tables parameter. (Optional)
	    * *repairThreadCount* : Since Cassandra 2.2, repairs can be performed with up to 4 threads in order to parallelize the work on different token ranges. (Optional)
    * Create and activate a scheduled repair.
  
  
* **DELETE  /repair_schedule/{id}**
  * Expected query parameters:
    * *owner*: Owner name for the schedule. If the given owner does not match the stored owner,
               the delete request will fail.
  * Delete a repair schedule object identified by the given "id" path parameter.
    Repair schedule will get deleted only if there are no associated repair runs for the schedule.
    Delete all the related repair runs before calling this endpoint.
  
  
* **PUT /repair_schedule/{id}**
  * Expected query parameters: *None*
  * Returns the repair schedule objects for the given cluster.

  
## Snapshot Resource

* **GET /snapshot/{clusterName}/{host}**
  * Expected query parameters: *None*
  * Lists snapshots for the given host in the given cluster.
  
  
* **GET /snapshot/cluster/{clusterName}**
  * Expected query parameters: *None*
  * Lists all snapshots for the the given cluster.
  
  
* **DELETE /snapshot/{clusterName}/{host}/{snapshotName}**
  * Expected query parameters: *None*
  * Deletes a specific snapshot on a given node.
  
  
* **DELETE /snapshot/{clusterName}/{snapshotName}**
  * Expected query parameters: *None*
  * Deletes a specific snapshot on all nodes in a given cluster.
  
    
* **POST /snapshot/{clusterName}**
  * Expected query parameters:
	    * *keyspace*: Name of the Cassandra cluster.
	    * *tables*: The name of the targeted tables (column families) as comma separated list.
	                If no tables given, then the whole keyspace is targeted. (Optional)
	    * *snapshot_name*: name to use for the snapshot. (Optional)
	    * *owner*: Owner name for the run. This could be any string identifying the owner.
	    * *cause*: Identifies the process, or cause the repair was started. (Optional)
    * Create a snapshot on all hosts in a cluster, using the same name.
  
  
* **POST /snapshot/{clusterName}/{host}**
  * Expected query parameters:
	    * *keyspace*: Name of the Cassandra cluster.
	    * *tables*: The name of the targeted tables (column families) as comma separated list.
	                If no tables given, then the whole keyspace is targeted. (Optional)
	    * *snapshot_name*: name to use for the snapshot. (Optional)
    * Create a snapshot on a specific host.

## Crypto Resource

* **GET /crypto/encrypt/{text}**
  * Expected query parameters:
	    * *text*: The text to encrypt.
  * Encrypt text when cryptograph settings are configured in the reaper yaml.

