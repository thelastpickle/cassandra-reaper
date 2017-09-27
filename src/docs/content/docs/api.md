+++
[menu.docs]
name = "Rest API"
weight = 75
+++

# Rest API


Source code for all the REST resources can be found from package io.cassandrareaper.resources.

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
    io.cassandrareaper.core.RunState are accepted.
  * Returns a list of repair runs, optionally fetching only the ones with *state* state.

* GET     /repair_run/{id}
  * Expected query parameters: *None*
  * Returns a repair run object identified by the given "id" path parameter.

* GET     /repair_run/cluster/{cluster_name} (io.cassandrareaper.resources.RepairRunResource)
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

