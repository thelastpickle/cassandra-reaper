---
title: "Running a Cluster Repair"
weight: 30
identifier: "single"
parent: "usage"
---

Reaper has the ability to launch a once-off repair on a cluster. This can be done in the following way.

## Start a New Repair

Click the *repair* menu item on the left side to navigate to the Repair page. Click *Start a new repair* to open the repair details form.

{{< screenshot src="/img/repair_page.png" />}}

<br/>

## Fill in the Details

Enter values for the keyspace, tables, owner and other fields and click the *Repair* button. See the table below for further information on the details for each field.

{{< screenshot src="/img/single_repair.png" />}}

<br/>

<h4>Option</h4> | <h4>Description</h4>
---|---
**Cluster** | This field maps to a cluster as defined in the Cluster managment page.
**Keyspace** | Restricts the keyspaces that will be repaired by this task.
**Tables** | A comma-delimited list of tables to repair.
**Owner** | Any string is accepted for this field which acts as a way to include notes in cases where many users have access to Reaper.
**Segments per node** | The number of segments to create per nodes in the cluster.
**Parallelism** | Options are: <br/> - `Sequential`: Used in cases where data center aware repairs are not yet supported (pre-2.0.12). <br/> - `Parallel`: Executes repairs across all nodes in parallel. <br/> - `DC-Aware`: Executes repairs across all nodes in a data center, one data center at a time. This is the safest option as it restricts extreme load to a specific data center, rather than impacting the full cluster.
**Repair intensity** | A value between 0.0 and 1.0, where 1.0 ensures that no sleeping occurs between repair sessions and 0.5 ensures that equal amounts of time while the repair task is running, will be spent sleeping and repairing.
**Cause** | Any string is accepted for this field which acts as a way to include notes in cases where many users have access to Reaper.
**Incremental** | This boolean value is only supported when the Parallelism is set to Parallel.
**Nodes** |  Allows to restrict the repair to token ranges of specific nodes.
**Datacenters** |  Allows to restrict the repair to specific datacenters.
**Repair threads** |  (Since Cassandra 2.2) Allows to set a number of threads to process several token ranges concurrently.
