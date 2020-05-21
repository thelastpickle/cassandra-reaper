+++
title = "Operating Multiple DCs with a Single Reaper"
menuTitle = "Multi DCs with One Reaper"
weight = 50
identifier = "multi_dc_single"
parent = "usage"
+++


Reaper can operate clusters which has a multi datacenter deployment. The `datacenterAvailability` setting in the Reaper YAML file indicates to Reaper its deployment in relation to cluster data center network locality.

## Single Reaper instance with JMX accessible for all DCs

In the case where the JMX port is accessible (with or without authentication) from the running Reaper instance for all nodes in all DCs, it is possible to have a single instance of Reaper handle one or multiple clusters by using the following setting in the configuration yaml file :  

```
datacenterAvailability: ALL
```

This setup works with all backends : Apache Cassandra, Memory, H2 and Postgres.


{{< screenshot src="/img/singlereaper-multidc-all.png">}}

{{< /screenshot >}}

Reaper must be able to access the JMX port (7199 by default) and port 9042 if the cluster is also used as Cassandra backend, on the local DC.

The keyspaces must be replicated using NetworkTopologyStrategy (NTS) and have replicas at least on the DC Reaper can access through JMX. Repairing the remote DC will be handled internally by Cassandra.

**Note : multiple instances of Reaper can be running at once with this setting only when using the Apache Cassandra backend. See distributed mode for more details.** 

## Single Reaper instance with JMX accessible for limited DCs

In the case where the JMX port is accessible (with or without authentication) from the running Reaper instance for all nodes in only some of the DCs, it is possible to have a single instance of Reaper handle one or multiple clusters by using the following setting in the configuration yaml file :  

```
datacenterAvailability: LOCAL
```

Be aware that this setup will not allow to handle backpressure for those remote DCs as JMX metrics (pending compactions, running repairs) from those remote nodes are not made available to Reaper.

If multiple clusters are registered in Reaper it is required that Reaper can access all nodes in at least one data center in each of the registered clusters.

This setup works with all backends : Apache Cassandra, Memory, H2 and Postgres.


{{< screenshot src="/img/singlereaper-multidc-local.png">}}

{{< /screenshot >}}

Reaper must be able to access the JMX port (7199 by default) and port 9042 if the cluster is also used as Cassandra backend, on the local DC.

The keyspaces must be replicated using NetworkTopologyStrategy (NTS) and have replicas at least on the DC Reaper can access through JMX. Repairing the remote DC will be handled internally by Cassandra.

**Note : multiple instances of Reaper can be running at once with this settings only using when the Apache Cassandra backend. See distributed mode for more details.** 
  



