+++
[menu.docs]
name = "Multi DC"
weight = 50
identifier = "multi_dc"
parent = "usage"
+++


# Reaper with multi DC clusters

## Single Reaper instance with JMX accessible for all DCs

In the case where the JMX port is accessible (with or without authentication) from the running Reaper instance for all nodes in all DCs, it is possible to have a single instance of Reaper handle the whole cluster by using the following setting in the configuration yaml file :  

```
datacenterAvailability: ALL
```

This setup works with all backends : Apache Cassandra, Memory, H2 and Postgres.


{{< screenshot src="/img/singlereaper-multidc-all.png">}}

{{< /screenshot >}}

Reaper must be able to access the JMX port (7199 by default) and port 9042 if the cluster is also used as Cassandra backend, on the local DC.

The keyspaces must be replicated using NetworkTopologyStrategy (NTS) and have replicas at least on the DC Reaper can access through JMX. Repairing the remote DC will be handled internally by Cassandra.

**Note : multiple instances of Reaper can be running at once with this setting only when using the Apache Cassandra backend.** 

## Single Reaper instance with JMX accessible for a single DC only

In the case where the JMX port is accessible (with or without authentication) from the running Reaper instance for all nodes in the current DC only, it is possible to have a single instance of Reaper handle the whole cluster by using the following setting in the configuration yaml file :  

```
datacenterAvailability: LOCAL
```

Be aware that this setup will not allow to handle backpressure for the remote DC as metrics (pending compactions, running repairs) will not be accessible from Reaper.

This setup works with all backends : Apache Cassandra, Memory, H2 and Postgres.


{{< screenshot src="/img/singlereaper-multidc-local.png">}}

{{< /screenshot >}}

Reaper must be able to access the JMX port (7199 by default) and port 9042 if the cluster is also used as Cassandra backend, on the local DC.

The keyspaces must be replicated using NetworkTopologyStrategy (NTS) and have replicas at least on the DC Reaper can access through JMX. Repairing the remote DC will be handled internally by Cassandra.

**Note : multiple instances of Reaper can be running at once with this settings only using when the Apache Cassandra backend.** 
  
  
## Multiple Reaper instances with JMX accessible for the local DC only

In the case where the JMX port is accessible (with or without authentication) from the running Reaper instance for all nodes in the current DC only, it is possible to have a multiple instances of Reaper running in different DCs by using the following setting in the configuration yaml file :  

```
datacenterAvailability: EACH
```


This setup works with Apache Cassandra as a backend only. It is unsuitable for memory, H2 and Postgres.

{{< screenshot src="/img/multireaper-multidc.png">}}

{{< /screenshot >}}

Reaper must be able to access the JMX port (7199 by default) and port 9042 if the cluster is also used as Cassandra backend, on the local DC.

**This is the safest setup for Reaper in multi DC clusters, as Reaper instances will have metrics from all replicas in all DCs available to handle backpressure.**  
It requires though that at least one Reaper instance should be up and running in each DC, otherwise segments will be postponed.

To avoid repairing the same tokens, Reaper instances will rely on lightweight transactions (LWT) to handle leader election for each segment.

Metrics for all nodes will be shared through a dedicated table in the Cassandra backend. This implies that the Reaper database must be installed on a multi DC cluster which spans over the same regions as the repaired cluster (if it is not the cluster itself), and the `reaper_db` keyspace must have replicas in all regions using `NetworkTopologyStrategy`.

More than one Reaper instance can run per DC, to allow fault tolerance.






