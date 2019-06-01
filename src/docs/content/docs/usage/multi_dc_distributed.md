+++
[menu.docs]
name = "Operating with Multi DC Cluster using Multiple Reaper instances"
weight = 50
identifier = "multi_dc"
parent = "usage"
+++


# Operating with a Multi DC Cluster using Multiple Reaper instances

Multiple Reaper instances can operate clusters which have multi datacenter deployment. Multiple Reaper instances, also known as Distributed mode, can only be used when using the Apache Cassandra backend. Using multiple Reaper instances allows improved availability and fault tolerance. It is more likely that a Reaper UI is available via one of the Reaper instances, and that scheduled repairs are executed by one of the running Reaper instances.

The `datacenterAvailability` setting in the Reaper YAML file indicates to Reaper its deployment in relation to cluster data center network locality.

## Multiple Reaper instances with JMX accessible for all DCs

In the case where the JMX port is accessible (with or without authentication) from the running Reaper instance for all nodes in all DCs, it is possible to have multiple instances of Reaper handle one or multiple clusters by using the following setting in the configuration yaml file :  

```
datacenterAvailability: ALL
```


{{< screenshot src="/img/singlereaper-multidc-all.png">}}

{{< /screenshot >}}

Reaper must be able to access the JMX port (7199 by default) and port 9042 if the cluster is also used as Cassandra backend, on the local DC.

## Multiple Reaper instances with JMX accessible for limited DCs

In the case where the JMX port is accessible (with or without authentication) from the running Reaper instance for all nodes in only some of the DCs, it is possible to have multiple instances of Reaper handle one or multiple clusters by using the following setting in the configuration yaml file :  

```
datacenterAvailability: LOCAL
```

Note, there is no backpressure for nodes in any datacenter if no Reaper instances have JMX access to that datacenter. This is because the JMX metrics (pending compactions, running repairs) required for backpressure is not available from those remote nodes.

If multiple clusters are registered in Reaper it is required that some Reaper instances can access all the nodes in at least one of the datacenters in each of the registered clusters.

`LOCAL` mode allows you to register multiple clusters in a distributed Reaper installation. `LOCAL` mode also allows you to prioritize repairs running according to their schedules over worrying about the load on remote and unaccessible datacenters and nodes.


{{< screenshot src="/img/singlereaper-multidc-local.png">}}

{{< /screenshot >}}

Reaper must be able to access the JMX port (7199 by default) and port 9042 if the cluster is also used as Cassandra backend, on the local DC.

Any keyspaces that only have replicas in remote JMX unreachable datacenters can not be repaired by Reaper.
  
  
## Multiple Reaper instances with JMX accessible locally to each DC

In the case where the JMX port is accessible (with or without authentication) from the running Reaper instance for all nodes in the current DC only, it is possible to have a multiple instances of Reaper running in different DCs by using the following setting in the configuration yaml file :  

```
datacenterAvailability: EACH
```

This setup prioritises handling backpressure on all nodes over running repairs. Where latency of requests and availability of nodes takes precedence over scheduled repairs this is the safest setup in Reaper. 

There must be installed and running a Reaper instance in every datacenter of every registered Cassandra cluster. And every Reaper instance must have CQL access to the backend Cassandra cluster it uses as a backend.

{{< screenshot src="/img/multireaper-multidc.png">}}

{{< /screenshot >}}

Reaper must be able to access the JMX port (7199 by default) and port 9042 if the cluster is also used as Cassandra backend, on the local DC.

