+++
title = "Adding a Cluster"
menutTitle = "Adding a Cluster"
weight = 1
identifier = "add_cluster"
parent = "usage"
+++


Enter an address of one of the nodes in the cluster, then click *Add Cluster* Reaper will contact that node and find the rest of the nodes in the cluster automatically.

{{< screenshot src="/img/add_cluster.png" />}}

<br/>

Once successfully completed, the Cluster's [health](../health) will be displayed.

<br/>

If JMX authentication is required and all clusters share the same credentials, they have to be filled in the Reaper YAML file, under `jmxAuth` (see the [configuration reference](../../configuration/reaper_specific)).
  

## Specific JMX credentials per cluster

_**Since 1.1.0**_

If the clusters require authentication for JMX access, credentials will need to be filled in the reaper yaml configuration file. See the `jmxCredentials` setting in the [configuration reference](../../configuration/reaper_specific) for detailed informations.

{{< screenshot src="/img/reaper_jmx_cred_add_cluster.png" />}}

When using `jmxCredentials` (if each cluster has specific credentials), the name of the cluster will need to be indicated in the seed node address.

