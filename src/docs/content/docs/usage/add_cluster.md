+++
[menu.docs]
name = "Add a Cluster"
weight = 1
identifier = "add_cluster"
parent = "usage"
+++


# Add A Cluster


{{< screenshot src="/img/add_cluster.png" title="Add a cluster for Reaper to manage repairs on.">}}
        
Enter an address of one of the nodes in the cluster.  Reaper will contact that node and find the rest of the nodes in the cluster automatically.

Once you've done this, you'll see the [Cluster Health](../health).

{{< /screenshot >}}

If JMX authentication is required and all clusters share the same credentials, they have to be filled in the Reaper yaml file, under `jmxAuth` (see the [configuration reference](../../configuration/reaper_specific))
  

## Specific JMX credentials per cluster

_**Since 1.1.0**_  
If the clusters require authentication for JMX access, credentials have to be filled in the reaper yaml configuration file. See the `jmxCredentials` setting in the [configuration reference](../../configuration/reaper_specific) for detailed informations.

{{< screenshot src="/img/reaper_jmx_cred_add_cluster.png" title="Add a cluster with specific JMX credentials.">}}

When using `jmxCredentials` (if you have specific credentials for each cluster), please indicate the name of the cluster in the seed node address

{{< /screenshot >}}



