+++
[menu.docs]
name = "Sidecar Mode"
weight = 50
identifier = "sidecar_mode"
parent = "usage"
+++


# Sidecar Mode

Sidecar Mode is a way of deploying Cassandra Reaper with one reaper instance for each node in the cluster.
The name "Sidecar" comes from [the Sidecar Pattern](https://github.com/microsoftdocs/architecture-center/blob/master/docs/patterns/sidecar.md) which describes a mechanism for co-locating an auxiliary service with its supported application.
See also  [Design Patterns for Container-based Distributed Systems](https://www.usenix.org/conference/hotcloud16/workshop-program/presentation/burns).
It is a pattern that is often used in Kubernetes, where the main application and the sidecar application are deployed as separate containers in the same pod.

In Sidecar Mode, each Cassandra node process is deployed alongside a Reaper process; Cassandra is the parent application and Reaper is the sidecar.

## Advantages

 * *Security*: Each Reaper process only needs local JMX access. No need to configure remote JMX access and JMX authentication.
 * *Fault Tolerance*: Since there is one Reaper process per node, the Reaper services will be as resilient as the parent Cassandra cluster.
 * *Kubernetes Friendly*: Sidecar mode is very easy to setup if your Cassandra Cluster is deployed using a container orchestration system, such as Kubernetes.


## Guidance

 * *Deploy one Reaper cluster per Cassandra cluster*: Sidecar mode has been designed to allow you to easily deploy a separate, highly available Reaper service for each of your Cassandra clusters.
   You can use a Sidecar Mode Reaper service to manage multiple clusters, but this is not the use-case for which it has been designed and tested.
 * *Deploy Reaper in a sidecar container alongside Cassandra*: If you are using Kubernetes to deploy Cassandra and Reaper, put the Reaper process in the same Pod as Cassandra, in a sidecar container. See [Pods that run multiple containers that need to work together](https://kubernetes.io/docs/concepts/workloads/pods/pod-overview/)
 * *Use Spreaper and the REST API for Reaper administration (use the WebUI for reporting)*: If you have automated the deployment of Reeaper in Sidecar Mode alongside a Cassandra Cluster (described above), you can also [Preconfigure that cluster](https://github.com/thelastpickle/cassandra-reaper/pull/425) by writing a script that interacts with the Reaper REST API (or using spreaper) to automatically configure the repair settings for that cluster.
 * *Use or write an operator*: For example, you could write a [Kubernetes Custom Resource Definition](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/) (e.g. a `Cassandra` resource) which contains configuration fields for the Cassandra database and fields with which you can define the Reaper repair settings.
   Then write an accompanying [Kubernetes Operator](https://coreos.com/operators/) which watches for changes to those resources and reconciles the declared (desired) configuration with the actual state of the Cassandra cluster and its Reaper service.

## Caveats

 * *Cassandra Backend Only*: When operating in this mode, Reaper must be configured to use the [Cassandra Backend](../../backends/cassandra).
 * *One Reaper cluster per Cassandra Cluster*: Sidecar Mode is designed to be used in situations where you deploy a separate Reaper cluster for every Cassandra cluster.
   If you prefer to deploy a single Reaper cluster to manage multiple Cassandra clusters then Sidecar Mode may not be suitable for you.
 * *Container orchestration (Kubernetes) preferred*: Following on from the "one-reaper-per-cluster" caveat (above) it is also worth noting that it is easiest to deploy Reaper in Sidecar Mode if your Cassandra cluster is managed by a container orchestration system, such as Kubernetes.
   Kubernetes ensures that the Reaper process starts and dies with the Cassandra process.
   Kubernetes also ensures that the Reaper process shares a network namespace with the Cassandra process so that it (and only it) can access the JMX service.
   If you are not using Kubernetes (or similar) it may be overly complicated to deploy in a secure and resilient Sidecar Mode Reaper cluster.
 * *High Resource Usage*: In Sidecar Mode you will be deploying an additional Java process alongside each Cassandra process.
   If your underlying infrastructure has limited resources, then Sidecar Mode may not be suitable for you.
 * *WebUI Session Affinity*: In Sidecar Mode, you will be able to connect to the Web based administration pages of any Reaper process in the cluster.
    But even once you have logged into the Web UI of one Reaper process, you will not be able to log into the Web UI on other Reaper processes.
    The Web UI session information is not stored in the underlying Cassandra Backend database.
    You will need to setup a loadbalancer / service that ensures that all Web UI traffic from a particular client is directed to a particular Reaper process.
