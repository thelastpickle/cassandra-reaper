+++
[menu.docs]
name = "Sidecar Mode"
weight = 55
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
 * *Kubernetes Friendly*: Sidecar mode is very easy to setup if your Cassandra Cluster is deployed using a container orchestration system, such as Kubernetes.


## Guidance

 * *Deploy one Reaper cluster per Cassandra cluster*: Sidecar mode has been designed to allow you to easily deploy a separate, highly available Reaper service for each of your Cassandra clusters. You currently cannot use a Sidecar Mode Reaper service to manage multiple clusters.
 * *Deploy Reaper in a sidecar container alongside Cassandra*: If you are using Kubernetes to deploy Cassandra and Reaper, put the Reaper process in the same Pod as Cassandra, in a sidecar container. See [Pods that run multiple containers that need to work together](https://kubernetes.io/docs/concepts/workloads/pods/pod-overview/)
 * *Use Spreaper and the REST API for Reaper administration (use the WebUI for reporting)*: If you have automated the deployment of Reaper in Sidecar Mode alongside a Cassandra Cluster (described above), then the Reaper cluster can be controlled from any of the Reaper instances' UIs (but see the below caveat about logging into multiple UI instances). You can turn on auto-scheduling to fully automate Reaper setup.
 * *Use an off-the-shelf operator*: For example, the k8ssandra project offers an [operator](https://github.com/k8ssandra/reaper-operator) which allows the management of Reaper in a Cassandra cluster on Kubernetes.
 * *Write an operator*: You could also write a custom [Kubernetes Custom Resource Definition](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/) (e.g. a `Cassandra` resource) which contains configuration fields for the Cassandra database and fields with which you can define the Reaper repair settings.
   Then write an accompanying [Kubernetes Operator](https://coreos.com/operators/) which watches for changes to those resources and reconciles the declared (desired) configuration with the actual state of the Cassandra cluster and its Reaper service.

## Caveats

 * *One Reaper cluster per Cassandra Cluster*: Sidecar Mode is designed to be used in situations where you deploy a separate Reaper cluster for every Cassandra cluster.
   If you prefer to deploy a single Reaper cluster to manage multiple Cassandra clusters then Sidecar Mode may not be suitable for you.
 * *Container orchestration (Kubernetes) preferred*: Following on from the "one-reaper-per-cluster" caveat (above) it is also worth noting that it is easiest to deploy Reaper in Sidecar Mode if your Cassandra cluster is managed by a container orchestration system, such as Kubernetes.
   Kubernetes ensures that the Reaper process starts and dies with the Cassandra process.
   Kubernetes also ensures that the Reaper process shares a network namespace with the Cassandra process so that it (and only it) can access the JMX service.
 * *High Resource Usage*: In Sidecar Mode you will be deploying an additional Java process alongside each Cassandra process.
   If your underlying infrastructure has limited resources, then Sidecar Mode may not be suitable for you.
 * *WebUI Session Affinity*: In Sidecar Mode, you will be able to connect to the Web based administration pages of any Reaper process in the cluster.
    But even once you have logged into the Web UI of one Reaper process, you will not be able to log into the Web UI on other Reaper processes.
    The Web UI session information is not stored in the underlying Cassandra Backend database.
    You will need to setup a loadbalancer / service that ensures that all Web UI traffic from a particular client is directed to a particular Reaper process.
 * *Snapshot support*: In Sidecar Mode, snapshot are not supported as Reaper currently tries to connect to all nodes directly for that feature. Asynchronous orchestration of such tasks is planned for a future release.

