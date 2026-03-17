---
title: "Backends"
weight: 10
---

Reaper for Apache Cassandra can be used with either an ephemeral memory storage or persistent database. For persistent scalable database storage, a Cassandra cluster can be set up to back Reaper. To use a Cassandra cluster as the backed storage for Reaper set `storageType` to a value of **cassandra** in the Reaper configuration file.

Further information on the available storage options is provided in the following section.

* [Local storage]({{<ref "memory.md" >}})
* [Cassandra]({{<ref "cassandra.md">}})
Sample YAML files are available in the *[src/packaging/resource](https://github.com/thelastpickle/cassandra-reaper/tree/master/src/packaging/resource)* directory for each of the above storage options:

* cassandra-reaper-memory.yaml
* cassandra-reaper-cassandra.yaml

For configuring other aspects of the service, see the available configuration options in the [Configuration Reference](../configuration).

The Cassandra storage is most useful in cases where multiple Reaper instances are running against the same cluster. The database will be used as a coordination layer between instances.
The Local storage can only be used when a single Reaper instance is running against the cluster(s).
