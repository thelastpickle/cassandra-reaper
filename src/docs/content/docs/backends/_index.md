+++
[menu.docs]
name = "Backends"
identifier = "backends"
weight = 10
+++

# Backends

Cassandra Reaper can be used with either an ephemeral memory storage or persistent database. For persistent scalable database storage, a Cassandra cluster can be set up to back Reaper. To use a Cassandra cluster as the backed storage for Reaper set `storageType` to a value of **cassandra** in the Reaper configuration file. **Astra** can also be used as storage backend by setting `storageType` to a value of **astra**. With purge settings tuned appropriately, Reaper's workload should fit perfectly into the free tier of Astra.
Alternatively, a relational database storage; either H2 or Postgres can be set up to back Reaper. To use one of the relational database options as the backed storage for Reaper set `storageType` to a value of either **h2** or **postrges** in the Reaper configuration file.

Further information on the available storage options is provided in the following section.

* [In-Memory]({{<ref "memory.md" >}})
* [Cassanda]({{<ref "cassandra.md">}})
* [PostgresQL]({{<ref "postgres.md">}})
* [H2]({{<ref "h2.md">}})
* [Astra]({{<ref "astra.md">}})

Sample YAML files are available in the *[src/packaging/resource](https://github.com/thelastpickle/cassandra-reaper/tree/master/src/packaging/resource)* directory for each of the above storage options:

* cassandra-reaper-memory.yaml
* cassandra-reaper-cassandra.yaml
* cassandra-reaper-postgres.yaml
* cassandra-reaper-h2.yaml
* cassandra-reaper-astra.yaml

For configuring other aspects of the service, see the available configuration options in the [Configuration Reference](../configuration).
