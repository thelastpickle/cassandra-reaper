+++
[menu.docs]
name = "Backends"
identifier = "backends"
weight = 10
+++

# Backends

Cassandra Reaper can be used with either an ephemeral memory storage or persistent database.

For persistent relational database storage, you must either setup PostgreSQL or H2. You also need to set `storageType: database` in the config file.

Reaper provides a number of backend storage options:

* [In-Memory]({{<ref "memory.md" >}})
* [Cassanda]({{<ref "cassandra.md">}})
* [PostgresQL]({{<ref "postgres.md">}})
* [H2]({{<ref "h2.md">}})

Sample yaml files are available in the `src/packaging/resource` directory for each of the above storage backends:



* cassandra-reaper-memory.yaml
* cassandra-reaper-cassandra.yaml
* cassandra-reaper-postgres.yaml
* cassandra-reaper-h2.yaml

For configuring other aspects of the service, see the available configuration options in the [Configuration Reference](../configuration)
