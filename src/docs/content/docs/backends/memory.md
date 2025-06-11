---
title: "Local Backend"
weight: 1
---

To use in memory storage as the storage type for Reaper, the `storageType` setting must be set to **memory** in the Reaper configuration YAML file. Note that the in memory storage is enabled by default. An example of how to configure Reaper with In-Menory storage can be found in the *[cassandra-reaper-memory.yaml](https://github.com/thelastpickle/cassandra-reaper/blob/master/src/packaging/resource/cassandra-reaper-memory.yaml)*.

```yaml
storageType: memory
persistenceStoragePath: /var/lib/cassandra-reaper/storage
```

In-memory storage is volatile and as such all registered cluster, column families and repair information will be lost upon service restart. This storage setting is intended for testing purposes only.

> Starting from 3.6.0, persistenceStoragePath is required for memory storage type. This enable lightweight deployments of Reaper, without requiring the use of a Cassandra database. It will store the data locally and reload them consistently upon startup.
