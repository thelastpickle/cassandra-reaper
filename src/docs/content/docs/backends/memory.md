+++
[menu.docs]
name = "In Memory"
parent = "backends"
weight = 1
+++

# In-Memory Backend

To use in memory storage as the storage type for Reaper, the `storageType` setting must be set to **memory** in the Reaper configuration YAML file. Note that the in memory storage is enabled by default. An example of how to configure Reaper with In-Menory storage can be found in the *[cassandra-reaper-memory.yaml](https://github.com/thelastpickle/cassandra-reaper/blob/master/src/packaging/resource/cassandra-reaper-memory.yaml)*.

```yaml
storageType: memory
```

In-memory storage is volatile and as such all registered cluster, column families and repair information will be lost upon service restart. This storage setting is intended for testing purposes only.