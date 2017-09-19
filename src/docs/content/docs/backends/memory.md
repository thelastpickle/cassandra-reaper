+++
[menu.docs]
name = "In Memory"
parent = "backends"
weight = 1
+++

# Memory Backend

Running Reaper with memory storage, which is not persistent, means that all
the registered clusters, column families, and repair runs will be lost upon service restart.
The memory based storage is meant to be used for testing purposes only. Enable this type of storage by using the `storageType: memory` setting in your config file (enabled by default).

```yaml
storageType: memory
```