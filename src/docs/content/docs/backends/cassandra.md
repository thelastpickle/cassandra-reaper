+++
[menu.docs]
name = "Cassandra"
parent = "backends"
weight = 4
+++

# Cassandra Backend

To use Apache Cassandra as the persistent storage for Reaper, the `storageType` setting must be set to **cassandra** in the Reaper configuration YAML file. In addition, the connection details for the Apache Cassandra cluster being used to store Reaper data must be specified in the configuration YAML file. An example of how to configure H2 as persistent storage for Reaper can be found in the *[cassandra-reaper-cassandra.yaml](https://github.com/thelastpickle/cassandra-reaper/blob/master/src/packaging/resource/cassandra-reaper-cassandra.yaml)*.

```yaml
storageType: cassandra
cassandra:
  clusterName: "test"
  contactPoints: ["127.0.0.1"]
  keyspace: reaper_db
  queryOptions:
    consistencyLevel: LOCAL_QUORUM
    serialConsistencyLevel: SERIAL
```

If you're using authentication or SSL:

```yaml
cassandra:
  storageType: cassandra
  clusterName: "test"
  contactPoints: ["127.0.0.1"]
  keyspace: reaper_db
  authProvider:
    type: plainText
    username: cassandra
    password: cassandra
  ssl:
    type: jdk
```

The Apache Cassandra backend is the only one that allows running several Reaper instances at once. This provides high availability and allows to repair multi DC clusters.

To run Reaper using the Cassandra backend, create a reaper_db keyspace with an appropriate placement strategy. This is installation specific, and names of the data centers in the cluster that will host the Reaper data must be specified. For example:

```none
CREATE KEYSPACE reaper_db WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3};
```