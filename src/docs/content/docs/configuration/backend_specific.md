---
title: "Backend Specific Settings"
parent: "configuration"
weight: 6
---

## Backend Specific Settings

Configuration settings in the *cassandra-reaper.yaml* that are specific to a particular backend.

### Cassandra Settings

The following settings are specific to a Reaper deployment that is backed by an Apache Cassandra database.
Note that Cassandra backend configuration relies on the [Dropwizard-Cassandra](https://github.com/composable-systems/dropwizard-cassandra) module.

</br>

#### `activateQueryLogger`

Type: *Boolean*

Default: *false*

Records the CQL calls made to the Cassandra backend in the log output.

</br>

#### `cassandra`

Settings to configure Reaper to use Cassandra for storage of its control data. Reaper uses the Cassandra Java driver version [4.17.0](http://docs.datastax.com/en/developer/java-driver/4.17/) to perform operations on the cluster. An example of the configuration settings for the driver are as follows.

```yaml
cassandra:
  type: basic
  sessionName: "test"
  contactPoints:
    - host: 127.0.0.1
      port: 9042
  sessionKeyspaceName: reaper_db
  loadBalancingPolicy:
    type: default
    localDataCenter: dc1
  retryPolicy:
    type: default
  schemaOptions:
    agreementIntervalMilliseconds: 2000
    agreementTimeoutSeconds: 10
    agreementWarnOnFailure: true
  requestOptionsFactory:
    requestTimeout: 20s
    requestDefaultIdempotence: true
  authProvider:
    type: plain-text
    username: cassandra
    password: cassandra
```



Definitions for some of the above sub-settings are as follows.

##### `sessionName`

Type: *String*

Name of the session to create

##### `contactPoints`

Type: *Array*

Seed nodes in the Cassandra cluster to contact, with their port each.
Can be provided as a comma separated list of objects:

```yaml
{"host": "host1", "port": "9042"}, {"host": "host2", "port": "9042"}
```

or as a list of objects:

```yaml
    - host: host1
      port: 9042
    - host: host2
      port: 9042
```

##### `sessionKeyspaceName`

Type: *String*

Name of the keyspace to store the Reaper control data.

##### `loadBalancingPolicy`

Settings to configure the policies used to generate the query plan which determines the nodes to connect to when performing query operations. Further information can be found in the Cassandra Java driver [Load balancing](https://docs.datastax.com/en/developer/java-driver/4.17/manual/core/load_balancing/index.html) section.

##### `type`

Type: *String*

The policy type used to contribute to the computation of the query plan.

##### `localDataCenter`

Type: *String*

Specifies the name of the datacenter closest to Reaper when using the `dcAwareRoundRobin` policy.

##### `authProvider`

If native protocol authentication is enabled on Cassandra,  settings configure Reaper to pass credentials to Cassandra when establishing a connection.

##### `username`

Type: *String*

Cassandra native protocol username.

##### `password`

Type: *String*

Cassandra native protocol password.

##### `requestTimeout`

Type: *Duration*
Default: *10s*

The timeout for requests to the Cassandra cluster.

##### Full Configuration Reference

See [here](https://github.com/dropwizard/dropwizard-cassandra?tab=readme-ov-file#usage-1) for more information.

```yaml
cassandra:
  type: basic
  sessionName: name
  sessionKeyspaceName: keyspace
  requestOptionsFactory:
    requestTimeout: 5s
    requestConsistency: local
    requestPageSize: 12
    requestSerialConsistency: local
    requestDefaultIdempotence: true
  metricsEnabled: true
  protocolVersion:
    type: default
    version: V5
  ssl:
    type: default
    cipherSuites: ["a", "b"]
    hostValidation: true
    keyStorePassword: keyStorePassword
    keyStorePath: keyStorePath
    trustStorePassword: trustStorePassword
    trustStorePath: trustStorePath
  compression: lz4
  contactPoints:
    - host: localhost
      port: 9041
  authProvider:
    type: plain-text
    username: admin
    password: hunter2
  retryPolicy:
    type: default
  speculativeExecutionPolicy:
    type: constant
    delay: 1s
    maxSpeculativeExecutions: 3
  poolingOptions:
    maxRequestsPerConnection: 5
    maxRemoteConnections: 10
    maxLocalConnections: 20
    heartbeatInterval: 5s
    connectionConnectTimeout: 10s
  addressTranslator:
    type: ec2-multi-region
  timestampGenerator:
    type: atomic
  reconnectionPolicyFactory:
    type: exponential
    baseConnectionDelay: 10s
    maxReconnectionDelay: 30s
  loadBalancingPolicy:
    type: default
    localDataCenter: local
    dataCenterFailoverAllowLocalConsistencyLevels: true
    slowAvoidance: true
    dcFailoverMaxNodesPerRemoteDc: 2
  cassandraOptions: # to add options which are not supported by default. Full list can be found at https://docs.datastax.com/en/developer/java-driver/4.11/manual/core/
    - type: long
      name: advanced.protocol.max-frame-length
      value: 12
  sessionMetrics:
    - continuous-cql-requests
  nodeMetrics:
    - bytes-sent
  schema:
    agreementIntervalMilliseconds: 200
    agreementTimeoutSeconds: 10
    agreementWarnOnFailure: true
```

### H2 or Postgres Database Settings

**Removed in v3.0.0**

The following settings are specific to a Reaper deployment that is backed by either a H2 or Postgres database. An example of the configuration settings for a Postgres database are as follows.

```yaml
postgres:
  url: jdbc:postgresql://127.0.0.1/reaper
  user: postgres
  password:
```

Definitions for the above sub-settings are as follows.

</br>

#### `h2`

Settings to configure Reaper to use H2 for storage of its control data.

#### `postgres`

Settings to configure Reaper to use Postgres for storage of its control data.

##### `driverClass`

Type: *String*

**WARNING** this setting is **DEPRECATED** and its usage should be avoided.

Specifies the driver to use to connect to the database.

##### `url`

Type: *String*

Specifies the URL to connect to the database (either H2 or Postgres) on.

##### `user`

Type: *String*

Database username.

##### `password`

Type: *String*

Database password.
