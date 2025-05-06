+++
[menu.docs]
name = "Reaper Specific Settings"
parent = "configuration"
weight = 4
+++

# Reaper Specific Settings

Configuration settings in the *cassandra-reaper.yaml* that are specific to Reaper

<br/>

### `autoScheduling`

Optional setting to automatically setup repair schedules for all non-system keyspaces in a cluster. If enabled, adding a new cluster will automatically setup a schedule repair  for each keyspace. Cluster keyspaces are monitored based on a configurable frequency, so that adding or removing a keyspace will result in adding / removing the corresponding scheduled repairs.

    autoScheduling:
      enabled: true
      initialDelayPeriod: PT15S
      periodBetweenPolls: PT10M
      timeBeforeFirstSchedule: PT5M
      scheduleSpreadPeriod: PT6H
      excludedKeyspaces: [myTTLKeyspace, ...]
      excludedClusters: [myCluster, ...]

Definitions for the above sub-settings are as follows.

#### `enabled`

Type: *Boolean*

Default: *false*

Enables or disables `autoScheduling`.

#### `initialDelayPeriod`

Type: *String*

Default: *PT15S* (15 seconds)

The amount of delay time before the schedule period starts.

#### `periodBetweenPolls`

Type: *String*

Default: *PT10M* (10 minutes)

The interval time to wait before checking whether to start a repair task.

#### `timeBeforeFirstSchedule`

Type: *String*

Default: *PT5M* (5 minutes)

Grace period before the first repair in the schedule is started.

#### `scheduleSpreadPeriod`

Type: *String*

Default: *PT6H* (6 hours)

The time spacing between each of the repair schedules that is to be carried out.

#### `excludedKeyspaces`

Type: *Array* (comma separated *Strings*)

The Keyspaces that are to be excluded from the repair schedule.

<br/>

#### `excludedClusters`

Type: *Array* (comma separated *Strings*)

The Clusters that are to be excluded from the repair schedule.

<br/>

### `datacenterAvailability`

Type: *String*

Default: *ALL*

Indicates to Reaper its deployment in relation to cluster data center network locality. The value must be either **ALL**, **LOCAL**, **EACH** or **SIDECAR**. Note that this setting controls the behavior for metrics collection.

By default, Apache Cassandra restricts JMX communications to localhost only. In this setup, only the **SIDECAR** value is suitable.

For security reasons, it is possible that Reaper will have access limited to nodes in a single datacenter via JMX (multi region clusters for example). In this case, it is possible to deploy an operate an instance of Reaper in each datacenter where each instance only has access via JMX (with or without authentication) to the nodes in its local datacenter. In addition, Reaper will check the number of pending compactions and actively running repairs on all replicas prior to processing a segment.

**ALL** - requires Reaper to have access via JMX to all nodes across all datacenters. In this mode Reaper can be backed by all available storage types.

**LOCAL** - requires Reaper to have access via JMX to all nodes only in the same datacenter local to Reaper. A single Reaper instance can operate in this mode and trigger repairs from within its local data center. In this case, can be backed by all available storage types and repairs to any remote datacenters are be handled internally by Cassandra. A Reaper instance can be deployed to each datacenter and be configured to operate in this mode. In this case, Reaper can use Apache Cassandra as its storage.

Further information can be found in the [Operating with a Multi DC Cluster](../../usage/multi_dc) section.

**EACH** - requires a minimum of one Reaper instance operating in each datacenter. Each Reaper instance is required to have access via JMX to all nodes only in its local datacenter. When operating in this mode, Reaper can use either of Apache Cassandra as its storage. In addition, metrics from nodes in remote datacenters must be collected through the storage backend. If any metric is unavailable, the segment will be postponed for later processing.

Further information can be found in the [Operating with a Multi DC Cluster](../../usage/multi_dc) section.

**SIDECAR** - requires one reaper instance for each node in the cluster.
Each Reaper instance is required to have access via JMX to its local node.
When operating in this mode, Reaper can use either of Apache Cassandra or Astra as its storage.

Further information can be found in the [Sidecar Mode](../../usage/sidecar_mode) section.

<br/>

### `enableCrossOrigin`

Type: *Boolean*

Default: *true*

Optional setting which can be used to enable the CORS headers for running an external GUI application, like [this project](https://github.com/spodkowinski/cassandra-reaper-ui). When enabled it will allow REST requests incoming from other origins than the domain that hosts Reaper.

<br/>

### `enableDynamicSeedList`

Type: *Boolean*

Default: *true*

Allow Reaper to add all nodes in the cluster as contact points when adding a new cluster, instead of just adding the provided node.

<br/>

### `hangingRepairTimeoutMins`

Type: *Integer*

The amount of time in minutes to wait for a single repair to finish. If this timeout is reached,
the repair segment in question will be cancelled, if possible, and then scheduled for later
repair again within the same repair run process.

<br/>

### `incrementalRepair`

Type: *Boolean*

Default: *false*

Sets the default repair type unless specifically defined for each run. Note that this is only supported with the PARALLEL repairParallelism setting. For more details in incremental repair, please refer to the following article.http://www.datastax.com/dev/blog/more-efficient-repairs

*Note*: It is recommended to avoid using incremental repair before Cassandra 4.0 as subtle bugs can lead to overstreaming and cluster instabililty.

<br/>

### `subrangeIncrementalRepair`

Type: *Boolean*

Default: *false*

Sets the default repair type unless specifically defined for each run. Note that this is only supported with the PARALLEL repairParallelism setting. For more details in incremental repair, please refer to the following article.http://www.datastax.com/dev/blog/more-efficient-repairs.
This mode will split the repair jobs into sets of token ranges using the incremental mode.
This will prevail over the `incrementalRepair` setting.


*Note*: Subrange incremental repair is only available since Cassandra 4.0.

<br/>

### `blacklistTwcsTables`

Type: *Boolean*

Default: *false*

Disables repairs of any tables that use either the `TimeWindowCompactionStrategy` or `DateTieredCompactionStrategy`. This automatic blacklisting is not stored in schedules or repairs. It is applied when repairs are triggered and visible in the UI for running repairs. Not storing which tables are TWCS/DTCS ensures changes to a table's compaction strategy are honored on every new repair.

*Note*: It is recommended to enable this option as repairing these tables, when they contain TTL'd data, causes overlaps between partitions across the configured time windows the sstables reside in. This leads to an increased disk usage as the older sstables are unable to be expired despite only containing TTL's data. Repairing DTCS tables has additional issues and is generally not recommended.

<br/>

### `jmxAuth`

Optional setting to allow Reaper to establish JMX connections to Cassandra clusters using password based JMX authentication.

    jmxAuth:
      username: cassandra
      password: cassandra

      #### `username`

#### `username`

Type: *String*

Cassandra JMX username.

#### `password`

Type: *String*

Cassandra JMX password.

<br/>

### `jmxCredentials`

_**Since 1.1.0**_
Optional setting to allow Reaper to establish JMX connections to Cassandra clusters with specific credentials per cluster.

    jmxCredentials:
      clusterProduction1:
        username: user1
        password: password1
      clusterProduction2:
        username: user2
        password: password2

This setting can be used in conjunction with the `jmxAuth` to override the credentials for specific clusters only.
The cluster name must match the one defined in the cassandra.yaml file (in the example above, `clusterProduction1` and `clusterProduction2`).

Adding a new cluster with specific credentials requires to add the seed node in the following format : `host@cluster`
To match the example above, it could be something like : `10.0.10.5@clusterProduction1`

When passed in as an environment variable, Reaper expects a comma-separated list of `user:password@cluster`.

### `jmxConnectionTimeoutInSeconds`

Type: *Integer*

Default: *20*

Controls the timeout for establishing JMX connections. The value should be low enough to avoid stalling simple operations in multi region clusters, but high enough to allow connections under normal conditions.

<br/>

### `jmxPorts`

Type: *Object*

Optional mapping of custom JMX ports to use for individual hosts. The used default JMX port value is 7199. [CCM](https://github.com/pcmanus/ccm) users will find IP and port number to add in `~/.ccm/<cluster>/*/node.conf` or by running `ccm <node> show`.

    jmxPorts:
      127.0.0.1: 7100
      127.0.0.2: 7200
      127.0.0.3: 7300

<br/>

### `logging`

Settings to configure Reaper logging.

    logging:
      level: INFO
      loggers:
        io.dropwizard: WARN
        org.eclipse.jetty: WARN
      appenders:
        - type: console
          logFormat: "%-6level [%d] [%t] %logger{5} - %msg %n"
          threshold: WARN
        - type: file
          logFormat: "%-6level [%t] %logger{5} - %msg %n"
          currentLogFilename: /var/log/cassandra-reaper/reaper.log
          archivedLogFilenamePattern: /var/log/cassandra-reaper/reaper-%d.log.gz
          archivedFileCount: 99

Definitions for some of the above sub-settings are as follows.

#### `level`

Type: *String*

Global log level to filter to. Where the level order is **ALL** < **DEBUG** < **INFO** < **WARN** < **ERROR** < **FATAL** < **OFF**. See the [log4j](https://logging.apache.org/log4j/1.2/apidocs/org/apache/log4j/Level.html) documentation for further information.

#### `loggers`

Type: *Object*

Key value pair containing the logger class name as the key and other sub-settings as its value.

#### `logFormat`

Type: *String*

The output format of an entry in the log.

#### `threshold`

Type: *String*

The log level to filter the console messages to. Where the level order is **ALL** < **DEBUG** < **INFO** < **WARN** < **ERROR** < **FATAL** < **OFF**. See the [log4j](https://logging.apache.org/log4j/1.2/apidocs/org/apache/log4j/Level.html) documentation for further information.

#### `archivedFileCount`

Type: *Integer*

The number of archive log files stored in the log rotation sliding window. That is the number of archived (compressed) log files kept at one point in time. If there are `archivedFileCount` number of archived log files and the current (uncompressed) log file is archived, the oldest archived log file is deleted.

<br/>

### `metrics`

Type: *Object*

Configuration parameters for sending metrics to a reporting system via the [Dropwizard interface](http://www.dropwizard.io/1.1.4/docs/manual/configuration.html#metrics). Reaper for Apache Cassandra ships the Graphite and DataDog reporters by default.

Further information on metrics configuration can be found in the [Metrics](../../metrics) section.

<br/>

### `repairIntensity`

Type: *Float* (value between 0.0 and 1.0, but must never be 0.0.)

Repair intensity defines the amount of time to sleep between triggering each repair segment while running a repair run. When intensity is 1.0, it means that Reaper doesn't sleep at all before triggering next segment, and otherwise the sleep time is defined by how much time it took to repair the last segment divided by the intensity value. 0.5 means half of the time is spent sleeping, and half running. Intensity 0.75 means that 25% of the total time is used sleeping and 75% running. This value can also be overwritten per repair run when invoking repairs.

<br/>

### `repairManagerSchedulingIntervalSeconds`

Type: *Integer*

Default: *30*

Controls the pace at which the Repair Manager will schedule processing of the next segment. Reducing this value from its default value of 30s to a lower value can speed up fast repairs by orders of magnitude.

<br/>

### `repairParallelism`

Type: *String*

Type of parallelism to apply by default to repair runs. The value must be either **SEQUENTIAL**, **PARALLEL**, or **DATACENTER_AWARE**.

**SEQUENTIAL** - one replica at a time, validation compaction performed on snapshots

**PARALLEL** - all replicas at the same time, no snapshot

**DATACENTER_AWARE** - one replica in each DC at the same time, with snapshots. If this value is used in clusters older than 2.0.12, Reaper will fall back into using **SEQUENTIAL** for those clusters.

<br/>

### `repairRunThreadCount`

Type: *Integer*

The amount of threads to use for handling the Reaper tasks. Have this big enough not to cause
blocking in cause some thread is waiting for I/O, like calling a Cassandra cluster through JMX.

<br/>

### `scheduleDaysBetween`

Type: *Integer*

Default: *7*

Defines the amount of days to wait between scheduling new repairs. The value configured here is the default for new repair schedules, but you can also define it separately for each new schedule. Using value 0 for continuous repairs is also supported.

<br/>

### `segmentCountPerNode`

Type: *Integer*

Default: *64*

Defines the default amount of repair segments to create for newly registered Cassandra repair runs, for each node in the cluster. When running a repair run by Reaper, each segment is repaired separately by the Reaper process, until all the segments in a token ring are repaired. The count might be slightly off the defined value, as clusters residing in multiple data centers require additional small token ranges in addition to the expected. This value can be overwritten when executing a repair run via Reaper.

In a 10 nodes cluster, setting a value of 20 segments per node will generate a repair run that splits the work into 200 token subranges. This number can vary due to vnodes (before 1.2.0, Reaper cannot create a segment with multiple token ranges, so the number of segments will be at least the total number of vnodes in the cluster). As Reaper tries to size segments evenly, the presence of very small token ranges can lead to have more segments than expected.

<br/>

### `server`

Settings to configure the application UI server.

    server:
      type: default
      applicationConnectors:
        - type: http
          port: 8080
          bindHost: 0.0.0.0
      adminConnectors:
        - type: http
          port: 8081
          bindHost: 0.0.0.0
      requestLog:
        appenders: []

Reaper provides two separate UIs; the application UI which is configured via the `applicationConnectors` settings and the administration UI which is configured via the `adminConnectors` settings. The application UI provides functionality to create/manage cluster schedules, and the administration UI provides tools for monitoring and debugging the Reaper system.

#### `port`

For the `applicationConnectors` this setting will be the port number used to access the application UI. For the `adminConnectors` this setting will be the port number to access the administration UI.

Note that the port numbers for each must be different values when bound to the same host.

#### `bindHost`

For the `applicationConnectors` this setting will be the host address used to access the application UI. For the `adminConnectors` this setting will be the host address used to access the administration UI.

Note that to bind the service to all interfaces use value **0.0.0.0** or leave the value for the setting this blank. A value of **\*** is an invalid value for this setting.

<br/>

### `storageType`

Type: *String*

The storage type to use in which Reaper will store its control data. The value must be either **cassandra**, **astra** or **memory**. If the recommended (persistent) storage type **cassandra**, or **astra** is being used, the database client parameters must be specified in the `cassandra` section in the configuration file. See the example settings in provided the *[src/packaging/resources](https://github.com/thelastpickle/cassandra-reaper/tree/master/src/packaging/resource)* directory of the repository.

<br/>

### `useAddressTranslator`

Type: *Boolean*

Default: *false*

When running multi region clusters in AWS, turn this setting to `true` in order to use the EC2MultiRegionAddressTranslator from the Datastax Java Driver. This will allow translating the public address that the nodes broadcast to the private IP address that is used to expose JMX.

<br/>

### `jmxAddressTranslator`

_**Since 2.1.0**_

Sometimes it’s not possible for Cassandra nodes to broadcast addresses that will work for each and every client; for instance, they might broadcast private IPs because most clients are in the same network, but a particular client could be on another network and go through a router. For such cases, you can configure a custom address translator that will perform additional address translation based on configured mapping.

```yaml
jmxAddressTranslator:
  type: multiIpPerNode
  ipTranslations:
    - from: "10.10.10.111"
      to: "node1.cassandra.reaper.io"
    - from: "10.10.10.112"
      to: "node2.cassandra.reaper.io"
```

When running multi region clusters in AWS, set type to `ec2MultiRegion` in order to use the EC2MultiRegionAddressTranslator from the Datastax Java Driver. This will allow translating the public address that the nodes broadcast to the private IP address that is used to expose JMX.

<br/>

### `accessControl`

Settings to activate and configure authentication for the web UI.
Deleting or commenting that block from the yaml file will turn off authentication.

```
accessControl:
  sessionTimeout: PT10M
  shiro:
    iniConfigs: ["file:/path/to/shiro.ini"]
```

### `repairThreadCount`

Type: *Integer*

Since Cassandra 2.2, repairs are multithreaded in order to process several token ranges concurrently and speed up the process.
This setting allows to set a default for automatic repair schedules.
No more than four threads are allowed by Cassandra.

### `maxPendingCompactions`

Type: *Integer*

Default: *20*

Reaper will prevent repair from overwhelming the cluster when lots of SSTables are streamed, by pausing segment processing if there are more than a specific number of pending compactions. Adjust this setting if you have a lot of tables in the cluster and the total number of pending compactions is usually high.

### `maxParallelRepairs`

_**Since 2.2.0**_

Type: *Integer*

Default: *2*

Reaper allows concurrent segments from distinct repair runs running on the same nodes at the same time. In order to limit the repair load, it will only allow a limited number of repair runs to run concurrently (by default, two). Repair runs over the threshold will still start and be in `RUNNING` state, but their segments will be postposned as long as there are too many repairs being processed.  
Setting this value too high could put a lot of pressure on clusters and negatively impact their performance. 

<br/>

### `cryptograph`

Optional settings to configure how confidential text (ie: passwords) are encyrpted/decrypted.

    cryptograph:
      type: symmetric
      systemPropertySecret: SOME_SYSTEM_PROPERTY_KEY

### `type`

The encryption technique used when encrypting, decrypting, or validating confidential text.  Symmetric encryption is the default.

### `algorithm`

Type: *String*

Default: PBKDF2WithHmacSHA512

The optional standard name of the requested secret-key algorithm. See the SecretKeyFactory section in the Java Cryptography Architecture Standard Algorithm Name Documentation for information about standard algorithm names.

### `cipher`

Type: *String*

Default: AES/CBC/PKCS5Padding

The optional name of the transformation, e.g., AES/CBC/PKCS5Padding. See the Cipher section in the Java Cryptography Architecture Standard Algorithm Name Documentation for information about standard transformation names.

### `cipherType`

Type: *String*

Default: AES

The optional name of the secret-key algorithm to be associated with the given key material. See Appendix A in the Java Cryptography Architecture Reference Guide for information about standard algorithm names.

### `iterationCount`

Type: Integer

Default: 1024

The optional number of times the password is hashed.

### `keyStrength`

Type: Integer

Default: 256

The optional length in bits of the derived symmetric key

### `salt`

Type: *String*

Default: deadbeef

The optional salt used for creating the PBEKeySpec

### `systemPropertySecret`

Type: *String*

The key of a system property that holds the shared secret for the symmetric encryption.  If encrypted text is required, then this key and its value need to be defined in the environment before reaper can be started.  
```export SOME_SYSTEM_PROPERTY_KEY=mysharedsecret```
