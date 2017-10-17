+++
[menu.docs]
name = "Reaper Specific Settings"
parent = "configuration"
weight = 4
+++

# Reaper Specific Settings

Configuration settings in the *cassandra-reaper.yaml* that are specific to Reaper

</br>

### `autoScheduling`

Optional setting to automatically setup repair schedules for all non-system keyspaces in a cluster. If enabled, adding a new cluster will automatically setup a schedule repair  for each keyspace. Cluster keyspaces are monitored based on a configurable frequency, so that adding or removing a keyspace will result in adding / removing the corresponding scheduled repairs.

    autoScheduling:
      enabled: true
      initialDelayPeriod: PT15S
      periodBetweenPolls: PT10M
      timeBeforeFirstSchedule: PT5M
      scheduleSpreadPeriod: PT6H
      excludedKeyspaces: [myTTLKeyspace, ...]

Definitions for the above sub-settings are as follows.

#### `endabled`

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

</br>

### `datacenterAvailability`

Type: *String*

Default: *ALL*

Indicates to Reapers its deployment in relation to cluster data center network locality. The value must be either **ALL**, **LOCAL**, or **EACH**. Note that this setting controls the behavior for metrics collection.

For security reasons, it is possible that Reaper will have access limited to nodes in a single datacenter via JMX (multi region clusters for example). In this case, it is possible to deploy an operate an instance of Reaper in each datacenter where each instance only has access via JMX (with or without authentication) to the nodes in its local datacenter. Where multiple instances of Reaper are in operation in this configuration, only the Apache Cassandra storage option can be used with Reaper. All other storage options are unsuitable in this case. This is because Reaper instances will rely on lightweight transactions to get leadership on segments before processing them. In addition, Reaper will check the number of pending compactions and actively running repairs on all replicas prior to processing a segment.

**ALL** - requires Reaper to have access via JMX to all nodes across all datacenters. In this mode Reaper can be backed by all available storage types.

**LOCAL** - requires Reaper to have access via JMX to all nodes only in the same datacenter local to Reaper. A single Reaper instance can operate in this mode and repair its local data center. In this case, can be backed by all available storage types and repairs to any remote datacenters are be handled internally by Cassandra. A Reaper instance can be deployed to each datacenter and be configured to operate in this mode. In this case, Reaper can only use Apache Cassandra as its storage. In addition, metrics can be collected asynchronously through the Apache Cassandra storage.

**EACH** - requires a minimum of one Reaper instance operating in each datacenter. Each Reaper instance is required to have access via JMX to all nodes only in its local datacenter. When operating in this mode, Reaper can only use Apache Cassandra as its storage. In addition, metrics from nodes in remote datacenters must be collected through the Cassandra storage backend. If all metrics are unavailable, the segment will be postponed for later processing.

</br>

### `enableCrossOrigin`

Type: *Boolean*

Default: *true*

Optional setting which can be used to enable the CORS headers for running an external GUI application, like [this project](https://github.com/spodkowinski/cassandra-reaper-ui). When enabled it will allow REST requests incoming from other origins than the domain that hosts Reaper.

</br>

### `enableDynamicSeedList`

Type: *Boolean*

Default: *true*

Allow Reaper to add all nodes in the cluster as contact points when adding a new cluster, instead of just adding the provided node.

</br>

### `hangingRepairTimeoutMins`

Type: *Integer*

The amount of time in minutes to wait for a single repair to finish. If this timeout is reached,
the repair segment in question will be cancelled, if possible, and then scheduled for later
repair again within the same repair run process.

</br>

### `incrementalRepair`

Type: *Boolean*

Default: *false*

Sets the default repair type unless specifically defined for each run. Note that this is only supported with the PARALLEL repairParallelism setting. For more details in incremental repair, please refer to the following article.http://www.datastax.com/dev/blog/more-efficient-repairs

*Note*: It is recommended to avoid using incremental repair before Cassandra 4.0 as subtle bugs can lead to overstreaming and cluster instabililty.

</br>

### `jmxAuth`

Optional setting to allow Reaper to establish JMX connections to Cassandra clusters using password based JMX authentication. 

    jmxAuth:
      username: cassandra
      password: cassandra

#### `username`

Type: *String*

Cassandra JMX username.

#### `password`

Type: *String*

Cassandra JMX password.

</br>

### `jmxConnectionTimeoutInSeconds`

Type: *Integer*

Default: *20*

Controls the timeout for establishing JMX connections. The value should be low enough to avoid stalling simple operations in multi region clusters, but high enough to allow connections under normal conditions.

</br>

### `jmxPorts`

Type: *Object*

Optional mapping of custom JMX ports to use for individual hosts. The used default JMX port value is 7199. [CCM](https://github.com/pcmanus/ccm) users will find IP and port number to add in `~/.ccm/<cluster>/*/node.conf` or by running `ccm <node> show`.

    jmxPorts:
      127.0.0.1: 7100
      127.0.0.2: 7200
      127.0.0.3: 7300

</br>

### `localJmxMode`

Type: *Boolean*

Default: *false*

Activates the mode where JMX is only accessible from localhost. If set to true, one Reaper instance must be running on each Cassandra node.

</br>

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

Definitions for some of the above sub-settings are as follows.

#### `level`

Type: *String*

The log level to filter to. Where the level order is **ALL** < **DEBUG** < **INFO** < **WARN** < **ERROR** < **FATAL** < **OFF**. See the [log4j](https://logging.apache.org/log4j/1.2/apidocs/org/apache/log4j/Level.html) documentation for further information.

#### `loggers`

Type: *Object*

Key value pair containing the logger class name as the key and other sub-settings as its value.

#### `logFormat`

Type: *String*

The output format of an entry in the log.

</br>

### `metrics`

Metrics configuration parameters for sending metrics to a reporting system via the [Dropwizard interface](http://www.dropwizard.io/1.1.4/docs/manual/configuration.html#metrics) .

    metrics:
      frequency: "1 minute"
      reporters:
        - type: <type>

</br>

### `repairIntensity`

Type: *Float* (value between 0.0 and 1.0, but must never be 0.0.)

Repair intensity defines the amount of time to sleep between triggering each repair segment while running a repair run. When intensity is 1.0, it means that Reaper doesn't sleep at all before triggering next segment, and otherwise the sleep time is defined by how much time it took to repair the last segment divided by the intensity value. 0.5 means half of the time is spent sleeping, and half running. Intensity 0.75 means that 25% of the total time is used sleeping and 75% running. This value can also be overwritten per repair run when invoking repairs.

</br>

### `repairManagerSchedulingIntervalSeconds`

Type: *Integer*

Default: *30*

Controls the pace at which the Repair Manager will schedule processing of the next segment. Reducing this value from its default value of 30s to a lower value can speed up fast repairs by orders of magnitude.

</br>

### `repairParallelism`

Type: *String*

Type of parallelism to apply by default to repair runs. The value must be either **SEQUENTIAL**, **PARALLEL**, or **DATACENTER_AWARE**.

**SEQUENTIAL** - one replica at a time, validation compaction performed on snapshots

**PARALLEL** - all replicas at the same time, no snapshot

**DATACENTER_AWARE** - all replicas in only one DC at a time, no snapshots. If this value is used in clusters older than 2.0.12, Reaper will fall back into using **SEQUENTIAL** for those clusters.

</br>

### `repairRunThreadCount`

Type: *Integer*

The amount of threads to use for handling the Reaper tasks. Have this big enough not to cause
blocking in cause some thread is waiting for I/O, like calling a Cassandra cluster through JMX.

</br>

### `scheduleDaysBetween`

Type: *Integer*

Default: *7*

Defines the amount of days to wait between scheduling new repairs. The value configured here is the default for new repair schedules, but you can also define it separately for each new schedule. Using value 0 for continuous repairs is also supported.

</br>

### `segmentCount`

Type: *Integer*

Defines the default amount of repair segments to create for newly registered Cassandra repair runs (token rings). When running a repair run by the Reaper, each segment is repaired separately by the Reaper process, until all the segments in a token ring are repaired. The count might be slightly off the defined value, as clusters residing in multiple data centers require additional small token ranges in addition to the expected. This value can be overwritten when executing a repair run via Reaper.

</br>

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

</br>

### `storageType`

Type: *String*

Whether to use database or memory based storage for storing the system state. The value must be either **cassandra**, **database** or **memory**. If the recommended (persistent) storage type **database** or **cassandra** is being used, the database client parameters must be specified in the respective `database` or `cassandra` section in the configuration file. See the example settings in provided testing configuration in *src/test/resources/cassandra-reaper.yaml*.

</br>

### `useAddressTranslator`

Type: *Boolean*

Default: *false*

When running multi region clusters in AWS, turn this setting to `true` in order to use the EC2MultiRegionAddressTranslator from the Datastax Java Driver. This will allow translating the public address that the nodes broadcast to the private IP address that is used to expose JMX.