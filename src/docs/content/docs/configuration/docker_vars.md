+++
[menu.docs]
name = "Docker Variables"
parent = "configuration"
weight = 10
+++

# Docker Variables

The Reaper Docker container has been designed to be highly configurable. Many of the environment variables map directly or indirectly to a settings in the *cassandra-reaper.yaml* configuration file.

## Direct Mapping to Reaper Specific Configuration Settings

The Docker environment variables listed in this section map directly to Reaper specific settings in the *cassandra-reaper.yaml* configuration file. The following table below lists the Docker environment variables, their associated Reaper specific setting in the *cassandra-reaper.yaml* configuration file, and the default value assigned by the Docker container (if any). Definitions for each Docker environment variable can be found via the link to the associated setting.

<style>
    .vartable tr td { padding-top:5px; }
    .vartable td { padding-left:15px; }
    .vartable th { padding-left:15px; }
    .vartable code { font-weight:bold; font-size:16px; }
</style>

<table class="vartable">
    <tr>
        <th>Environment Variable</th>
        <th>Configuration Setting</th>
        <th>Default Value</th>
    </tr>
    <tr>
        <td><code>REAPER_AUTO_SCHEDULING_ENABLED</code></td>
        <td><a href="../reaper_specific#endabled">endabled</a></td>
        <td>false</td>
    </tr>
    <tr>
        <td><code>REAPER_AUTO_SCHEDULING_EXCLUDED_KEYSPACES</code></td>
        <td><a href="../reaper_specific#excludedkeyspaces">excludedKeyspaces</a></td>
        <td>[]</td>
    </tr>
    <tr>
        <td><code>REAPER_AUTO_SCHEDULING_INITIAL_DELAY_PERIOD</code></td>
        <td><a href="../reaper_specific#initialdelayperiod">initialDelayPeriod</a></td>
        <td>PT15S</td>
    </tr>
    <tr>
        <td><code>REAPER_AUTO_SCHEDULING_PERIOD_BETWEEN_POLLS</code></td>
        <td><a href="../reaper_specific#periodbetweenpolls">periodBetweenPolls</a></td>
        <td>PT10M</td>
    </tr>
    <tr>
        <td><code>REAPER_AUTO_SCHEDULING_SCHEDULE_SPREAD_PERIOD</code></td>
        <td><a href="../reaper_specific#schedulespreadperiod">scheduleSpreadPeriod</a></td>
        <td>PT6H</td>
    </tr>
    <tr>
        <td><code>REAPER_AUTO_SCHEDULING_TIME_BEFORE_FIRST_SCHEDULE</code></td>
        <td><a href="../reaper_specific#timebeforefirstschedule">timeBeforeFirstSchedule</a></td>
        <td>PT5M</td>
    </tr>
    <tr>
        <td><code>REAPER_DATACENTER_AVAILABILITY</code></td>
        <td><a href="../reaper_specific#datacenteravailability">datacenterAvailability</a></td>
        <td>ALL</td>
    </tr>
    <tr>
        <td><code>REAPER_ENABLE_CROSS_ORIGIN</code></td>
        <td><a href="../reaper_specific#enablecrossorigin">enableCrossOrigin</a></td>
        <td>true</td>
    </tr>
    <tr>
        <td><code>REAPER_ENABLE_DYNAMIC_SEED_LIST</code></td>
        <td><a href="../reaper_specific#enabledynamicseedlist">enableDynamicSeedList</a></td>
        <td>true</td>
    </tr>
    <tr>
        <td><code>REAPER_HANGING_REPAIR_TIMEOUT_MINS</code></td>
        <td><a href="../reaper_specific#hangingrepairtimeoutmins">hangingRepairTimeoutMins</a></td>
        <td>30</td>
    </tr>
    <tr>
        <td><code>REAPER_INCREMENTAL_REPAIR</code></td>
        <td><a href="../reaper_specific#incrementalrepair">incrementalRepair</a></td>
        <td>false</td>
    </tr>
    <tr>
        <td><code>REAPER_JMX_AUTH_PASSWORD</code></td>
        <td><a href="../reaper_specific#password">password</a></td>
        <td></td>
    </tr>
    <tr>
        <td><code>REAPER_JMX_AUTH_USERNAME</code></td>
        <td><a href="../reaper_specific#username">username</a></td>
        <td></td>
    </tr>
    <tr>
        <td><code>REAPER_JMX_CONNECTION_TIMEOUT_IN_SECONDS</code></td>
        <td><a href="../reaper_specific#jmxconnectiontimeoutinseconds">jmxConnectionTimeoutInSeconds</a></td>
        <td>20</td>
    </tr>
    <tr>
        <td><code>REAPER_JMX_PORTS</code></td>
        <td><a href="../reaper_specific#jmxports">jmxPorts</a></td>
        <td>{}</td>
    </tr>
    <tr>
        <td><code>REAPER_LOGGING_APPENDERS_LOG_FORMAT</code></td>
        <td><a href="../reaper_specific#logformat">logFormat</a></td>
        <td>"%-6level [%d] [%t] %logger{5} - %msg %n"</td>
    </tr>
    <tr>
        <td><code>REAPER_LOGGING_LOGGERS</code></td>
        <td><a href="../reaper_specific#loggers">loggers</a></td>
        <td>{}</td>
    </tr>
    <tr>
        <td><code>REAPER_LOGGING_ROOT_LEVEL</code></td>
        <td><a href="../reaper_specific#level">level</a></td>
        <td>INFO</td>
    </tr>
    <tr>
        <td><code>REAPER_METRICS_FREQUENCY</code></td>
        <td><a href="../reaper_specific#fequency">fequency</a></td>
        <td>1 minute</td>
    </tr>
    <tr>
        <td><code>REAPER_METRICS_REPORTERS</code></td>
        <td><a href="../reaper_specific#reporters">reporters</a></td>
        <td>[]</td>
    </tr>
    <tr>
        <td><code>REAPER_REPAIR_INTENSITY</code></td>
        <td><a href="../reaper_specific#repairintensity">repairIntensity</a></td>
        <td>0.9</td>
    </tr>
    <tr>
        <td><code>REAPER_REPAIR_MANAGER_SCHEDULING_INTERVAL_SECONDS</code></td>
        <td><a href="../reaper_specific#repairmanagerschedulingintervalseconds">repairManagerSchedulingIntervalSeconds</a></td>
        <td>30</td>
    </tr>
    <tr>
        <td><code>REAPER_REPAIR_PARALELLISM</code></td>
        <td><a href="../reaper_specific#repairparallelism">repairParallelism</a></td>
        <td>DATACENTER_AWARE</td>
    </tr>
    <tr>
        <td><code>REAPER_REPAIR_RUN_THREADS</code></td>
        <td><a href="../reaper_specific#repairrunthreadcount">repairRunThreadCount</a></td>
        <td>15</td>
    </tr>
    <tr>
        <td><code>REAPER_SCHEDULE_DAYS_BETWEEN</code></td>
        <td><a href="../reaper_specific#scheduledaysbetween">scheduleDaysBetween</a></td>
        <td>7</td>
    </tr>
    <tr>
        <td><code>REAPER_SEGMENT_COUNT</code></td>
        <td><a href="../reaper_specific#segmentcount">segmentCount</a></td>
        <td>200</td>
    </tr>
    <tr>
        <td><code>REAPER_SERVER_ADMIN_BIND_HOST</code></td>
        <td><a href="../reaper_specific#bindhost">bindHost</a></td>
        <td>0.0.0.0</td>
    </tr>
    <tr>
        <td><code>REAPER_SERVER_ADMIN_PORT</code></td>
        <td><a href="../reaper_specific#port">port</a></td>
        <td>8081</td>
    </tr>
    <tr>
        <td><code>REAPER_SERVER_APP_BIND_HOST</code></td>
        <td><a href="../reaper_specific#bindhost">bindHost</a></td>
        <td>0.0.0.0</td>
    </tr>
    <tr>
        <td><code>REAPER_SERVER_APP_PORT</code></td>
        <td><a href="../reaper_specific#port">port</a></td>
        <td>8080</td>
    </tr>
    <tr>
        <td><code>REAPER_STORAGE_TYPE</code></td>
        <td><a href="../reaper_specific#storagetype">storageType</a></td>
        <td>memory</td>
    </tr>
    <tr>
        <td><code>REAPER_USE_ADDRESS_TRANSLATOR</code></td>
        <td><a href="../reaper_specific#useaddresstranslator">useAddressTranslator</a></td>
        <td>false</td>
    </tr>
</table>

## Associated Reaper Specific Configuration Settings

The following Docker environment variables have no direct mapping to a setting in the *cassandra-reaper.yaml* configuration file. However, they do affect the content contained in the file that is Reaper specific.

</br>

#### `REAPER_METRICS_ENABLED`

Type: *Boolean*

Default: *false*

Allows the sending of Reaper metrics to a metrics reporting system such as Graphite. If enabled, the other associated environment variables `REAPER_METRICS_FREQUENCY` and `REAPER_METRICS_REPORTERS` must be set to appropriate values in order for metrics reporting to function correctly.

## Direct Mapping to Cassandra Backend Specific Configuration Settings

The Docker environment variables listed in this section map directly to Cassandra backend specific settings in the *cassandra-reaper.yaml* configuration file. The following table below lists the Docker environment variables, their associated Cassandra backend specific setting in the *cassandra-reaper.yaml* configuration file, and the default value assigned by the Docker container (if any). Definitions for each Docker environment variable can be found via the link to the associated setting.

In order for the Cassandra backend to be used, `REAPER_STORAGE_TYPE` must be set to `cassandra`.

**Note:**

Some variable names and defaults have changed between the release of
Docker-support and Reaper for Apache Cassandra 1.0.

The following variable name changes that have occurred in an effort to match
closely with our yaml parameter names:

* `REAPER_ENABLE_CORS` -> `REAPER_ENABLE_CROSS_ORIGIN`
* `REAPER_ENABLE_DYNAMIC_SEEDS` -> `REAPER_ENABLE_DYNAMIC_SEED_LIST`
* `REAPER_AUTO_SCHEDULE_ENABLED` -> `REAPER_AUTO_SCHEDULING_ENABLED`
* `REAPER_AUTO_SCHEDULE_INITIAL_DELAY_PERIOD` -> `REAPER_AUTO_SCHEDULING_INITIAL_DELAY_PERIOD`
* `REAPER_AUTO_SCHEDULE_PERIOD_BETWEEN_POLLS` -> `REAPER_AUTO_SCHEDULING_PERIOD_BETWEEN_POLLS`
* `REAPER_AUTO_SCHEDULE_TIME_BETWEEN_FIRST_SCHEDULE` -> `REAPER_AUTO_SCHEDULING_TIME_BEFORE_FIRST_SCHEDULE`
* `REAPER_AUTO_SCHEDULE_EXCLUDED_KEYSPACES` -> `REAPER_AUTO_SCHEDULING_EXCLUDED_KEYSPACES`
* `REAPER_JMX_USERNAME` -> `REAPER_JMX_AUTH_USERNAME`
* `REAPER_JMX_PASSWORD` -> `REAPER_JMX_AUTH_PASSWORD`
* `REAPER_LOGGERS` -> `REAPER_LOGGING_LOGGERS`
* `REAPER_LOGGING_FORMAT` -> `REAPER_LOGGING_APPENDERS_LOG_FORMAT`
* `REAPER_APP_PORT` -> `REAPER_SERVER_APP_PORT`
* `REAPER_APP_BIND_HOST` -> `REAPER_SERVER_APP_BIND_HOST`
* `REAPER_ADMIN_PORT` -> `REAPER_SERVER_ADMIN_PORT`
* `REAPER_ADMIN_BIND_HOST` -> `REAPER_SERVER_ADMIN_BIND_HOST`
* `REAPER_ACTIVATE_QUERY_LOGGER` -> `REAPER_CASS_ACTIVATE_QUERY_LOGGER`

The following default values have changed:

* `REAPER_CASS_KEYSPACE`:
    * Previous default: `cassandra-reaper`
    * New default: `reaper_db`

<table class="vartable">
    <tr>
        <th>Environment Variable</th>
        <th>Configuration Setting</th>
        <th>Default Value</th>
    </tr>
    <tr>
        <td><code>REAPER_CASS_ACTIVATE_QUERY_LOGGER</code></td>
        <td><a href="../backend_specific#activatequerylogger">activateQueryLogger</a></td>
        <td>false</td>
    </tr>
    <tr>
        <td><code>REAPER_CASS_CLUSTER_NAME</code></td>
        <td><a href="../backend_specific#clustername">clusterName</a></td>
        <td>clustername</td>
    </tr>
    <tr>
        <td><code>REAPER_CASS_CONTACT_POINTS</code></td>
        <td><a href="../backend_specific#contactpoints">contactPoints</a></td>
        <td>[]</td>
    </tr>
    <tr>
        <td><code>REAPER_CASS_KEYSPACE</code></td>
        <td><a href="../backend_specific#keyspace">keyspace</a></td>
        <td>cassandra-reaper</td>
    </tr>
    <tr>
        <td><code>REAPER_CASS_LOCAL_DC</code></td>
        <td><a href="../backend_specific#localdc">localDC</a></td>
        <td></td>
    </tr>
    <tr>
        <td><code>REAPER_CASS_AUTH_USERNAME</code></td>
        <td><a href="../backend_specific#username">username</a></td>
        <td>cassandra</td>
    </tr>
    <tr>
        <td><code>REAPER_CASS_AUTH_PASSWORD</code></td>
        <td><a href="../backend_specific#password">password</a></td>
        <td>cassandra</td>
    </tr>
</table>

## Associated Cassandra Backend Specific Configuration Settings

The following Docker environment variables have no direct mapping to a setting in the *cassandra-reaper.yaml* configuration file. However, the do affect the content contained in the file that is Cassandra backend specific.

</br>

#### `REAPER_CASS_AUTH_ENABLED`

Type: *Boolean*

Default: *false*

Allows Reaper to send authentication credentials when establishing a connection with Cassandra via the native protocol. When enabled, authentication credentials must be specified by setting values for `REAPER_CASS_AUTH_USERNAME` and `REAPER_CASS_AUTH_PASSWORD`.

</br>

#### `REAPER_CASS_NATIVE_PROTOCOL_SSL_ENCRYPTION_ENABLED`

Type: *Boolean*

Default: *false*

Allows Reaper to establish an encrypted connection when establishing a connection with Cassandra via the native protocol.

## Direct Mapping to H2 or Postgres Backend Configuration Settings

The Docker environment variables listed in this section map directly to H2/Postgres backend specific settings in the *cassandra-reaper.yaml* configuration file. The following table below lists the Docker environment variables, their associated H2/Postgres backend specific setting in the *cassandra-reaper.yaml* configuration file, and the default value assigned by the Docker container (if any). Definitions for each Docker environment variable can be found via the link to the associated setting.

In order to use the following settings, `REAPER_STORAGE_TYPE` must be set to `database`.

<table class="vartable">
    <tr>
        <th>Environment Variable</th>
        <th>Configuration Setting</th>
        <th>Default Value</th>
    </tr>
    <tr>
        <td><code>REAPER_DB_DRIVER_CLASS</code></td>
        <td><a href="../backend_specific#driverclass">driverClass</a></td>
        <td>org.h2.Driver</td>
    </tr>
    <tr>
        <td><code>REAPER_DB_URL</code></td>
        <td><a href="../backend_specific#url">url</a></td>
        <td>jdbc:h2:/var/lib/cassandra-reaper/db;MODE=PostgreSQL</td>
    </tr>
    <tr>
        <td><code>REAPER_DB_USERNAME</code></td>
        <td><a href="../backend_specific#user">user</a></td>
        <td></td>
    </tr>
    <tr>
        <td><code>REAPER_DB_PASSWORD</code></td>
        <td><a href="../backend_specific#password-1">password</a></td>
        <td></td>
    </tr>
</table>
