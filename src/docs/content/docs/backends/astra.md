+++
[menu.docs]
name = "Astra"
parent = "backends"
weight = 5
+++

# Astra Backend

To use Astra (Cassandra managed service by Datastax) as the persistent storage for Reaper, the `storageType` setting must be set to **astra** in the Reaper configuration YAML file. In addition, the connection details for the Astra cluster being used to store Reaper data must be specified in the configuration YAML file. An example of how to configure Astra as persistent storage for Reaper can be found in the *[cassandra-reaper-astra.yaml](https://github.com/thelastpickle/cassandra-reaper/blob/master/src/packaging/resource/cassandra-reaper-astra.yaml)*.

```yaml
storageType: astra
cassandra:
  clusterName: "reaper"
  contactPoints: ["astra host from the secure bundle config.json file"]
  keyspace: reaper_db
  port: <cql port found in the secure bundle cqlshrc file>
  authProvider:
    type: plainText
    username: reaper
    password: ReaperOnAstraRocks
  ssl:
    type: jdk
```

The CQL port to connect to can be found in the `cqlshrc` file of Astra's secure connect bundle. The port found in `config.json` is the metadata port which cannot be used for CQL connections. 

The Astra backend provides the same capabilities as [the Cassanda backend]({{<ref "cassandra.md">}}).

Schema initialization and migration will be done automatically upon startup.

## SSL settings

Astra enables client to node encryption by default, which requires some additional setup in Reaper.
After installing Reaper and configuring the yaml file, copy the `cassandra-reaper-ssl.properties` file to the `/etc/reaper` directory (the template can be found under `/etc/reaper/configs/`) and configure it as follows:

```
-Djavax.net.ssl.keyStore=/path/to/identity.jks
-Djavax.net.ssl.keyStorePassword=keystore_password

-Djavax.net.ssl.trustStore=/path/to/trustStore.jks
-Djavax.net.ssl.trustStorePassword=truststore_password

# Comment the following line when using the Astra backend
# unless JMX encryption is enabled with the same keystore/truststore
# -Dssl.enable=true
```

The truststore and keystore (identity) files can be found in the secure connect bundle which should be downloaded from your Astra dashboard. The passwords will be found in the `config.json` file of that same bundle.
Make sure you comment the `-Dssl.enable=true` line as it enables JMX encryption.

If both CQL and JMX encryption need to be enabled, then JMX encryption must be configured to use the same truststore/keystore than Astra, and the `-Dssl.enable=true` should be left uncommented.