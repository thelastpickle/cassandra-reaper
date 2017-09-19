+++
[menu.docs]
name = "Docker"
weight = 50
+++

## Docker

[Docker](https://docs.docker.com/engine/installation/) and [Docker Compose](https://docs.docker.com/compose/install/) will need to be installed in order to use the commands in this section.

### Building Reaper Packages with Docker

Building Reaper packages requires quite a few dependencies, especially when making changes to the web interface code. In an effort to simplify the build process, Dockerfiles have been created that implement the build actions required to package Reaper.

To build the JAR and other packages which are then placed in the _packages_ directory run the following commands from the top level directory:

```bash
cd src/packaging
docker-compose build reaper-build-packages && docker-compose run reaper-build-packages
```

### Building Reaper Docker Image

To build the Reaper Docker Image which is then added to the local image cache using the `cassandra-reaper:latest` tag, run the following commands from the top level directory:

```bash
cd src/server
mvn package docker:build
```

Note that the above command will build the Reaper JAR and place it in the _src/server/target_ directory prior to creating the Docker Image. It is also possible to build the JAR file using the [Docker package build](building-reaper-packages-with-docker) instructions and omitting the `package` command from the above Maven commands.

### Start Docker Environment

The `docker-compose` services available allow for orchestration of an environment that uses default settings. In addition, services are provided that allow orchestration of an environment in which the connections between the services are SSL encrypted. Services which use SSL encryption contain a `-ssl` suffix in their name.

#### Default Settings Environment

From the top level directory change to the _src/packaging_ directory:

```bash
cd src/packaging
```

Start the Cassandra cluster:

```bash
docker-compose up cassandra
```

The `nodetool` Docker Compose service can be used to check on the Cassandra node's status:

```bash
docker-compose run nodetool status
```

Once the Cassandra node is online and accepting CQL connections, create the required `reaper_db` Cassandra keyspace to allow Reaper to save its cluster and scheduling data.

By default, the `reaper_db` keyspace is created using a replication factor of 1. To change this replication factor, provide the intended replication factor as an optional argument:

```bash
docker-compose run cqlsh-initialize-reaper_db [$REPLICATION_FACTOR]
```

Wait a few moments for the `reaper_db` schema change to propagate, then start Reaper:

```bash
docker-compose up reaper
```

#### SSL Encrypted Connections Environment

From the top level directory change to the _src/packaging_ directory:

```bash
cd src/packaging
```

Generate the SSL Keystore and Truststore which will be used to encrypt the connections between Reaper and Cassandra.

```bash
docker-compose run generate-ssl-stores
```

Start the Cassandra cluster which encrypts both the JMX and Native Protocol:

```bash
docker-compose up cassandra-ssl
```

The `nodetool-ssl` Docker Compose service can be used to check on the Cassandra node's status:

```bash
docker-compose run nodetool-ssl status
```

Once the Cassandra node is online and accepting encrypted SSL connections via the Native Transport protocol, create the required `reaper_db` Cassandra keyspace to allow Reaper to save its cluster and scheduling data.

By default, the `reaper_db` keyspace is created using a replication factor of 1. To change this replication factor, provide the intended replication factor as an optional argument:

```bash
docker-compose run cqlsh-initialize-reaper_db-ssl [$REPLICATION_FACTOR]
```

Wait a few moments for the `reaper_db` schema change to propagate, then start the Reaper service that will establish encrypted connections to Cassandra:

```bash
docker-compose up reaper-ssl
```


### Access The Environment

Once started, the UI can be accessed through:

http://127.0.0.1:8080/webui/

When adding the Cassandra node to the Reaper UI, use the IP address found via:

```bash
docker-compose run nodetool status
```

The helper `cqlsh` Docker Compose service has also been included for both the default and SSL encrypted environments:

#### Default Environment

```bash
docker-compose run cqlsh
```

#### SSL Encrypted Environment

```bash
docker-compose run cqlsh-ssl
```

### Destroying the Docker Environment

When terminating the infrastructure, use the following command to stop
all related Docker Compose services:

```bash
docker-compose down
```

To completely clean up all persistent data, delete the `./data/` directory:

```bash
rm -rf ./data/
```
