+++
[menu.docs]
name = "Docker"
weight = 50
parent = "download"
+++

# Docker

[Docker](https://docs.docker.com/engine/installation/) and [Docker Compose](https://docs.docker.com/compose/install/) will need to be installed in order to use the commands in this section.

## Building Reaper Packages with Docker

Building Reaper packages requires quite a few dependencies, especially when making changes to the web interface code. In an effort to simplify the build process, Dockerfiles have been created that implement the build actions required to package Reaper.

To build the JAR and other packages which are then placed in the _src/packages_ directory run the following commands:

```bash
cd src/packaging/docker-build
docker-compose build
docker-compose run build
```

## Building Reaper Docker Image

### Prerequisite

The generation of the Docker image requires that the JAR file be built and placed in the _src/packages_ directory. If the JAR package is missing from the directory then it can built using either the steps in the [Docker package build](building-reaper-packages-with-docker) section (above), or in the [Building from Source]({{< relref "building.md#building-jars-from-source" >}}) section.

### Building Image

To build the Reaper Docker Image which is then added to the local image cache using the `cassandra-reaper:latest` tag, run the following command from the top level directory.

```bash
mvn -pl src/server/ docker:build -Ddocker.directory=src/server/src/main/docker
```

## Docker Hub Image

A prebuilt Docker Image is available for download from [Docker Hub](https://hub.docker.com/r/thelastpickle/cassandra-reaper/). The image `TAG` can be specified when pulling the image from Docker Hub to pull a particular version. Set:

* `TAG=master` to run Reaper with the latest commits
* `TAG=latest` to run Reaper with the latest stable release

To pull the image from Docker Hub with a particular tag, run the following command.

```bash
docker pull thelastpickle/cassandra-reaper:${TAG}
```

# Start Docker Environment

## Using Docker

Reaper can be executed within a Docker container with either an ephemeral memory storage or persistent database.

### In-Memory Backend

To launch a Reaper container backed by an In-Memory backend, use the following example with the appropriate JMX authentication settings for the cluster it will manage repairs for.

```bash
TAG=latest

REAPER_JMX_AUTH_USERNAME=reaperUser
REAPER_JMX_AUTH_PASSWORD=reaperPass

docker run \
    -p 8080:8080 \
    -p 8081:8081 \
    -e "REAPER_JMX_AUTH_USERNAME=${REAPER_JMX_AUTH_USERNAME}" \
    -e "REAPER_JMX_AUTH_PASSWORD=${REAPER_JMX_AUTH_PASSWORD}" \
    thelastpickle/cassandra-reaper:${TAG}
```

Then visit the the Reaper UI: [http://localhost:8080/webui/](http://localhost:8080/webui/).

### Cassandra Backend

To launch a Reaper container backed by Cassandra, use the following example to connect to a Cassandra cluster that already has the `reaper_db` keyspace. Set the appropriate JMX authentication settings for the cluster that Reaper will manage repairs for.

```bash
TAG=latest

REAPER_JMX_AUTH_USERNAME=reaperUser
REAPER_JMX_AUTH_PASSWORD=reaperPass

REAPER_CASS_CLUSTER_NAME=reaper-cluster
REAPER_CASS_CONTACT_POINTS=["192.168.2.185"]

docker run \
    -p 8080:8080 \
    -p 8081:8081 \
    -e "REAPER_JMX_AUTH_USERNAME=${REAPER_JMX_AUTH_USERNAME}" \
    -e "REAPER_JMX_AUTH_PASSWORD=${REAPER_JMX_AUTH_PASSWORD}" \
    -e "REAPER_STORAGE_TYPE=cassandra" \
    -e "REAPER_CASS_CLUSTER_NAME=${REAPER_CASS_CLUSTER_NAME}" \
    -e "REAPER_CASS_CONTACT_POINTS=${REAPER_CASS_CONTACT_POINTS}" \
    -e "REAPER_CASS_KEYSPACE=reaper_db" \
    thelastpickle/cassandra-reaper:${TAG}
```

Then visit the the Reaper UI: [http://localhost:8080/webui/](http://localhost:8080/webui/).

## Using Docker Compose

The Docker Compose services available allow for orchestration of an environment that uses Reaper's default settings. This provides a quick way to start Reaper and become familiar with its usage without the need of additional infrastructure. The environment created using Docker Compose comprises a single containerised Apache Cassandra node and a single containerised Reaper service.

In addition to the environment using Reaper's default settings, Docker Compose services are provided that allow orchestration of an environment in which the connections between Reaper and Cassandra are SSL encrypted. The services which create this environment contain a `-ssl` suffix in their name.

All available Docker Compose services can be found in the [docker-compose.yml](https://github.com/thelastpickle/cassandra-reaper/blob/master/src/packaging/docker-compose.yml) file.


### Default Settings Environment

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

### SSL Encrypted Connections Environment

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


## Access The Environment

Once started, the UI can be accessed through:

http://127.0.0.1:8080/webui/

A `nodetool` Docker Compose service is included for both the default and SSL encrypted environments to allow `nodetool` commands to be performed on Cassandra.

For the **default** environment use:

```bash
docker-compose run nodetool status
```

For the **SSL encrypted** environment use:

```bash
docker-compose run nodetool-ssl status
```

When adding the Cassandra node to the Reaper UI, the above commands can be used to find the node IP address.

A `cqlsh` Docker Compose service is included as well for both the default and SSL encrypted environments to allow the creation of user tables in Cassandra.

For the **default** environment use:

```bash
docker-compose run cqlsh
```

For the **SSL encrypted** environment use:

```bash
docker-compose run cqlsh-ssl
```

## Destroying the Docker Environment

When terminating the infrastructure, use the following command to stop
all related Docker Compose services:

```bash
docker-compose down
```

To completely clean up all persistent data, delete the `./data/` directory:

```bash
rm -rf ./data/
```
