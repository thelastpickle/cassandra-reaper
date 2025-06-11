---
title: "Docker"
weight: 50
parent: "download"
---

[Docker](https://docs.docker.com/engine/installation/) and [Docker Compose](https://docs.docker.com/compose/install/) will need to be installed in order to use the commands in this section.

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

# Authentication credentials (required for security)
REAPER_AUTH_USER=admin
REAPER_AUTH_PASSWORD=your-secure-admin-password

docker run \
    -p 8080:8080 \
    -p 8081:8081 \
    -e "REAPER_JMX_AUTH_USERNAME=${REAPER_JMX_AUTH_USERNAME}" \
    -e "REAPER_JMX_AUTH_PASSWORD=${REAPER_JMX_AUTH_PASSWORD}" \
    -e "REAPER_AUTH_USER=${REAPER_AUTH_USER}" \
    -e "REAPER_AUTH_PASSWORD=${REAPER_AUTH_PASSWORD}" \
    thelastpickle/cassandra-reaper:${TAG}
```

Then visit the the Reaper UI: [http://localhost:8080/webui/](http://localhost:8080/webui/).

### Cassandra Backend

To launch a Reaper container backed by Cassandra, use the following example to connect to a Cassandra cluster that already has the `reaper_db` keyspace. Set the appropriate JMX authentication settings for the cluster that Reaper will manage repairs for.

```bash
TAG=latest

REAPER_JMX_AUTH_USERNAME=reaperUser
REAPER_JMX_AUTH_PASSWORD=reaperPass

# Authentication credentials (required for security)
REAPER_AUTH_USER=admin
REAPER_AUTH_PASSWORD=your-secure-admin-password

REAPER_CASS_CLUSTER_NAME=reaper-cluster
REAPER_CASS_CONTACT_POINTS={\"host\": \"192.168.2.185\", \"port\": \"9042\"}

docker run \
    -p 8080:8080 \
    -p 8081:8081 \
    -e "REAPER_JMX_AUTH_USERNAME=${REAPER_JMX_AUTH_USERNAME}" \
    -e "REAPER_JMX_AUTH_PASSWORD=${REAPER_JMX_AUTH_PASSWORD}" \
    -e "REAPER_AUTH_USER=${REAPER_AUTH_USER}" \
    -e "REAPER_AUTH_PASSWORD=${REAPER_AUTH_PASSWORD}" \
    -e "REAPER_STORAGE_TYPE=cassandra" \
    -e "REAPER_CASS_CLUSTER_NAME=${REAPER_CASS_CLUSTER_NAME}" \
    -e "REAPER_CASS_CONTACT_POINTS=${REAPER_CASS_CONTACT_POINTS}" \
    -e "REAPER_CASS_KEYSPACE=reaper_db" \
    thelastpickle/cassandra-reaper:${TAG}
```

Then visit the the Reaper UI: [http://localhost:8080/webui/](http://localhost:8080/webui/).

