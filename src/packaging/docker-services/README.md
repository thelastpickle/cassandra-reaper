# Dockerized Reaper Example

This directory, as well as the root directory's `docker-compose.yml`, compose
the repo's Dockerized environment.

Instructions for running this Dockerized environment appear in the main
[README](../#running-a-dockerized-reaper-example).

Simply put, the following actions are needed:

* Use `mvn` to build the `cassandra-reaper:latest` Docker image.
* Start a Cassandra node to act as:
    * Reaper's backend database.
    * The Cassandra node that Reaper will manage repairs for.
* Create the `reaper_db` keyspace to store Reaper's cluster and scheduling data.
* Start Reaper.

Within this README, we will discuss the individual components that simplify the
entire setup.

## Docker Compose

While the Docker Compose prerequisite is not necessary for running a Dockerized Reaper
environment, it simplifies all the options that would normally go into the
command-line when running `docker` into a clean `docker-compose.yml`. These
options can later be duplicated using the Docker provisioning service of your
choice.

### Cassandra

The Cassandra options are set to use a `mem_limit` and `memswap_limit` both set
to `4g`. Equivalent settings for both `mem_limit` and `memswap_limit`,
along with `mem_swappiness: 0`, will
[disable swapping](https://docs.docker.com/engine/reference/run/#user-memory-constraints)
which is ideal for Cassandra.

The [`env_file`](cassandra/cassandra.env), uses the `cassandra`
Docker Compose service name/container's hostname and sets both the
`$CASSANDRA_LISTEN_ADDRESS` and `$CASSANDRA_SEEDS` to `cassandra`. The
`env_file` also sets `LOCAL_JMX=no` to allow JMX to bind to the `cassandra`
hostname instead of `localhost` allowing Reaper to communicate with the
Cassandra node. All other settings are simply for demonstration purposes.

The [jmxremote.access](cassandra/jmxremote.access) and
[jmxremote.password](cassandra/jmxremote.password) files define the JMX
user/pass combination to be `reaperUser`/`reaperPass`. They are mounted using
the locations defined in the `services:cassandra:volumes` section of the
`docker-compose.yml`.

Through the inclusion of the `ports` settings within the `docker-compose.yml`'s
`cassandra` service, accessing the Cassandra node from outside of this
Docker Compose setup should also be immediately possible when providing the
`cassandra` service's IP address as defined by:
`docker-compose exec cassandra hostname -i`.

#### Cassandra Utilities

The `docker-compose.yml` also provides helper services to quickly use `cqlsh`
and `nodetool` without having to configure the necessary information.

The `cqlsh` service already comes preconfigured to use the `cassandra` hostname.

The `nodetool` service comes preconfigured to use the `cassandra` hostname as
well as the preconfigured JMX username and password as defined within
[jmxremote.access](cassandra/jmxremote.access) and
[jmxremote.password](cassandra/jmxremote.password).

### Reaper

Before the `reaper` service is started, the `initialize-reaper_db` service must
be run at least once in order to create the `reaper_db`
[keyspace](initialize-reaper_db/Dockerfile).
As long as the `./data/` directory is not removed the Cassandra data will be
persisted and thus the `reaper_db` keyspace, related tables, and data will all
appear once the `cassandra` service is started again.

The `reaper` service simply uses the pre-built images provided by running the
command: `mvn clean package docker:build`.

The [`env_file`](reaper/reaper.env) provides Reaper with the
preconfigured JMX username and password as defined within
[jmxremote.access](cassandra/jmxremote.access) and
[jmxremote.password](cassandra/jmxremote.password) in order to contact
the Cassandra nodes Reaper will be managing repairs for. The `env_file` also
sets the Reaper storage backend to use the same Cassandra node that is started
through the `cassandra` service.

## Using Dockerized Reaper in Production

In order to use Dockerized Reaper in production environments, you can use
the following Docker Compose service definition:

```yaml
version: '2.2'

services:
  reaper:
    image: cassandra-reaper:latest
    env_file:
      - path/to/reaper.env
    ports:
      - "8080:8080"
      - "8081:8081"
```

The above definition can be used on any machine that has built
the Reaper Docker image using `mvn clean package docker:build`.

The `env_file` environmental variable choices are defined within
[src/main/docker/cassandra-reaper.yml](../src/main/docker/cassandra-reaper.yml) and
[src/main/docker/append-persistence.sh](../src/main/docker/append-persistence.sh).
The default values can be seen within [src/main/docker/Dockerfile](../src/main/docker/Dockerfile).

The `ports` parameters will expose and map the `8080` and `8181` ports from the
container onto the host. This allows you to access the Reaper UI using:
`http://hostname:8080`.

To start the service, run the container in `detached` mode by including the
`-d` parameter: `docker-compose up -d reaper`.
