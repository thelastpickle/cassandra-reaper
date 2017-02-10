Docker Support and Infrastructure for Reaper
============================================

The following software is required:

* [Docker](https://docs.docker.com/engine/installation/)
* [Docker Compose](https://docs.docker.com/compose/install/)

**Note**: The following commands are all assumed to be run from the root
repository directory.


Building Reaper Packages Using Docker
-------------------------------------

This command installs all dependencies needed to build the Debian, jar,
and RPM packages:

    docker build --tag reaper-build-packages --file docker/build-packages/Dockerfile .

This command builds the packages into the host machine's `./packages` directory:

    docker run -ti -v `pwd`/packages:/usr/src/app/packages reaper-build-packages


Running In-Memory Reaper Using Docker Compose
---------------------------------------------

These commands will build the jar file into the `./packages` directory:

    docker build --tag reaper-build-packages --file docker/build-packages/Dockerfile .
    docker run -ti -v `pwd`/packages:/usr/src/app/packages reaper-build-packages

This command will build the service images using the previously built jar file:

    docker-compose build

This command will run Reaper in in-memory mode:

    docker-compose up reaper-in-memory

The following URLs become available:

* Main Interface: [http://localhost:8080/webui/index.html](http://localhost:8080/webui/index.html)
* Operational Interface: [http://localhost:8081](http://localhost:8081)


Running Cassandra-backed Reaper using Docker Compose
----------------------------------------------------

After having built the images as described in the
`Running In-Memory Reaper Using Docker Compose` section, ensure there are no
running containers by using:

    docker-compose down

Then, ensure there exists a running Cassandra container. This can be confirmed
once `Starting listening for CQL clients` appears in the log output:

    docker-compose up cassandra

If not already initialized, make sure the Cassandra cluster has the correct
schema:

    docker-compose run reaper-setup

Once Cassandra's schema has been initialized, run Reaper:

    docker-compose up reaper

The following URLs become available:

* Main Interface: [http://localhost:8080/webui/index.html](http://localhost:8080/webui/index.html)
* Operational Interface: [http://localhost:8081](http://localhost:8081)

**Note**: Although Reaper will be using a Cassandra backend, the Dockerized
Cassandra container will be running with JMX accessible *only* from localhost.
Therefore, another Cassandra cluster with proper JMX settings will need to
be created in order for Reaper to monitor a cluster.

In order for JMX authentication to be set up correctly, the following settings
must be in effect on the Cassandra-side:

* cassandra.yaml
    * `seeds`: must be set to the host's private IP address.
    * `listen_address`: must be set to the host's private IP address.
    * `rpc_address`: must be set to the host's private IP address.
* cassandra-env.sh
    * `LOCAL_JMX` must resolve to something other than "yes".
* /usr/lib/jvm/java-8-openjdk-amd64/jre/lib/management/jmxremote.access
    * Should include a line in the format of: `<user> readwrite`.
* /etc/cassandra/jmxremote.password
    * Should include a line in the format of: `<user> <password>`.
    * Have `600` file permissions.
    * Be owned by the `cassandra` user and group.



Running Cassandra-backed Reaper using Docker Compose and External Cassandra Cluster
-----------------------------------------------------------------------------------

If you wish to use an existing Cassandra backend on a separate machine, instead
of the provided containerized Cassandra, the directions are the same as the
above sections with a few differences:

* Update `./docker-compose.yml`'s `reaper` and `reaper-setup` services to use
`command`s that match an existing Cassandra node's IP address.
* Initialize the schema using: `docker-compose run reaper-setup`.
* Run Reaper using: `docker-compose up reaper`.

**Note**: Because Docker networking can use different settings and use of the
correct JMX settings are required for this sort of setup, using a Cassandra cluster not
located at `127.0.0.1`, or `localhost`, is ideal.


Shutting Down Docker Compose Containers
---------------------------------------

To shut everything down all containers that were started using `docker-compose`
command, run:

    docker-compose down


Using a Docker Container in Production
--------------------------------------

To facilitate production environments where Docker is heavily in use, we've
provided a Dockerfile example for building an ideal production image. That file
and README can be found in [./production-reaper](./production-reaper).

There is a [ticket](https://github.com/thelastpickle/cassandra-reaper/issues/51)
to deploy new Docker images to Docker Hub on each release. Once this ticket has
been closed, the instructions should stay the same while the Dockerfile is
simpler and pre-built using Docker Hub's automated build system.
