Dockerfile for Production
=========================

This directory contains an example Dockerfile that is ideal for running on
production clusters.

Once the Reaper build process is simplified and images are auto-published to
Docker Hub, this Dockerfile will be slightly simplified, but realistically the
same as what is provided here today.

Building
--------

In order to build the Docker image, we require two files in this directory:

* A pre-built Reaper jar file.
* A pre-configured Reaper yaml.

Example commands that would fulfill the above requirements are:

    cp ../../packages/cassandra-reaper-*.jar .
    cp ../../resource/cassandra-reaper-memory.yaml .

Once both files have been placed in this directory and modified to work with
with the production environment, run the following command to build the
container using the `reaper-production` tag:

    cd docker/production
    docker build --tag reaper-production .

Running
-------

In order to run the Docker container, run the command:

    docker run -ti -p "8080:8080" -p "8081:8081" reaper-production

Web Interface
-------------

The Reaper Web UI can be accessed from the local machine at:

* Main Interface: [http://localhost:8080/webui/index.html](http://localhost:8080/webui/index.html)
* Operational Interface: [http://localhost:8081](http://localhost:8081)
