+++
[menu.docs]
name = "Building from Source"
identifier = "building"
weight = 4
parent = "download"
+++

# Building Install Packages

Debian packages and RPMs can be built from this project using Make, for example:

```bash
make deb
make rpm
```

# Building JARs from source

To build use the following command:

```bash
mvn clean package
```

# Building Docker Image from source

 See the [Docker]({{<ref "docs/download/docker.md">}}) section for more details.

# Building Using Docker

To simplify the build toolchain it's possible to build everything using Docker itself. This is the process used to build the release binary artifacts from jar files to debian packages.

Building Reaper packages requires quite a few dependencies, especially when making changes to the web interface code. In an effort to simplify the build process, Dockerfiles have been created that implement the build actions required to package Reaper.

To build the JAR and other packages which are then placed in the _src/packages_ directory run the following commands:

```bash
cd src/packaging/docker-build
docker-compose build
docker-compose run build
```
