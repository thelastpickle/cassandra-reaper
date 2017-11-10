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

The easiest way to build is to use the following make command:

```bash
make package
```

To build Reaper without rebuilding the UI, run the following command:

```bash
mvn clean package
```

To only regenerate the UI (requires npm and bower):

```bash
mvn generate-sources -Pbuild-ui
```

To rebuild both the UI and Reaper:

```bash
mvn clean package -Pbuild-ui
```

# Building Docker Image from source

To build the Reaper docker image from source, run the following command:

```bash
mvn clean package -pl src/server/ docker:build -Ddocker.directory=src/server/src/main/docker
```

# Building Using Docker

To simplify the build toolchain it's possible to build everything using Docker itself.  See the [Docker]({{<ref "docs/download/docker.md">}}) section for details.