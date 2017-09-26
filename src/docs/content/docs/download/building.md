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


To build Reaper without rebuilding the UI, run the following command : 

```mvn clean package```

To only regenerate the UI (requires npm and bower) : 

```mvn generate-sources -Pbuild-ui```

To rebuild both the UI and Reaper : 

```mvn clean package -Pbuild-ui```


# Building Docker Image


```mvn clean package docker:build```


# Building Using Docker

To simplify the build toolchain it's possible to build everything using Docker itself.  See the [Docker]({{<ref "docs/install/docker.md">}}) section for details.