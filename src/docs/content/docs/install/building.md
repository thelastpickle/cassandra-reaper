+++
[menu.docs]
name = "Building from Source"
identifier = "building"
weight = 4
parent = "download_install"
+++

## Building Packages

Debian and RPM packages can be built from this project using Make, for example:

```bash
make deb
make rpm
```

## Docker


A [Docker](https://docs.docker.com/engine/installation/) build environment is
also provided in the `src/packaging` directory to build the entire project and can be run by using
[Docker Compose](https://docs.docker.com/compose/install/):

```bash
docker-compose -f docker-build/docker-compose.yml build \
    && docker-compose -f docker-build/docker-compose.yml run build
```

The final packages will be located within:

`./packages/`


## Building from source

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

To build the docker image :

```mvn clean package docker:build```