#!/usr/bin/env bash

set -x

# build jar
# build web UI
# build Debian and RPM packages
# copy built packages into a mounted volume
mvn clean package -Pbuild-ui \
    && make all \
    && cp *.deb *.rpm target/*.jar ${WORKDIR}/packages

# cd into the directory that contains the built packages
cd ${WORKDIR}/packages

# execute any provided command
$@
