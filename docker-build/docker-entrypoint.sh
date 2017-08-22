#!/usr/bin/env bash

set -ex

# build jar
# build web UI
# build Debian and RPM packages
# copy built packages into a mounted volume
cd ${WORKDIR}/cassandra-reaper
mvn clean package -Pbuild-ui \
    && make all \
    && mv *.deb *.rpm ${WORKDIR}/packages \
    && cp target/cassandra-*.jar ${WORKDIR}/packages

# execute any provided command
$@
