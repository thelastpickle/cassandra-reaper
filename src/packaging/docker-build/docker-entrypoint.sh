#!/usr/bin/env bash

set -ex

# build jar
# build web UI
# build Debian and RPM packages
# copy built packages into a mounted volume
cd ${WORKDIR}/cassandra-reaper/src/packaging \
    && make all \
    && mv *.deb *.rpm ${WORKDIR}/packages \
    && cp ../server/target/cassandra-*.jar ${WORKDIR}/packages \
    && rm ${WORKDIR}/packages/cassandra*-sources.jar

# execute any provided command
$@
