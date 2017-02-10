#!/usr/bin/env bash

set -x

# build jar
mvn clean package
mvn clean package -Pbuild-ui
cp ${WORKDIR}/cassandra-reaper/target/*.jar ${WORKDIR}/packages

# cd into the directory that contains the built packages
cd ${WORKDIR}/packages

# execute any provided command
$@
