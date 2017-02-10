#!/usr/bin/env bash

set -x

# build rpm
make all
cp *.rpm ${WORKDIR}/packages

# cd into the directory that contains the built packages
cd ${WORKDIR}/packages

# execute any provided command
$@

