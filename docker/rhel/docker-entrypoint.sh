#!/usr/bin/env bash

set -x

make all
cp *.rpm ${WORKDIR}/packages
cd ${WORKDIR}/packages

$@
