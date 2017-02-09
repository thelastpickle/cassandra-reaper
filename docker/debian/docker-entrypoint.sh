#!/usr/bin/env bash

set -x

# -d is required to avoid:
#    dpkg-checkbuilddeps: error: Unmet build dependencies: java7-jdk
debuild -uc -us -b -d
cp ${WORKDIR}/*.deb ${WORKDIR}/packages

# cd into the directory that contains the built packages
cd ${WORKDIR}/packages

# execute any provided command
$@
