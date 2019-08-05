#!/usr/bin/env bash
# Copyright 2017-2017 Spotify AB
# Copyright 2017-2018 The Last Pickle Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -ex

# build jar
# build web UI
# build Debian and RPM packages
# copy built packages into a mounted volume
cd ${WORKDIR}/cassandra-reaper
export VERSION=$(printf 'VER\t${project.version}' | mvn help:evaluate | grep '^VER' | cut -f2)
cd ${WORKDIR}/cassandra-reaper/src/packaging \
    && make build-packages \
    && mv *.deb *.rpm ${WORKDIR}/packages \
    && cp ../server/target/cassandra-*.jar ${WORKDIR}/packages \
    && rm ${WORKDIR}/packages/cassandra*-sources.jar

# execute any provided command
$@
