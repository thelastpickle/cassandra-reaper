#!/usr/bin/env bash
# Copyright 2017-2017 Spotify AB
# Copyright 2017-2019 The Last Pickle Ltd
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

arch=$(dpkg-architecture -q DEB_BUILD_ARCH)

# build jar
# build web UI
# build Debian and RPM packages
# copy built packages into a mounted volume
pushd ${WORKDIR}/cassandra-reaper > /dev/null
export VERSION=$(printf 'VER\t${project.version}' | mvn help:evaluate 2>/dev/null | grep '^VER' | cut -f2)
echo "Building package for version ${VERSION}"
java_home=""
java_path=""
javac_path=""
javadoc_path=""
java_home="/usr/lib/jvm/java-21-openjdk-${arch}"
java_path="bin/java"
javac_path="bin/javac"
javadoc_path="bin/javadoc"
export JAVA_HOME=${java_home}
update-alternatives --set java "${JAVA_HOME}/${java_path}"
update-alternatives --set javac "${JAVA_HOME}/${javac_path}"
update-alternatives --set javadoc "${JAVA_HOME}/${javadoc_path}"

make_tasks=()
# Check if the caller has asked us to build the JAR regardless of whether it exists already.
if [ "$1" = "forcebuild" ]
then
  make_tasks+=("package")
  shift
fi
make_tasks+=("build-packages")

pushd ${WORKDIR}/cassandra-reaper/src/packaging > /dev/null \
    && make ${make_tasks[*]} \
    && mv *.deb *.rpm ${WORKDIR}/packages \
    && cp ../server/target/cassandra-*.jar ${WORKDIR}/packages \
    && rm -f ${WORKDIR}/packages/cassandra*-sources.jar
