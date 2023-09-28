#!/bin/bash
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

echo "Starting Before Deploy step..."

set -xe
mkdir -p src/packages

export VERSION=$(printf 'VER\t${project.version}' | mvn help:evaluate | grep '^VER' | cut -f2)

# Used for the docker image build
echo "SHADED_JAR=src/server/target/cassandra-reaper-${VERSION}.jar" >> $GITHUB_ENV

if [ "${GITHUB_REF}" = "refs/heads/master" ]
then
    mkdir -p cassandra-reaper-master/server/target
    cp -R src/packaging/bin cassandra-reaper-master/
    cp src/server/target/cassandra-reaper-*.jar cassandra-reaper-master/server/target
    cp -R src/packaging/resource cassandra-reaper-master/
    # docker-compose based build stopped working so we fell back to raw fpm
    sudo apt-get install ruby ruby-dev build-essential rpm -y
    sudo gem install --no-document fpm
    cd src/packaging
    make all
    sudo mv reaper_*_*.deb ../packages/
    sudo mv reaper-*.*.rpm ../packages/
    cd ../..
    tar czf cassandra-reaper-${VERSION}.tar.gz cassandra-reaper-master/
    sudo mv cassandra-reaper-${VERSION}.tar.gz src/packages/
    export GIT_HASH=$(git log --pretty=format:'%h' -n 1)
    export REPO=thelastpickle/cassandra-reaper
    echo "DOCKER_TAG1=$REPO:master" >> $GITHUB_ENV
    echo "DOCKER_TAG2=$REPO:$GIT_HASH" >> $GITHUB_ENV
fi
if [[ ${GITHUB_REF} == "refs/tags"* ]]
then
    mkdir -p cassandra-reaper-${VERSION}/server/target
    cp -R src/packaging/bin cassandra-reaper-${VERSION}/
    cp src/server/target/cassandra-reaper-*.jar cassandra-reaper-${VERSION}/server/target
    cp -R src/packaging/resource cassandra-reaper-${VERSION}/
    sudo apt-get install ruby ruby-dev build-essential rpm -y
    sudo gem install --no-document fpm
    cd src/packaging
    make all
    sudo mv reaper_*_*.deb ../packages/
    sudo mv reaper-*.*.rpm ../packages/
    cd ../..
    tar czf cassandra-reaper-${VERSION}-release.tar.gz cassandra-reaper-${VERSION}/
    sudo mv cassandra-reaper-${VERSION}-release.tar.gz src/packages/
    export REPO=thelastpickle/cassandra-reaper
    echo "DOCKER_TAG1=$REPO:master" >> $GITHUB_ENV
    echo "DOCKER_TAG2=$REPO:$VERSION" >> $GITHUB_ENV
fi
