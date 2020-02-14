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
if [ "${GITHUB_REF}" = "refs/heads/master" ]
then
    mkdir -p cassandra-reaper-master/server/target
    cp -R src/packaging/bin cassandra-reaper-master/
    cp src/server/target/cassandra-reaper-*.jar cassandra-reaper-master/server/target
    cp -R src/packaging/resource cassandra-reaper-master/
    # docker-compose based build stopped working so we fell back to raw fpm
    sudo apt-get install ruby ruby-dev build-essential rpm -y
    sudo gem install --no-ri --no-rdoc fpm
    cd src/packaging
    make all
    sudo mv reaper_*_amd64.deb ../packages/
    sudo mv reaper-*.x86_64.rpm ../packages/
    cd ../..
    tar czf cassandra-reaper-${VERSION}.tar.gz cassandra-reaper-master/
    sudo mv cassandra-reaper-${VERSION}.tar.gz src/packages/
    export GIT_HASH=$(git log --pretty=format:'%h' -n 1)
    docker login -u $DOCKER_USER -p $DOCKER_PASS
    export REPO=thelastpickle/cassandra-reaper
    mvn -B -pl src/server/ docker:build -Ddocker.directory=src/server/src/main/docker
    docker tag cassandra-reaper:latest $REPO:master
    #docker push $REPO:master
    docker tag cassandra-reaper:latest $REPO:$GIT_HASH
    #docker push $REPO:$GIT_HASH
fi
if [[ ${GITHUB_REF} =~ ^[0-9]{1}\.[0-9]{1}\.[0-9]{1}$ ]]
then
    mkdir -p cassandra-reaper-${VERSION}/server/target
    cp -R src/packaging/bin cassandra-reaper-${VERSION}/
    cp src/server/target/cassandra-reaper-*.jar cassandra-reaper-${VERSION}/server/target
    cp -R src/packaging/resource cassandra-reaper-${VERSION}/
    sudo apt-get install ruby ruby-dev build-essential rpm -y
    sudo gem install --no-ri --no-rdoc fpm
    cd src/packaging
    make all
    sudo mv reaper_*_amd64.deb ../packages/
    sudo mv reaper-*.x86_64.rpm ../packages/
    cd ../..
    tar czf cassandra-reaper-${VERSION}-release.tar.gz cassandra-reaper-${VERSION}/
    sudo mv cassandra-reaper-${VERSION}-release.tar.gz src/packages/
    docker login -u $DOCKER_USER -p $DOCKER_PASS
    export REPO=thelastpickle/cassandra-reaper
    mvn -B -pl src/server/ docker:build -Ddocker.directory=src/server/src/main/docker
    docker tag cassandra-reaper:latest $REPO:latest
    #docker push $REPO:latest
    docker tag cassandra-reaper:latest $REPO:$VERSION
    #docker push $REPO:$VERSION
fi
