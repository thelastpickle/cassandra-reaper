#!/bin/bash

echo "Starting Before Deploy step..."

set -xe
mkdir -p src/packages

if [ "${CASSANDRA_VERSION}" = "2.1.19" -a "x${GRIM_MIN}" = "x" ]
then
    if [ "${TRAVIS_BRANCH}" = "master" -a ! -d "cassandra-reaper-master" ]
    then
        VERSION=$(printf 'VER\t${project.version}' | mvn help:evaluate | grep '^VER' | cut -f2)
        DATE=$(date +"%Y%m%d")
        RELEASEDATE=$(date +"%Y-%m-%d")
        RPM_VERSION=$(echo "${VERSION}" | sed "s/-/_/")
        # Update Bintray descriptor files with appropriate version numbers and release dates
        sed -i "s/VERSION/${VERSION}/g" src/ci/descriptor-rpm-beta.json
        sed -i "s/RELEASEDATE/${RELEASEDATE}/" src/ci/descriptor-rpm-beta.json
        sed -i "s/VERSION/${VERSION}/g" src/ci/descriptor-deb-beta.json
        sed -i "s/RELEASEDATE/${RELEASEDATE}/" src/ci/descriptor-deb-beta.json
        sed -i "s/VERSION/${VERSION}/g" src/ci/descriptor-tarball-beta.json
        sed -i "s/RELEASEDATE/${RELEASEDATE}/" src/ci/descriptor-tarball-beta.json
        sed -i "s/VERSION/${VERSION}/g" src/ci/descriptor-maven-snapshot.json
        sed -i "s/RELEASEDATE/${RELEASEDATE}/" src/ci/descriptor-maven-snapshot.json
        mkdir -p cassandra-reaper-master/server/target
        cp -R src/packaging/bin cassandra-reaper-master/
        cp src/server/target/cassandra-reaper-*.jar cassandra-reaper-master/server/target
        cp -R src/packaging/resource cassandra-reaper-master/
        tar czf cassandra-reaper-${VERSION}.tar.gz cassandra-reaper-master/
        sudo mv cassandra-reaper-${VERSION}.tar.gz src/packages/
        # docker-compose based build stopped working so we fell back to raw fpm
        sudo apt-get install ruby ruby-dev build-essential rpm -y
        gem install --no-ri --no-rdoc fpm
        cd src/packaging
        make all
        sudo mv reaper_*_amd64.deb ../packages/
        sudo mv reaper-*.x86_64.rpm ../packages/
        export GIT_HASH=$(git log --pretty=format:'%h' -n 1)
        docker login -u $DOCKER_USER -p $DOCKER_PASS
        export REPO=thelastpickle/cassandra-reaper
        mvn -pl src/server/ docker:build -Ddocker.directory=src/server/src/main/docker
        docker tag cassandra-reaper:latest $REPO:master
        docker push $REPO:master
        docker tag cassandra-reaper:latest $REPO:$GIT_HASH
        docker push $REPO:$GIT_HASH
    fi
    if [ "x${TRAVIS_TAG}" != "x" -a ! -d "cassandra-reaper-${TRAVIS_TAG}" ]
    then
        VERSION=$(printf 'VER\t${project.version}' | mvn help:evaluate | grep '^VER' | cut -f2)
        RELEASEDATE=$(date +"%Y-%m-%d")
        # Update Bintray descriptor files with appropriate version numbers and release dates
        sed -i "s/VERSION/${VERSION}/g" src/ci/descriptor-rpm.json
        sed -i "s/RELEASEDATE/${RELEASEDATE}/" src/ci/descriptor-rpm.json
        sed -i "s/VERSION/${VERSION}/g" src/ci/descriptor-deb.json
        sed -i "s/RELEASEDATE/${RELEASEDATE}/" src/ci/descriptor-deb.json
        sed -i "s/VERSION/${VERSION}/g" src/ci/descriptor-tarball.json
        sed -i "s/RELEASEDATE/${RELEASEDATE}/" src/ci/descriptor-tarball.json
        sed -i "s/VERSION/${VERSION}/g" src/ci/descriptor-maven.json
        sed -i "s/RELEASEDATE/${RELEASEDATE}/" src/ci/descriptor-maven.json
        mkdir -p cassandra-reaper-${TRAVIS_TAG}/server/target
        cp -R src/packaging/bin cassandra-reaper-${TRAVIS_TAG}/
        cp src/server/target/cassandra-reaper-*.jar cassandra-reaper-${TRAVIS_TAG}/server/target
        cp -R src/packaging/resource cassandra-reaper-${TRAVIS_TAG}/
        tar czf cassandra-reaper-${TRAVIS_TAG}-release.tar.gz cassandra-reaper-${TRAVIS_TAG}/
        sudo mv cassandra-reaper-${TRAVIS_TAG}-release.tar.gz src/packages/
        # docker-compose based build stopped working so we fell back to raw fpm
        sudo apt-get install ruby ruby-dev build-essential rpm -y
        gem install --no-ri --no-rdoc fpm
        cd src/packaging
        make all
        sudo mv reaper_*_amd64.deb ../packages/
        sudo mv reaper-*.x86_64.rpm ../packages/
        docker login -u $DOCKER_USER -p $DOCKER_PASS
        export REPO=thelastpickle/cassandra-reaper
        mvn -pl src/server/ docker:build -Ddocker.directory=src/server/src/main/docker
        docker tag cassandra-reaper:latest $REPO:latest
        docker push $REPO:latest
        docker tag cassandra-reaper:latest $REPO:$TRAVIS_TAG
        docker push $REPO:$TRAVIS_TAG
    fi
fi
