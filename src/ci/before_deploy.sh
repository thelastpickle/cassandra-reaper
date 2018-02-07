#!/bin/bash

echo "Starting Before Deploy step..."

set -xe

if [ "${CASSANDRA_VERSION}" = "2.1.19" -a "x${GRIM_MIN}" = "x" ]
then
    if [ "${TRAVIS_BRANCH}" = "master" -a ! -d "cassandra-reaper-master" ]
    then
        VERSION=$(printf 'VER\t${project.version}' | mvn help:evaluate | grep '^VER' | cut -f2)
        DATE=$(date +"%Y%m%d")
        RELEASEDATE=$(date +"%Y-%m-%d")
        RPM_VERSION=$(echo "${VERSION}" | sed "s/-/_/")
        # Update Bintray descriptor files with appropriate version numbers and release dates
        sed -i "s/VERSION/${VERSION}/" src/ci/descriptor-rpm-beta.json
        sed -i "s/RELEASEDATE/${RELEASEDATE}/" src/ci/descriptor-rpm-beta.json
        sed -i "s/VERSION/${VERSION}/" src/ci/descriptor-deb-beta.json
        sed -i "s/RELEASEDATE/${RELEASEDATE}/" src/ci/descriptor-deb-beta.json
        sed -i "s/VERSION/${VERSION}/" src/ci/descriptor-tarball-beta.json
        sed -i "s/RELEASEDATE/${RELEASEDATE}/" src/ci/descriptor-tarball-beta.json
        mkdir -p cassandra-reaper-master/server/target
        cp -R src/packaging/bin cassandra-reaper-master/
        cp src/server/target/cassandra-reaper-*.jar cassandra-reaper-master/server/target
        cp -R src/packaging/resource cassandra-reaper-master/
        tar czf cassandra-reaper-${VERSION}.tar.gz cassandra-reaper-master/
        docker-compose -f src/packaging/docker-build/docker-compose.yml build &> /dev/null
        docker-compose -f src/packaging/docker-build/docker-compose.yml run build &> /dev/null
        # Renaming the packages to avoid conflicts in Bintray
        sudo mv cassandra-reaper-${VERSION}.tar.gz src/packages/
        #sudo mv src/packages/reaper_${VERSION}_amd64.deb src/packages/reaper_${VERSION}-${DATE}_amd64.deb
        #sudo mv src/packages/reaper-*.x86_64.rpm src/packages/reaper-${RPM_VERSION}.x86_64.rpm
    fi
    if [ "x${TRAVIS_TAG}" != "x" -a ! -d "cassandra-reaper-${TRAVIS_TAG}" ]
    then
        VERSION=$(printf 'VER\t${project.version}' | mvn help:evaluate | grep '^VER' | cut -f2)
        RELEASEDATE=$(date +"%Y-%m-%d")
        # Update Bintray descriptor files with appropriate version numbers and release dates
        sed -i "s/VERSION/${VERSION}/" src/ci/descriptor-rpm.json
        sed -i "s/RELEASEDATE/${RELEASEDATE}/" src/ci/descriptor-rpm.json
        sed -i "s/VERSION/${VERSION}/" src/ci/descriptor-deb.json
        sed -i "s/RELEASEDATE/${RELEASEDATE}/" src/ci/descriptor-deb.json
        sed -i "s/VERSION/${VERSION}/" src/ci/descriptor-tarball.json
        sed -i "s/RELEASEDATE/${RELEASEDATE}/" src/ci/descriptor-tarball.json
        mkdir -p cassandra-reaper-${TRAVIS_TAG}/server/target
        cp -R src/packaging/bin cassandra-reaper-${TRAVIS_TAG}/
        cp src/server/target/cassandra-reaper-*.jar cassandra-reaper-${TRAVIS_TAG}/server/target
        cp -R src/packaging/resource cassandra-reaper-${TRAVIS_TAG}/
        tar czf cassandra-reaper-${TRAVIS_TAG}-release.tar.gz cassandra-reaper-${TRAVIS_TAG}/
        sudo mv cassandra-reaper-${TRAVIS_TAG}-release.tar.gz src/packages/
        docker-compose -f src/packaging/docker-build/docker-compose.yml build &> /dev/null
        docker-compose -f src/packaging/docker-build/docker-compose.yml run build &> /dev/null
    fi
fi