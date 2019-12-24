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

echo "Starting Script step..."

set -xe

case "${TEST_TYPE}" in
    "")
        echo "ERROR: Environment variable TEST_TYPE is unspecified."
        exit 1
        ;;
    "deploy")
        mvn --version -B
        if [ "${TRAVIS_BRANCH}" = "master" ]
            then
                VERSION=$(printf 'VER\t${project.version}' | mvn help:evaluate | grep '^VER' | cut -f2)
                DATE=$(date +"%Y%m%d")
                # Bintray doesn't like snapshots, but accepts betas :)
                BETA_VERSION=$(echo $VERSION | sed "s/SNAPSHOT/BETA/")
                mvn -B versions:set "-DnewVersion=${BETA_VERSION}-${DATE}"
        fi

        mvn -B install -DskipTests
        ;;
    "ccm")
        mvn --version -B
        ccm start
        sleep 30
        ccm status

        case "${STORAGE_TYPE}" in
            "")
                echo "ERROR: Environment variable STORAGE_TYPE is unspecified."
                exit 1
                ;;
            "local")
                mvn -B package
                mvn -B surefire:test -DsurefireArgLine="-Xmx256m"  -Dtest=ReaperShiroIT
                mvn -B surefire:test -DsurefireArgLine="-Xmx256m"  -Dtest=ReaperIT
                mvn -B surefire:test -DsurefireArgLine="-Xmx256m"  -Dtest=ReaperH2IT
                ;;
            "postgresql")
                mvn -B package -DskipTests
                mvn -B surefire:test -DsurefireArgLine="-Xmx384m" -Dtest=ReaperPostgresIT -Dgrim.reaper.min=${GRIM_MIN} -Dgrim.reaper.max=${GRIM_MAX}
                ;;
            "cassandra")
                mvn -B package -DskipTests
                mvn -B surefire:test -DsurefireArgLine="-Xmx384m" -Dtest=ReaperCassandraIT -Dgrim.reaper.min=${GRIM_MIN} -Dgrim.reaper.max=${GRIM_MAX}
                ;;
            *)
                echo "Skipping, no actions for STORAGE_TYPE=${STORAGE_TYPE}."
                ;;
        esac

        ;;
    "sidecar")
        mvn --version -B
        mvn -B package -DskipTests
        ccm start
        sleep 30
        ccm status

        case "${STORAGE_TYPE}" in
            "")
                echo "ERROR: Environment variable STORAGE_TYPE is unspecified."
                exit 1
                ;;
            "postgresql")
                mvn -B surefire:test -DsurefireArgLine="-Xmx512m" -Dtest=ReaperPostgresSidecarIT -Dcucumber.options="-t @sidecar"
                ;;
            "cassandra")
                mvn -B surefire:test -DsurefireArgLine="-Xmx512m" -Dtest=ReaperCassandraSidecarIT -Dcucumber.options="-t @sidecar"
                ;;
            *)
                echo "Skipping, no actions for STORAGE_TYPE=${STORAGE_TYPE}."
                ;;
        esac

        ;;
    "upgrade")
        mvn --version -B
        ccm start
        sleep 30
        ccm status

        mvn package -B -DskipTests -Pintegration-upgrade-tests
        MAVEN_OPTS="-Xmx384m" mvn -B surefire:test -Dtest=ReaperCassandraIT
        ;;
    "docker")
        mvn -B package -DskipTests
        docker-compose -f ./src/packaging/docker-build/docker-compose.yml build
        docker-compose -f ./src/packaging/docker-build/docker-compose.yml run build

        # Need to change the permissions after building the packages using the Docker image because they
        # are set to root and if left unchanged they will cause Maven to fail
        sudo chown -R travis:travis ./src/server/target/
        mvn -B -f src/server/pom.xml dockerfile:build -DskipTests
        docker images

        # Generation of SSL stores - this can be done at any point in time prior to running setting up the SSL environment
        docker-compose -f ./src/packaging/docker-compose.yml run generate-ssl-stores

        # Test default environment then test SSL encrypted environment
        for docker_env in "" "-ssl"
        do
            # Clear out Cassandra data before starting a new cluster
            sudo rm -vfr ./src/packaging/data/

            docker-compose -f ./src/packaging/docker-compose.yml up -d cassandra${docker_env}
            sleep 30
            docker-compose -f ./src/packaging/docker-compose.yml run cqlsh-initialize-reaper_db${docker_env}
            sleep 10
            docker-compose -f ./src/packaging/docker-compose.yml up -d reaper${docker_env}
            sleep 30
            docker ps -a

            echo "admin" > ~/.reaper/credentials
            src/packaging/bin/spreaper login admin
            src/packaging/bin/spreaper add-cluster $(docker-compose -f ./src/packaging/docker-compose.yml run nodetool${docker_env} status | grep UN | tr -s ' ' | cut -d' ' -f2) 7199
            sleep 5
            docker-compose -f ./src/packaging/docker-compose.yml down
        done
        ;;
    *)
        echo "Skipping, no actions for TEST_TYPE=${TEST_TYPE}."
esac
