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
JACOCO_VERSION="0.8.6"
REAPER_ENCRYPTION_KEY="SECRET_KEY"

set -xe

function set_java_home() {
    major_version=$1
    for jdk in /opt/hostedtoolcache/Java_Adopt_jdk/${major_version}*/; 
    do 
        export JAVA_HOME="${jdk/}"x64/
        echo "JAVA_HOME is set to $JAVA_HOME"
    done
}

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
    "ccm"|"elassandra")
        mvn --version -B
        ps uax | grep cass
        # dependending on the version of cassandra, we may need to use a different jdk
        set_java_home ${JDK_VERSION}
        ccm start -v --no-wait --skip-wait-other-notice || true
        echo "${TEST_TYPE}" | grep -q ccm && sleep 30 || sleep 120
        ccm status
        ccm node1 nodetool -- -u cassandra -pw cassandrapassword status
        # Reaper requires JDK11 for compilation
        set_java_home 11
        case "${STORAGE_TYPE}" in
            "")
                echo "ERROR: Environment variable STORAGE_TYPE is unspecified."
                exit 1
                ;;
            "local")
                mvn -B package -DskipTests
                mvn -B org.jacoco:jacoco-maven-plugin:${JACOCO_VERSION}:prepare-agent surefire:test -DsurefireArgLine="-Xmx256m"  -Dtest=ReaperShiroIT,ReaperIT,ReaperMetricsIT -Dcucumber.options="$CUCUMBER_OPTIONS" org.jacoco:jacoco-maven-plugin:${JACOCO_VERSION}:report
                ;;
            "cassandra"|"elassandra")
                ccm node1 cqlsh -e "DROP KEYSPACE reaper_db" || true
                mvn -B package -DskipTests
                mvn -B org.jacoco:jacoco-maven-plugin:${JACOCO_VERSION}:prepare-agent surefire:test -DsurefireArgLine="-Xmx384m" -Dtest=ReaperCassandraIT -Dgrim.reaper.min=${GRIM_MIN} -Dgrim.reaper.max=${GRIM_MAX} -Dcucumber.options="$CUCUMBER_OPTIONS" org.jacoco:jacoco-maven-plugin:${JACOCO_VERSION}:report
                ;;
            *)
                echo "Skipping, no actions for STORAGE_TYPE=${STORAGE_TYPE}."
                ;;
        esac

        ;;
    "sidecar")
        mvn --version -B
        mvn -B package -DskipTests
        # dependending on the version of cassandra, we may need to use a different jdk
        set_java_home ${JDK_VERSION}
        ccm start -v --no-wait --skip-wait-other-notice || true
        sleep 30
        ccm status
        set_java_home 11
        case "${STORAGE_TYPE}" in
            "")
                echo "ERROR: Environment variable STORAGE_TYPE is unspecified."
                exit 1
                ;;
            "cassandra")
                ccm node1 cqlsh -e "DROP KEYSPACE reaper_db" || true
                mvn -B org.jacoco:jacoco-maven-plugin:${JACOCO_VERSION}:prepare-agent surefire:test -DsurefireArgLine="-Xmx512m" -Dtest=ReaperCassandraSidecarIT -Dcucumber.options="$CUCUMBER_OPTIONS" org.jacoco:jacoco-maven-plugin:${JACOCO_VERSION}:report
                ;;
            *)
                echo "Skipping, no actions for STORAGE_TYPE=${STORAGE_TYPE}."
                ;;
        esac

        ;;
    "each")
        mvn --version -B
        mvn -B package -DskipTests
        # dependending on the version of cassandra, we may need to use a different jdk
        set_java_home ${JDK_VERSION}
        ccm start -v --no-wait --skip-wait-other-notice || true
        sleep 30
        ccm status
        set_java_home 11
        case "${STORAGE_TYPE}" in
            "")
                echo "ERROR: Environment variable STORAGE_TYPE is unspecified."
                exit 1
                ;;
            "cassandra")
                ccm node1 cqlsh -e "DROP KEYSPACE reaper_db" || true
                mvn -B org.jacoco:jacoco-maven-plugin:${JACOCO_VERSION}:prepare-agent surefire:test -DsurefireArgLine="-Xmx512m" -Dtest=ReaperCassandraEachIT -Dcucumber.options="$CUCUMBER_OPTIONS" org.jacoco:jacoco-maven-plugin:${JACOCO_VERSION}:report
                ;;
            *)
                echo "Skipping, no actions for STORAGE_TYPE=${STORAGE_TYPE}."
                ;;
        esac

        ;;
    "upgrade")
        mvn --version -B
        # dependending on the version of cassandra, we may need to use a different jdk
        set_java_home ${JDK_VERSION}
        ccm start -v --no-wait --skip-wait-other-notice || true
        sleep 30
        ccm status
        ccm node1 cqlsh -e "DROP KEYSPACE reaper_db" || true
        set_java_home 11
        mvn package -B -DskipTests -Pintegration-upgrade-tests
        MAVEN_OPTS="-Xmx384m" mvn -B surefire:test -Dtest=ReaperCassandraIT
        ;;
    "docker")
        sudo apt-get update
        sudo apt-get install jq -y
        mvn -B package -DskipTests
        docker-compose -f ./src/packaging/docker-build/docker-compose.yml build
        docker-compose -f ./src/packaging/docker-build/docker-compose.yml run build
        mvn -B -f src/server/pom.xml docker:build -Ddocker.directory=src/server/src/main/docker -DskipTests
        docker images

        # Clear out Cassandra data before starting a new cluster
        sudo rm -vfr ./src/packaging/data/

        docker-compose -f ./src/packaging/docker-compose.yml up -d cassandra
        sleep 30 && docker-compose -f ./src/packaging/docker-compose.yml run cqlsh-initialize-reaper_db
        sleep 10 && docker-compose -f ./src/packaging/docker-compose.yml up -d reaper
        docker ps -a

        # requests python package is needed to use spreaper
        pip install requests
        mkdir -p ~/.reaper
        echo "admin" > ~/.reaper/credentials
        sleep 30 && src/packaging/bin/spreaper login admin
        src/packaging/bin/spreaper add-cluster $(docker-compose -f ./src/packaging/docker-compose.yml run nodetool status | grep UN | tr -s ' ' | cut -d' ' -f2) 7199 > cluster.json
        cat cluster.json
        cluster_name=$(cat cluster.json|grep -v "#" | jq -r '.name')
        if [[ "$cluster_name" != "reaper-cluster" ]]; then
            echo "Failed registering cluster in Reaper running in Docker"
            exit 1
        fi
        sleep 5 && docker-compose -f ./src/packaging/docker-compose.yml down
        ;;
    *)
        echo "Skipping, no actions for TEST_TYPE=${TEST_TYPE}."
esac
