#!/bin/bash

echo "Starting Script step..."

set -xe

case "${TEST_TYPE}" in
    "")
        echo "ERROR: Environment variable TEST_TYPE is unspecified."
        exit 1
        ;;
    "ccm")
        ccm start -v
        sleep 30
        ccm status
        ccm node1 nodetool status
        if [ "${TRAVIS_BRANCH}" = "master" ]
            then
                VERSION=$(printf 'VER\t${project.version}' | mvn help:evaluate | grep '^VER' | cut -f2)
                DATE=$(date +"%Y%m%d")
                # Bintray doesn't like snapshots, but accepts betas :)
                BETA_VERSION=$(echo $VERSION | sed "s/SNAPSHOT/BETA/")
                mvn versions:set "-DnewVersion=${BETA_VERSION}-${DATE}"
        fi
        
        if [ "x${GRIM_MIN}" = "x" ]
        then
            # Rebuild the UI to get the right version number there
            MAVEN_OPTS="-Xmx1g" mvn clean generate-sources -Pbuild-ui > /dev/null
        fi

        MAVEN_OPTS="-Xmx1g" mvn clean install

        if [ "x${GRIM_MIN}" = "x" ]
        then
            mvn surefire:test -Dtest=ReaperIT
            mvn surefire:test -Dtest=ReaperH2IT
            mvn surefire:test -Dtest=ReaperPostgresIT
            mvn surefire:test -DsurefireArgLine="-Xmx1g" -Dtest=ReaperCassandraIT
        else
            mvn surefire:test -DsurefireArgLine="-Xmx1g" -Dtest=ReaperCassandraIT -Dgrim.reaper.min=${GRIM_MIN} -Dgrim.reaper.max=${GRIM_MAX}
        fi
        ;;
    "docker")
        docker-compose -f ./src/packaging/docker-build/docker-compose.yml build
        docker-compose -f ./src/packaging/docker-build/docker-compose.yml run build

        # Need to change the permissions after building the packages using the Docker image because they
        # are set to root and if left unchanged they will cause Maven to fail
        sudo chown -R travis:travis ./src/server/target/
        mvn -f src/server/pom.xml docker:build -Ddocker.directory=src/server/src/main/docker -DskipTests
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

            src/packaging/bin/spreaper add-cluster $(docker-compose -f ./src/packaging/docker-compose.yml run nodetool${docker_env} status | grep UN | tr -s ' ' | cut -d' ' -f2)
            sleep 5
            docker-compose -f ./src/packaging/docker-compose.yml down
        done
        ;;
    *)
        echo "Skipping, no actions for TEST_TYPE=${TEST_TYPE}."
esac