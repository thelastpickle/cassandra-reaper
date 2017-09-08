#!/bin/bash

echo "Starting After Success step..."

set -xe

case "${TEST_TYPE}" in
    "")
        echo "ERROR: Environment variable TEST_TYPE is unspecified."
        exit 1
        ;;
    "ccm")
        if [ "${TRAVIS_PULL_REQUEST}" = "false" -a "${TRAVIS_BRANCH}" = "master" -a "${CASSANDRA_VERSION}" = "2.1.18" ]
        then
            mvn sonar:sonar \
                -Dsonar.host.url=https://sonarqube.com \
                -Dsonar.login=${SONAR_TOKEN} \
                -Dsonar.projectKey=tlp-cassandra-reaper \
                -Dsonar.github.oauth=${GITHUB_TOKEN} \
                -Dsonar.github.repository=thelastpickle/cassandra-reaper
        fi
        ;;
    *)
        echo "Skipping, no actions for TEST_TYPE=${TEST_TYPE}."
esac