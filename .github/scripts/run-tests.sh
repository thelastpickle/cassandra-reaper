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
    for jdk in /opt/hostedtoolcache/Java_Temurin-Hotspot_jdk/${major_version}*/*/;
    do 
        export JAVA_HOME="${jdk/}"
        echo "JAVA_HOME is set to $JAVA_HOME"
    done
}

add_management_api () {
   if [[ ! -L /tmp/datastax-mgmtapi-agent.jar ]]; then
     # Do some fancy pom.xml parsing to figure out which version of the Management API client we are using
     MGMT_API_VERSION=`mvn dependency:tree -f src/server/pom.xml |grep datastax-mgmtapi-client-openapi|cut -d ":" -f 4`
     # Download the Management API bundle
     mvn dependency:copy -Dartifact=io.k8ssandra:datastax-mgmtapi-server:$MGMT_API_VERSION -f src/server/pom.xml -DoutputDirectory=/tmp -Dmdep.stripVersion=true -Dmdep.overWriteReleases=true
     # Unzip the agent for the version of Cassandra
     if [[ "$CASSANDRA_VERSION" == *"3.11"* ]]; then
        mvn dependency:copy -Dartifact=io.k8ssandra:datastax-mgmtapi-agent-3.x:$MGMT_API_VERSION -f src/server/pom.xml -DoutputDirectory=/tmp -Dmdep.stripVersion=true -Dmdep.overWriteReleases=true
        ln -s /tmp/datastax-mgmtapi-agent-3.x.jar /tmp/datastax-mgmtapi-agent.jar
     elif [[ "$CASSANDRA_VERSION" == *"4.0"* ]]; then
        mvn dependency:copy -Dartifact=io.k8ssandra:datastax-mgmtapi-agent-4.x:$MGMT_API_VERSION -f src/server/pom.xml -DoutputDirectory=/tmp -Dmdep.stripVersion=true -Dmdep.overWriteReleases=true
        ln -s /tmp/datastax-mgmtapi-agent-4.x.jar /tmp/datastax-mgmtapi-agent.jar
     elif [[ "$CASSANDRA_VERSION" == *"4.1"* ]]; then
        mvn dependency:copy -Dartifact=io.k8ssandra:datastax-mgmtapi-agent-4.1.x:$MGMT_API_VERSION -f src/server/pom.xml -DoutputDirectory=/tmp -Dmdep.stripVersion=true -Dmdep.overWriteReleases=true
        ln -s /tmp/datastax-mgmtapi-agent-4.1.x.jar /tmp/datastax-mgmtapi-agent.jar
     fi
   fi
  echo "JVM_OPTS=\"\$JVM_OPTS -javaagent:/tmp/datastax-mgmtapi-agent.jar\"" >> ~/.ccm/test/node$1/conf/cassandra-env.sh
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
                mvn -B org.jacoco:jacoco-maven-plugin:${JACOCO_VERSION}:prepare-agent surefire:test -DsurefireArgLine="-Xmx256m"  -Dtest=ReaperShiroIT -Dcucumber.options="$CUCUMBER_OPTIONS" org.jacoco:jacoco-maven-plugin:${JACOCO_VERSION}:report
                mvn -B org.jacoco:jacoco-maven-plugin:${JACOCO_VERSION}:prepare-agent surefire:test -DsurefireArgLine="-Xmx256m"  -Dtest=ReaperIT -Dcucumber.options="$CUCUMBER_OPTIONS" org.jacoco:jacoco-maven-plugin:${JACOCO_VERSION}:report
                ;;
            "cassandra"|"elassandra")
                ccm node1 cqlsh -e "DROP KEYSPACE reaper_db" || true
                mvn -B package -DskipTests
                mvn -B org.jacoco:jacoco-maven-plugin:${JACOCO_VERSION}:prepare-agent surefire:test -DsurefireArgLine="-Xmx384m" -Dtest=ReaperCassandraIT -Dgrim.reaper.min=${GRIM_MIN} -Dgrim.reaper.max=${GRIM_MAX} -Dcucumber.options="$CUCUMBER_OPTIONS" org.jacoco:jacoco-maven-plugin:${JACOCO_VERSION}:report
                mvn -B org.jacoco:jacoco-maven-plugin:${JACOCO_VERSION}:prepare-agent surefire:test -DsurefireArgLine="-Xmx384m" -Dtest=ReaperMetricsIT -Dgrim.reaper.min=${GRIM_MIN} -Dgrim.reaper.max=${GRIM_MAX} -Dcucumber.options="$CUCUMBER_OPTIONS" org.jacoco:jacoco-maven-plugin:${JACOCO_VERSION}:report
                ;;
            *)
                echo "Skipping, no actions for STORAGE_TYPE=${STORAGE_TYPE}."
                ;;
        esac

        ;;
    "http-api")
            mvn --version -B
            ps uax | grep cass
            # dependending on the version of cassandra, we may need to use a different jdk
            set_java_home ${JDK_VERSION}
            # Add in  Management API agent jarfile
            for i in `seq 1 2` ; do
              add_management_api $i
              mkdir -p /tmp/log/cassandra$i/ && touch /tmp/log/cassandra$i/stdout.log
            done
            ccm start -v --no-wait --skip-wait-other-notice || true
            echo "${TEST_TYPE}" | grep -q ccm && sleep 30 || sleep 120
            ccm status
            ccm node1 nodetool -- -u cassandra -pw cassandrapassword status
            # Stop CCM now so we can restart it with Management API
            ccm stop
            # Start Management API
            CERT_DIR=/home/runner/work/cassandra-reaper/cassandra-reaper/.github/files
            MGMT_API_LOG_DIR=/tmp/log/cassandra1 MGMT_API_TLS_CA_CERT_FILE=$CERT_DIR/mutual_auth_ca.pem MGMT_API_TLS_CERT_FILE=$CERT_DIR/mutual_auth_server.crt MGMT_API_TLS_KEY_FILE=$CERT_DIR/mutual_auth_server.key bash -c 'nohup java -jar /tmp/datastax-mgmtapi-server.jar --tlscacert=$MGMT_API_TLS_CA_CERT_FILE --tlscert=$MGMT_API_TLS_CERT_FILE --tlskey=$MGMT_API_TLS_KEY_FILE --db-socket=/tmp/db1.sock --host=unix:///tmp/mgmtapi1.sock --host=http://127.0.0.1:8080 --db-home=`dirname ~/.ccm/test/node1`/node1 > /tmp/log/cassandra1/mgmt.out 2>&1 &'
            MGMT_API_LOG_DIR=/tmp/log/cassandra2 MGMT_API_TLS_CA_CERT_FILE=$CERT_DIR/mutual_auth_ca.pem MGMT_API_TLS_CERT_FILE=$CERT_DIR/mutual_auth_server.crt MGMT_API_TLS_KEY_FILE=$CERT_DIR/mutual_auth_server.key bash -c 'nohup java -jar /tmp/datastax-mgmtapi-server.jar --tlscacert=$MGMT_API_TLS_CA_CERT_FILE --tlscert=$MGMT_API_TLS_CERT_FILE --tlskey=$MGMT_API_TLS_KEY_FILE --db-socket=/tmp/db2.sock --host=unix:///tmp/mgmtapi2.sock --host=http://127.0.0.2:8080 --db-home=`dirname ~/.ccm/test/node2`/node2 > /tmp/log/cassandra2/mgmt.out 2>&1 &'
            # wait for Cassandra to be ready
            for i in `seq 1 30` ; do
                # keep curl from exiting with non-zero
                HTTPCODE1=`curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8080/api/v0/probes/readiness` || true
                HTTPCODE2=`curl -s -o /dev/null -w "%{http_code}" http://127.0.0.2:8080/api/v0/probes/readiness` || true
                if [ "${HTTPCODE1}" != "200" -o "${HTTPCODE2}" != "200" ]
                then
                    echo "Cassandra not ready yet. Sleeping.... $i"
                else
                    echo "Cassandra started via Management API successfully"
                    break
                fi
                sleep 5
            done
            # Reaper requires JDK11 for compilation
            set_java_home 11
            case "${STORAGE_TYPE}" in
                "")
                    echo "ERROR: Environment variable STORAGE_TYPE is unspecified."
                    exit 1
                    ;;
                "ccm")
                    mvn -B package -DskipTests
                    ccm node1 cqlsh -e "DROP KEYSPACE reaper_db" || true
                    mvn -B org.jacoco:jacoco-maven-plugin:${JACOCO_VERSION}:prepare-agent surefire:test -DsurefireArgLine="-Xmx256m"  -Dtest=ReaperHttpIT -Dcucumber.options="$CUCUMBER_OPTIONS" org.jacoco:jacoco-maven-plugin:${JACOCO_VERSION}:report
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
        VERSION=$(printf 'VER\t${project.version}' | mvn help:evaluate | grep '^VER' | cut -f2)
        docker build --build-arg SHADED_JAR=src/server/target/cassandra-reaper-${VERSION}.jar -f src/server/src/main/docker/Dockerfile -t thelastpickle/cassandra-reaper:ci-build .
        docker images

        # start a kind cluster with 2 worker nodes
        cat <<EOF > /tmp/kind-config.yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
EOF

        kind delete cluster --name reaper
        kind create cluster --name reaper --config /tmp/kind-config.yaml

        kind load docker-image thelastpickle/cassandra-reaper:ci-build --name reaper

        # Install cert-manager and cass-operator
        kubectl apply -f https://github.com/jetstack/cert-manager/releases/download/v1.17.0/cert-manager.yaml
        kubectl wait --for=condition=available --timeout=600s deployment/cert-manager -n cert-manager
        kubectl wait --for=condition=available --timeout=600s deployment/cert-manager-webhook -n cert-manager
        kubectl apply --force-conflicts --server-side -k github.com/k8ssandra/cass-operator/config/deployments/default?ref=v1.23.2
        kubectl wait --for=condition=available --timeout=600s deployment/cass-operator-controller-manager -n cass-operator
        if [ $? -ne 0 ]; then
            echo "cass-operator failed to be ready"
            exit 1
        fi

        # Create CassandraDatacenter
        kubectl apply -f .github/files/reaper-cql-secret.yaml
        kubectl apply -f .github/files/reaper-ui-secret.yaml
        kubectl apply -f .github/files/cassdc.yaml

        # Wait for the Cassandra statefulset to be created
        for i in `seq 1 30` ; do
            DC_STS=`kubectl get sts/test-dc1-r1-sts -n cass-operator | wc -l`
            if [ "${DC_STS}" != "0" ]
            then
                echo "Cassandra statefulset created successfully"
                break
            else
                echo "Cassandra statefulset not created yet. Sleeping.... $i"
            fi
            sleep 10
        done

        # Wait for the Cassandra statefulset to be ready
        kubectl rollout status --watch --timeout=600s statefulset/test-dc1-r1-sts -n cass-operator
        if [ $? -ne 0 ]; then
            echo "Cassandra statefulset failed to be ready"
            exit 1
        fi

        # Create the reaper_db keyspace
        REAPER_USERNAME=$(kubectl get secret reaper-cql-secret -n cass-operator -o jsonpath='{.data.username}' | base64 --decode | xargs)
        REAPER_PASSWORD=$(kubectl get secret reaper-cql-secret -n cass-operator -o jsonpath='{.data.password}' | base64 --decode | xargs)
        kubectl exec -it test-dc1-r1-sts-0 -n cass-operator -- cqlsh -u REAPER_USERNAME -p REAPER_PASSWORD -e "CREATE KEYSPACE reaper_db WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};"
        # Create Reaper deployment
        kubectl apply -f .github/files/reaper.yaml
        # Wait for the Reaper deployment to be ready
        kubectl rollout status --watch --timeout=600s deployment/test-dc1-reaper -n cass-operator
        if [ $? -ne 0 ]; then
            echo "Reaper deployment failed to be ready"
            exit 1
        fi
        echo "Reaper deployment created successfully"
        ;;
    *)
        echo "Skipping, no actions for TEST_TYPE=${TEST_TYPE}."
esac
