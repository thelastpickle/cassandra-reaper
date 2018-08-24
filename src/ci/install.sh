#!/bin/bash
# Copyright 2017-2018 The Last Pickle Ltd
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

echo "Starting Install step..."

set -xe

case "${TEST_TYPE}" in
    "")
        echo "ERROR: Environment variable TEST_TYPE is unspecified."
        exit 1
        ;;
    "ccm")
        cp src/ci/jmxremote.password /home/travis/.local/jmxremote.password
        chmod 400 /home/travis/.local/jmxremote.password
        cat /usr/lib/jvm/java-8-oracle/jre/lib/management/jmxremote.access
        sudo chmod 777 /usr/lib/jvm/java-8-oracle/jre/lib/management/jmxremote.access
        echo "cassandra     readwrite" >> /usr/lib/jvm/java-8-oracle/jre/lib/management/jmxremote.access
        cat /usr/lib/jvm/java-8-oracle/jre/lib/management/jmxremote.access
        ccm create test -v $CASSANDRA_VERSION > /dev/null
        # use "2:0" to ensure the first datacenter name is "dc1" instead of "datacenter1", so to be compatible with CircleCI tests
        ccm populate --vnodes -n 2:0 > /dev/null
        sed -i 's/etc\/cassandra\/jmxremote.password/home\/travis\/.local\/jmxremote.password/' /home/travis/.ccm/test/node1/conf/cassandra-env.sh
        sed -i 's/etc\/cassandra\/jmxremote.password/home\/travis\/.local\/jmxremote.password/' /home/travis/.ccm/test/node2/conf/cassandra-env.sh
        sed -i 's/#MAX_HEAP_SIZE="4G"/MAX_HEAP_SIZE="192m"/' /home/travis/.ccm/test/node1/conf/cassandra-env.sh
        sed -i 's/#MAX_HEAP_SIZE="4G"/MAX_HEAP_SIZE="192m"/' /home/travis/.ccm/test/node2/conf/cassandra-env.sh
        sed -i 's/#HEAP_NEWSIZE="800M"/HEAP_NEWSIZE="100M"/' /home/travis/.ccm/test/node1/conf/cassandra-env.sh
        sed -i 's/#HEAP_NEWSIZE="800M"/HEAP_NEWSIZE="100M"/' /home/travis/.ccm/test/node2/conf/cassandra-env.sh
        sed -i 's/_timeout_in_ms:.*/_timeout_in_ms: 60000/' /home/travis/.ccm/test/node1/conf/cassandra.yaml
        sed -i 's/_timeout_in_ms:.*/_timeout_in_ms: 60000/' /home/travis/.ccm/test/node2/conf/cassandra.yaml
        sed -i 's/start_rpc: true/start_rpc: false/' /home/travis/.ccm/test/node1/conf/cassandra.yaml
        sed -i 's/start_rpc: true/start_rpc: false/' /home/travis/.ccm/test/node2/conf/cassandra.yaml
        sed -i 's/cross_node_timeout: false/cross_node_timeout: true/' /home/travis/.ccm/test/node1/conf/cassandra.yaml
        sed -i 's/cross_node_timeout: false/cross_node_timeout: true/' /home/travis/.ccm/test/node2/conf/cassandra.yaml
        sed -i 's/concurrent_reads: 32/concurrent_reads: 4/' /home/travis/.ccm/test/node1/conf/cassandra.yaml
        sed -i 's/concurrent_reads: 32/concurrent_reads: 4/' /home/travis/.ccm/test/node2/conf/cassandra.yaml
        sed -i 's/concurrent_writes: 32/concurrent_writes: 4/' /home/travis/.ccm/test/node1/conf/cassandra.yaml
        sed -i 's/concurrent_writes: 32/concurrent_writes: 4/' /home/travis/.ccm/test/node2/conf/cassandra.yaml
        sed -i 's/concurrent_counter_writes: 32/concurrent_counter_writes: 4/' /home/travis/.ccm/test/node1/conf/cassandra.yaml
        sed -i 's/concurrent_counter_writes: 32/concurrent_counter_writes: 4/' /home/travis/.ccm/test/node2/conf/cassandra.yaml
        sed -i 's/num_tokens: 256/num_tokens: 32/' /home/travis/.ccm/test/node1/conf/cassandra.yaml
        sed -i 's/num_tokens: 256/num_tokens: 32/' /home/travis/.ccm/test/node2/conf/cassandra.yaml
        ;;
    *)
        echo "Skipping, no actions for TEST_TYPE=${TEST_TYPE}."
esac
