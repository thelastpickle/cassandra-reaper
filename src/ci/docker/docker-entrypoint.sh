#!/bin/sh
# Copyright 2019-2019 The Last Pickle Ltd
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
set -x
mkdir -p ~/.local
cp src/ci/jmxremote.password ~/.local/jmxremote.password
touch ~/.local/jmxremote.blank.password
chmod 400 ~/.local/jmxremote*.password
cat /usr/lib/jvm/java-8-openjdk-amd64/jre/lib/management/jmxremote.access
sudo chmod 777 /usr/lib/jvm/java-8-openjdk-amd64/jre/lib/management/jmxremote.access
echo "cassandra     readwrite" >> /usr/lib/jvm/java-8-openjdk-amd64/jre/lib/management/jmxremote.access
cat /usr/lib/jvm/java-8-openjdk-amd64/jre/lib/management/jmxremote.access
ccm create test -v $CASSANDRA_VERSION
ccm populate --vnodes -n $NODES_PER_DC:$NODES_PER_DC
for i in `seq 1 $(($NODES_PER_DC *2))` ; do
    if [ "$i" -le "$NODES_PER_DC" ] ; then
        sed -i 's/etc\/cassandra\/jmxremote.password/home\/circleci\/.local\/jmxremote.password/' ~/.ccm/test/node$i/conf/cassandra-env.sh
    else
        if  echo "$JOB_COMMAND" | grep -q "Pskip-tests-needing-all-nodes-reachable"  ; then
            # scenarios that are not tagged with @all_nodes_reachable can be tested against an unreachable DC2
            sed -i 's/etc\/cassandra\/jmxremote.password/home\/circleci\/.local\/jmxremote.blank.password/' ~/.ccm/test/node$i/conf/cassandra-env.sh
        else
            # @all_nodes_reachable scenarios need all datacenters+nodes reachable
            sed -i 's/etc\/cassandra\/jmxremote.password/home\/circleci\/.local\/jmxremote.password/' ~/.ccm/test/node$i/conf/cassandra-env.sh
        fi
    fi
    sed -i 's/#MAX_HEAP_SIZE="4G"/MAX_HEAP_SIZE="136m"/' ~/.ccm/test/node$i/conf/cassandra-env.sh
    sed -i 's/#HEAP_NEWSIZE="800M"/HEAP_NEWSIZE="112M"/' ~/.ccm/test/node$i/conf/cassandra-env.sh
    sed -i 's/_timeout_in_ms:.*/_timeout_in_ms: 60000/' ~/.ccm/test/node$i/conf/cassandra.yaml
    sed -i 's/cross_node_timeout: false/cross_node_timeout: true/' ~/.ccm/test/node$i/conf/cassandra.yaml
    sed -i 's/concurrent_reads: 32/concurrent_reads: 4/' ~/.ccm/test/node$i/conf/cassandra.yaml
    sed -i 's/concurrent_writes: 32/concurrent_writes: 4/' ~/.ccm/test/node$i/conf/cassandra.yaml
    sed -i 's/concurrent_counter_writes: 32/concurrent_counter_writes: 4/' ~/.ccm/test/node$i/conf/cassandra.yaml
    sed -i 's/num_tokens: 256/num_tokens: 4/' ~/.ccm/test/node$i/conf/cassandra.yaml
    sed -i 's/auto_snapshot: true/auto_snapshot: false/' ~/.ccm/test/node$i/conf/cassandra.yaml
    sed -i 's/enable_materialized_views: true/enable_materialized_views: false/' ~/.ccm/test/node$i/conf/cassandra.yaml
    sed -i 's/internode_compression: dc/internode_compression: none/' ~/.ccm/test/node$i/conf/cassandra.yaml
    echo 'phi_convict_threshold: 16' >> ~/.ccm/test/node$i/conf/cassandra.yaml
    sed -i 's/# file_cache_size_in_mb: 512/file_cache_size_in_mb: 1/' ~/.ccm/test/node$i/conf/cassandra.yaml
    if  echo "$CASSANDRA_VERSION" | grep -q "trunk"  ; then
        sed -i 's/start_rpc: true//' ~/.ccm/test/node$i/conf/cassandra.yaml
        echo '-Dcassandra.max_local_pause_in_ms=15000' >> ~/.ccm/test/node$i/conf/jvm-server.options
        sed -i 's/#-Dcassandra.available_processors=number_of_processors/-Dcassandra.available_processors=2/' ~/.ccm/test/node$i/conf/jvm-server.options
    else
        sed -i 's/start_rpc: true/start_rpc: false/' ~/.ccm/test/node$i/conf/cassandra.yaml
    fi
done
ccm start -v
ccm status
ccm checklogerror
cat -
