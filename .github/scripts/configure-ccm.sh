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

echo "Starting Install step..."

set -xe

configure_ccm () {
  sed -i 's/#MAX_HEAP_SIZE="4G"/MAX_HEAP_SIZE="256m"/' ~/.ccm/test/node$1/conf/cassandra-env.sh
  sed -i 's/#HEAP_NEWSIZE="800M"/HEAP_NEWSIZE="200M"/' ~/.ccm/test/node$1/conf/cassandra-env.sh
  sed -i 's/_timeout_in_ms:.*/_timeout_in_ms: 60000/' ~/.ccm/test/node$1/conf/cassandra.yaml
  sed -i 's/start_rpc: true/start_rpc: false/' ~/.ccm/test/node$1/conf/cassandra.yaml
  sed -i 's/cross_node_timeout: false/cross_node_timeout: true/' ~/.ccm/test/node$1/conf/cassandra.yaml
  sed -i 's/concurrent_reads: 32/concurrent_reads: 4/' ~/.ccm/test/node$1/conf/cassandra.yaml
  sed -i 's/concurrent_writes: 32/concurrent_writes: 4/' ~/.ccm/test/node$1/conf/cassandra.yaml
  sed -i 's/concurrent_counter_writes: 32/concurrent_counter_writes: 4/' ~/.ccm/test/node$1/conf/cassandra.yaml
  sed -i 's/num_tokens: 256/num_tokens: 4/' ~/.ccm/test/node$1/conf/cassandra.yaml
  sed -i 's/auto_snapshot: true/auto_snapshot: false/' ~/.ccm/test/node$1/conf/cassandra.yaml
  sed -i 's/enable_materialized_views: true/enable_materialized_views: false/' ~/.ccm/test/node$1/conf/cassandra.yaml
  sed -i 's/internode_compression: dc/internode_compression: none/' ~/.ccm/test/node$1/conf/cassandra.yaml
  sed -i 's/# file_cache_size_in_mb: 512/file_cache_size_in_mb: 1/' ~/.ccm/test/node$1/conf/cassandra.yaml
  echo 'phi_convict_threshold: 16' >> ~/.ccm/test/node$1/conf/cassandra.yaml
  if  echo "$CASSANDRA_VERSION" | grep -q "trunk"  ; then
    sed -i 's/start_rpc: true//' ~/.ccm/test/node$1/conf/cassandra.yaml
    echo '-Dcassandra.max_local_pause_in_ms=15000' >> ~/.ccm/test/node$1/conf/jvm-server.options
    sed -i 's/#-Dcassandra.available_processors=number_of_processors/-Dcassandra.available_processors=2/' ~/.ccm/test/node$1/conf/jvm-server.options
  else
    sed -i 's/start_rpc: true/start_rpc: false/' ~/.ccm/test/node$1/conf/cassandra.yaml
  fi
}

case "${TEST_TYPE}" in
    "")
        echo "ERROR: Environment variable TEST_TYPE is unspecified."
        exit 1
        ;;
    "ccm"|"upgrade"|"elassandra")
        mkdir -p ~/.local
        cp ./.github/files/jmxremote.password ~/.local/jmxremote.password
        chmod 400 ~/.local/jmxremote.password
        sudo chmod 777 /usr/lib/jvm/zulu-8-azure-amd64/jre/lib/management/jmxremote.access
        echo "cassandra     readwrite" >> /usr/lib/jvm/zulu-8-azure-amd64/jre/lib/management/jmxremote.access
        sudo chmod 777 /opt/hostedtoolcache/jdk/8.0.192/x64/jre/lib/management/jmxremote.access
        echo "cassandra     readwrite" >> /opt/hostedtoolcache/jdk/8.0.192/x64/jre/lib/management/jmxremote.access
        if [[ ! -z $ELASSANDRA_VERSION ]]; then
          ccm create test -v file:elassandra-${ELASSANDRA_VERSION}.tar.gz
        else
          ccm create test -v $CASSANDRA_VERSION
        fi
        # use "2:0" to ensure the first datacenter name is "dc1" instead of "datacenter1", so to be compatible with CircleCI tests
        ccm populate --vnodes -n 2:0 > /dev/null
        for i in `seq 1 2` ; do
          sed -i 's/LOCAL_JMX=yes/LOCAL_JMX=no/' ~/.ccm/test/node$i/conf/cassandra-env.sh
          sed -i 's/etc\/cassandra\/jmxremote.password/home\/runner\/.local\/jmxremote.password/' ~/.ccm/test/node$i/conf/cassandra-env.sh
          # relevant for elassandra, ensure the node's dc name matches the client
          sed -i 's/DC1/dc1/' ~/.ccm/test/node$i/conf/cassandra-rackdc.properties
          sed -i 's/PropertyFileSnitch/GossipingPropertyFileSnitch/' ~/.ccm/test/node$i/conf/cassandra.yaml
          configure_ccm $i
        done
        ;;
    "sidecar")
        ccm create test -v $CASSANDRA_VERSION > /dev/null
        # use "2:0" to ensure the first datacenter name is "dc1" instead of "datacenter1", so to be compatible with CircleCI tests
        ccm populate --vnodes -n 2:0 > /dev/null
        for i in `seq 1 2` ; do
          sed -i 's/LOCAL_JMX=yes/LOCAL_JMX=no/' ~/.ccm/test/node$i/conf/cassandra-env.sh
          sed -i 's/jmxremote.authenticate=true/jmxremote.authenticate=false/' ~/.ccm/test/node$i/conf/cassandra-env.sh
          configure_ccm $i
        done
        ;;
    *)
        echo "Skipping, no actions for TEST_TYPE=${TEST_TYPE}."
esac


