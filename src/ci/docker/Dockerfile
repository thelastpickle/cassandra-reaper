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

FROM circleci/openjdk:8-jdk-node-browsers

ENV LOCAL_JMX=no
ENV NODES_PER_DC=2
ENV CASSANDRA_VERSION=2.1.20

RUN sudo apt-get update -qq; \
    sudo apt-get install -y libjna-java python-dev python-pip libyaml-dev nodejs; \
    sudo pip install pyYaml ccm; \
    sudo npm install -g bower > /dev/null

WORKDIR /home/circleci/
ADD pom.xml .
ADD src src

RUN  mvn -B dependency:go-offline; \
     ccm create test_2_1 --no-switch -v 2.1.20; \
     ccm create test_2_2 --no-switch -v 2.2.13; \
     ccm create test_3_0 --no-switch -v 3.0.17; \
     ccm create test_3_11 --no-switch -v 3.11.3;
     # 4.0 builds are ignored until new ccm version is released and available in pip
     #   see comment below under "workflows"
     #ccm create test_trunk --no-switch -v git:trunk

ADD src/ci/docker/docker-entrypoint.sh docker-entrypoint.sh
RUN sudo chmod +x /home/circleci/docker-entrypoint.sh

ENTRYPOINT ["./docker-entrypoint.sh"]
