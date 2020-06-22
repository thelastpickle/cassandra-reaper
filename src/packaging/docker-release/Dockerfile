# Copyright 2020 DataStax, Inc.
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

FROM ubuntu:18.04

RUN apt-get update \
    && apt-get install -y curl git openjdk-8-jdk maven xz-utils

ENV JVM_DIR /usr/lib/jvm
WORKDIR $JVM_DIR
RUN unlink default-java \
    && ln -s java-1.8.0-openjdk-amd64 default-java

ENV ALTERNATIVES_DIR /etc/alternatives/
WORKDIR $ALTERNATIVES_DIR
RUN unlink java \
    && ln -s $JVM_DIR/java-8-openjdk-amd64/bin/java ./java

RUN curl -sL https://deb.nodesource.com/setup_12.x | bash \
    && apt-get update \
    && apt-get install -y nodejs \
    && npm install -g github-changes bower

RUN java -version \
    && echo -n "mvn version: " \
    && mvn -version \
    && echo -n "node version: " \
    && node --version \
    && echo -n "npm version: " \
    && npm --version \
    && echo -n "bower version: " \
    && bower -version

ENV WORKING_DIR /cassandra-reaper
WORKDIR $WORKING_DIR
VOLUME /cassandra-reaper

COPY ./docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD [""]