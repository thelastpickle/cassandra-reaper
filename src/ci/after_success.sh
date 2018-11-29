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

echo "Starting After Success step..."

set -xe


if [ "${TRAVIS_PULL_REQUEST}" = "false" -a "${TRAVIS_BRANCH}" = "master" ]
then
    mvn -B sonar:sonar \
        -Dsonar.host.url=https://sonarcloud.io \
        -Dsonar.login=${SONAR_TOKEN} \
        -Dsonar.projectKey=tlp-cassandra-reaper \
        -Dsonar.github.oauth=${GITHUB_TOKEN} \
        -Dsonar.github.repository=thelastpickle/cassandra-reaper 1> /dev/null
fi