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

echo "Starting Before Install step..."

set -xe

sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 6B05F25D762E3157
sudo apt-get update
sudo apt-get install libjna-java > /dev/null
sudo apt-get install python-support > /dev/null
sudo apt-get install python-pip > /dev/null
sudo apt-get install nodejs > /dev/null
sudo apt-get install npm > /dev/null
pip install --user pyyaml > /dev/null
pip install --user ccm > /dev/null
npm install -g bower

if [ "${TEST_TYPE}" = "docker" ]
then
    sudo apt-get install docker > /dev/null
    sudo curl -o /usr/local/bin/docker-compose -L "https://github.com/docker/compose/releases/download/1.15.0/docker-compose-$(uname -s)-$(uname -m)"
    sudo chmod +x /usr/local/bin/docker-compose

    # Requests needed for the src/packaging/bin/spreaper python script which calls the Reaper API
    pip install --user requests > /dev/null
fi
