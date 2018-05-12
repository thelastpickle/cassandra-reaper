#!/bin/bash

echo "Starting Before Install step..."

set -xe

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