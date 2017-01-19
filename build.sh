#!/bin/bash

set -ex

[ -n "$1" ]
TAG=pierone.stups.zalan.do/acid/cassandra-reaper:0.3.3-$1

scm-source
grep -v -e "locally modified" scm-source.json

docker build . -t $TAG
pierone login
docker push $TAG
