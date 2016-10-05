#!/bin/bash

mkdir -p build/usr/share/cassandra-reaper
mkdir -p build/etc/spotify

cp resource/cassandra-reaper.yaml build/etc/spotify/
cp target/cassandra-reaper-0.3.0-SNAPSHOT.jar build/usr/share/cassandra-reaper/

fpm -s dir \
-t deb \
-n reaper \
-v 0.3 \
-C build .
