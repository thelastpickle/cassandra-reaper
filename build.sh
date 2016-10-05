#!/bin/bash

mkdir -p build/usr/share/cassandra-reaper
mkdir -p build/usr/local/bin
mkdir -p build/etc/{spotify,init.d}


cp resource/cassandra-reaper.yaml build/etc/spotify/
cp target/cassandra-reaper-0.3.0-SNAPSHOT.jar build/usr/share/cassandra-reaper/
cp bin/* build/usr/local/bin/
cp resource/reaper.init build/etc/init.d/cassandra-reaper
chmod 755 build/etc/init.d/cassandra-reaper

fpm -s dir \
-t deb \
-n reaper \
-v 0.3 \
-C build .
