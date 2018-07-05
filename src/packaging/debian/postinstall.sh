#!/usr/bin/env bash
mkdir -p /var/log/cassandra-reaper/
touch /var/log/cassandra-reaper/reaper.log
chown -R reaper: /var/log/cassandra-reaper/