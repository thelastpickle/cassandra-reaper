#!/bin/bash
if id -u reaper; then
echo "skipping user"
else
/usr/sbin/groupadd reaper || true
/usr/sbin/useradd -r -g reaper -s /sbin/nologin -d /var/empty -c 'cassandra reaper' reaper || true
fi
