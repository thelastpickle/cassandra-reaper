/usr/bin/getent group reaper > /dev/null || /usr/sbin/groupadd -r reaper
/usr/bin/getent passwd reaper > /dev/null || /usr/sbin/useradd -r -g reaper -s /sbin/nologin -d /var/empty -c 'cassandra reaper' reaper || :
