FROM cassandra:3.11

ENV WORKDIR /root
WORKDIR ${WORKDIR}

ENV NODETOOL_ENABLE_SSL="false" \
    CASSANDRA_HOSTNAME=""

COPY nodetool-ssl.properties ${WORKDIR}/.cassandra/nodetool-ssl.properties

COPY docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]

CMD [""] 