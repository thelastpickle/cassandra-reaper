FROM openjdk:8-jre-alpine

ENV WORKDIR /usr/src/app
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

# Install openssl to generate and sign SSL certificates
RUN apk update \
    && apk add openssl

# Add entrypoint script
COPY docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]

# default passwords if no arguments supplied
CMD ["keypassword", "trustpassword"]