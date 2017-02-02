FROM ubuntu

# use a common app path, copied from python-onbuild:latest
ENV WORKDIR /usr/src/app
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

# install dependencies
RUN apt-get update \
    && apt-get install -y \
        apt-file \
        debhelper \
        devscripts \
        git \
        maven \
        openjdk-8-jdk \
    && mvn --version

# cache maven dependencies, useful during Dockerfile testing
COPY pom.xml /tmp
WORKDIR /tmp
RUN mvn package
WORKDIR ${WORKDIR}

# grab source
COPY . ${WORKDIR}/cassandra-reaper
WORKDIR ${WORKDIR}/cassandra-reaper

# target directory for final build
RUN mkdir ${WORKDIR}/packages

# -d is required to avoid:
#    dpkg-checkbuilddeps: error: Unmet build dependencies: java7-jdk
RUN debuild -uc -us -b -d \
    && cp ${WORKDIR}/*.deb ${WORKDIR}/packages

# change the WORKDIR to the directory that only includes the built packages
WORKDIR ${WORKDIR}/packages
